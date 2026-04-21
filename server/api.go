package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/ani03sha/kv-fabric/consistency"
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// Holds the dependencies for all HTTP handlers. It is the "translation layer": it reads HTTP semantics
// (headers, body, query params) and calls internal mechanisms (router, leader, engine).
// It contains zero business logic — no consistency decisions live here.
type handler struct {
	nodeID  string
	engine  store.KVEngine
	router  *consistency.Router
	leader  *replication.LeaderReplicator
	tracker *replication.ReplicationTracker
	metrics *Metrics
	logger  *zap.Logger
}

// --- Request / Response Types ---

type putRequest struct {
	Value     []byte `json:"value"` // json auto-base64s
	IfVersion uint64 `json:"if_version,omitempty"`
}

type putResponse struct {
	Version      uint64 `json:"version"`
	SessionToken string `json:"session_token,omitempty"`
}

type getResponse struct {
	Value    []byte `json:"value,omitempty"`
	Version  uint64 `json:"version"`
	FromNode string `json:"from_node"`
	IsStale  bool   `json:"is_stale"`
	LagMs    int64  `json:"lag_ms,omitempty"`
}

type scanResponse struct {
	Keys []kvPairJSON `json:"keys"`
}

type kvPairJSON struct {
	Key     string `json:"key"`
	Value   []byte `json:"value"`
	Version uint64 `json:"version"`
}

type statusResponse struct {
	NodeID         string           `json:"node_id"`
	State          string           `json:"state"`
	CommitIndex    uint64           `json:"commit_index"`
	AppliedIndex   uint64           `json:"applied_index"`
	ReplicationLag map[string]int64 `json:"replication_lag"`
}

// --- Handlers ---

// This function handles PUT /v1/keys/{key}
//
// If this node is not the leader, it returns 503 with X-Leader-ID header so the client can redirect.
// Never silently proxies — the client must explicitly retry against the leader.
func (h *handler) putKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	var req putRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}

	mode := ConsistencyModeFromCtx(r.Context())

	op := replication.KVOperation{
		Type:  replication.OpPut,
		Key:   key,
		Value: req.Value,
	}

	result, err := h.leader.Propose(r.Context(), op)
	if err != nil {
		// Proposal rejected: most likely this node is not the leader.
		// Return 503 with leader identity so client can redirect.
		w.Header().Set("X-Leader-ID", h.leader.Raft().LeaderID())
		writeError(w, http.StatusServiceUnavailable,
			fmt.Sprintf("not the leader: %v", err))
		return
	}

	h.metrics.RecordPut(mode.String())

	// Issue a session token so clients can use ReadYourWrites consistency
	// on subsequent reads without hitting the leader.
	token, err := consistency.IssueToken(h.nodeID, result.Version)
	if err != nil {
		h.logger.Warn("failed to issue session token", zap.Error(err))
		// Non-fatal: respond without a token
	}

	writeJSON(w, http.StatusOK, putResponse{
		Version:      result.Version,
		SessionToken: token,
	})
}

// Handles GET /v1/keys/{key}
func (h *handler) getKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	mode := ConsistencyModeFromCtx(r.Context())

	// Build GetOptions from headers.
	opts := store.GetOptions{
		Consistency:  mode,
		SessionToken: r.Header.Get("X-Session_Token"),
	}

	if minVerStr := r.Header.Get("X-Min-Version"); minVerStr != "" {
		minVer, err := strconv.ParseUint(minVerStr, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid X-Min-Version header: %v", err))
			return
		}
		opts.MinVersion = minVer
	}

	result, err := h.router.Get(r.Context(), key, opts)
	if err != nil {
		// ErrNotCaughtUp → 503 with Retry-After: the node isn't caught up to the client's session version. Retry shortly.
		var notReady *consistency.ErrNotCaughtUp
		if errors.As(err, &notReady) {
			h.metrics.RecordSessionReject()
			w.Header().Set("Retry-After", "1")
			writeError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.metrics.RecordGet(mode.String(), result.IsStale)

	writeJSON(w, http.StatusOK, getResponse{
		Value:    result.Value,
		Version:  result.Version,
		FromNode: result.FromNode,
		IsStale:  result.IsStale,
		LagMs:    result.LagMs,
	})
}

// Handles DELETE /v1/keys/{key}
func (h *handler) deleteKey(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "key is required")
		return
	}

	op := replication.KVOperation{
		Type: replication.OpDelete,
		Key:  key,
	}

	if _, err := h.leader.Propose(r.Context(), op); err != nil {
		w.Header().Set("X-Leader-ID", h.leader.Raft().LeaderID())
		writeError(w, http.StatusServiceUnavailable, err.Error())
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// Handles GET /v1/keys?start=x&end=y&limit=n
func (h *handler) scanKeys(w http.ResponseWriter, r *http.Request) {
	start := r.URL.Query().Get("start")
	end := r.URL.Query().Get("end")
	limit := 100 // default

	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err != nil && n > 0 {
			limit = n
		}
	}

	pairs, err := h.engine.Scan(start, end, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	resp := scanResponse{Keys: make([]kvPairJSON, len(pairs))}
	for i, p := range pairs {
		resp.Keys[i] = kvPairJSON{Key: p.Key, Value: p.Value, Version: p.Version}
	}
	writeJSON(w, http.StatusOK, resp)
}

// Handles GET /v1/status
func (h *handler) getStatus(w http.ResponseWriter, r *http.Request) {
	state := "follower"
	if h.leader.Raft().IsLeader() {
		state = "leader"
	}

	lag := make(map[string]int64)
	for _, f := range h.tracker.Stats() {
		lag[f.NodeID] = f.LagMs
	}

	writeJSON(w, http.StatusOK, statusResponse{
		NodeID:         h.nodeID,
		State:          state,
		CommitIndex:    h.leader.Raft().CommitIndex(),
		AppliedIndex:   h.leader.Raft().AppliedIndex(),
		ReplicationLag: lag,
	})
}

// Handles GET /v1/debug/gc
func (h *handler) getGCDebug(w http.ResponseWriter, r *http.Request) {
	eng, ok := h.engine.(*store.MVCCEngine)
	if !ok {
		writeError(w, http.StatusNotImplemented, "GC debug not available for this engine type")
		return
	}
	writeJSON(w, http.StatusOK, eng.GCStats())
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		// Headers already sent — can't change status. Log and move on.
		return
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
