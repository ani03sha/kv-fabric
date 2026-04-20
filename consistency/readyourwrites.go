package consistency

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// SessionToken is the payload carried by the client between a write and subsequent reads.
// It tells any node: "I wrote a version WriteVersion - you must have applied at least that to serve my reads".
//
// We use base64-encoded JSON rather than a signed JWT. For a production system we'd sign it to prevent forgery,
// but for this project the simplicity of base64 JSON is preferable — it's human-readable and we can decode it with
// any base64/JSON tool to inspect it.
type SessionToken struct {
	NodeID       string    `json:"node_id"`
	WriteVersion uint64    `json:"write_version"`
	IssuedAt     time.Time `json:"issued_at"`
}

// IssueToken creates and encodes a session token after a successful write.
// This is Called by the HTTP PUT handler after LeaderReplicator.Propose() returns.
func IssueToken(nodeID string, writeVersion uint64) (string, error) {
	t := SessionToken{
		NodeID:       nodeID,
		WriteVersion: writeVersion,
		IssuedAt:     time.Now(),
	}
	data, err := json.Marshal(t)
	if err != nil {
		return "", fmt.Errorf("issue token: marshal: %w", err)
	}
	// URL-safe base64 so the token can be used directly in HTTP headers without further encoding.
	return base64.URLEncoding.EncodeToString(data), nil
}

// ParseToken decodes a session token from a client request.
func ParseToken(encoded string) (SessionToken, error) {
	data, err := base64.URLEncoding.DecodeString(encoded)
	if err != nil {
		return SessionToken{}, fmt.Errorf("parse token: base64 decode: %w", err)
	}
	var t SessionToken
	if err := json.Unmarshal(data, &t); err != nil {
		return SessionToken{}, fmt.Errorf("parse token: json unmarshal: %w", err)
	}
	return t, nil
}

// RYWReader implements the Read-Your-Writes consistency guarantee.
//
// Every node (leader or follower) can serve RYW reads — no round-trip to the leader required.
// The only coordination is the node's own applied index checked against the version in the client's token.
//
// This makes RYW significantly cheaper than Strong over the lifetime of a session: after a brief catch-up window
// (typically < 10ms on a LAN cluster), all reads are served locally at follower speed.
type RYWReader struct {
	nodeID string
	engine store.KVEngine
	raft   replication.RaftNode
	logger *zap.Logger
}

func NewRYWReader(
	nodeID string,
	engine store.KVEngine,
	raft replication.RaftNode,
	logger *zap.Logger,
) *RYWReader {
	return &RYWReader{
		nodeID: nodeID,
		engine: engine,
		raft:   raft,
		logger: logger,
	}
}

// Get serves a read that guarantees the client sees their own prior writes.
//
// Three cases:
//  1. No token -> no write history to enforce. Serve as eventual read.
//  2. Token present, node not caught up -> 503 (ErrNotCaughtUp).
//  3. Token present, node caught up -> serve from local engine.
func (r *RYWReader) Get(ctx context.Context, key string, opts store.GetOptions) (*store.GetResult, error) {
	if opts.SessionToken == "" {
		// No token: this client has not written before, or is not tracking their session.
		// Fall through to a plain local read. We do not enforce any version constraint.
		result, err := r.engine.Get(key, opts)
		if err != nil {
			return nil, err
		}
		result.FromNode = r.nodeID
		return result, nil
	}

	token, err := ParseToken(opts.SessionToken)
	if err != nil {
		// Malformed token. Treat as no-token rather than failing the request:
		// degrading to an un-guaranteed read is better than a hard error.
		r.logger.Warn("RYW: malformed session token, degrading to local read",
			zap.String("node", r.nodeID),
			zap.String("key", key),
			zap.Error(err),
		)
		result, err := r.engine.Get(key, opts)
		if err != nil {
			return nil, err
		}
		result.FromNode = r.nodeID
		return result, nil
	}

	appliedIndex := r.raft.AppliedIndex()

	if appliedIndex < token.WriteVersion {
		// This node hasn't applied the version the client wrote yet.
		// Return ErrNotCaughtUp: the HTTP handler converts this to 503 with Retry-After so the client tries again shortly.
		//
		// Why not redirect to the leader? Because that defeats the purpose:
		// we want follower reads. The replication lag is typically 1-10ms,
		// so the client will succeed on its first or second retry.
		r.logger.Debug("RYW: node not caught up, returning 503",
			zap.String("node", r.nodeID),
			zap.String("key", key),
			zap.Uint64("applied", appliedIndex),
			zap.Uint64("required", token.WriteVersion),
		)
		return nil, &ErrNotCaughtUp{
			Mode:            "read-your-writes",
			RequiredVersion: token.WriteVersion,
			CurrentVersion:  appliedIndex,
		}
	}
	// Node is caught up. Serve from the local engine.
	// The client is guaranteed to see the version they wrote.
	result, err := r.engine.Get(key, opts)
	if err != nil {
		return nil, err
	}
	result.FromNode = r.nodeID
	r.logger.Debug("RYW: served",
		zap.String("node", r.nodeID),
		zap.String("key", key),
		zap.Uint64("applied", appliedIndex),
		zap.Uint64("required", token.WriteVersion),
		zap.Uint64("result_version", result.Version),
	)

	return result, nil
}
