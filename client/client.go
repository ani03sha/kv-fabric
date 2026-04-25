package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ani03sha/kv-fabric/store"
)

type Config struct {
	// Base HTTP URLs of all cluster nodes.  e.g. ["http://localhost:8001", "http://localhost:8002", "http://localhost:8003"]
	// Reads are distributed across all nodes (round-robin). Writes are retried until the leader accepts.
	Addrs []string

	// This is used when PutOptions/GetOptions don't specify a mode.
	DefaultConsistency store.ConsistencyMode

	// Per-request deadline. Default: 30s.
	Timeout time.Duration
}

// Controls the behaviour of the Put call
type PutOptions struct {
	Consistency store.ConsistencyMode // 0 = use client default
	IfVersion   uint64                // 0 = unconditional write
}

// Controls the behavior of a Get call.
type GetOptions struct {
	Consistency  store.ConsistencyMode
	SessionToken string // for ReadYourWrites: token from last Put
	MinVersion   uint64 // for Monotonic: minimum version watermark
	MaxRetries   int    // max retries on 503 (default 3)
}

// This is the parsed response from a successful Put.
type PutResult struct {
	Version      uint64
	SessionToken string // use this for subsequent ReadYourWrites Gets
}

// This is the parsed response from a successful Get.
type GetResult struct {
	Value    []byte
	Version  uint64
	FromNode string
	IsStale  bool
	LagMs    int64
}

// This is one entry from a Scan result.
type KVPair struct {
	Key     string
	Value   []byte
	Version uint64
}

// This is the parsed response from a Status call.
type StatusResult struct {
	NodeID         string
	State          string
	CommitIndex    uint64
	AppliedIndex   uint64
	ReplicationLag map[string]int64
}

// This is returned when the server responds 503 with Retry-After.
// The client retries automatically up to MaxRetries: this error only surfaces when all retries are exhausted.
type RetryableError struct {
	StatusCode int
	Message    string
	RetryAfter time.Duration
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("server not ready (HTTP %d): %s", e.StatusCode, e.Message)
}

// This is a thread-safe kv-fabric client. Create one instance and share it across goroutines.
type Client struct {
	cfg     Config
	http    *http.Client
	mu      sync.Mutex
	nodeIdx int // round-robin cursor for reads
}

// This creates a new Client. The underlying http.Client is shared across all calls:
// its connection pool is what makes high-throughput benchmarks work.
func NewClient(cfg Config) *Client {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	return &Client{
		cfg: cfg,
		http: &http.Client{
			Timeout: cfg.Timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 32,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// This function writes a key-value pair to the cluster. Automatically retries against other nodes if
// it hits a non-leader.
func (c *Client) Put(ctx context.Context, key string, value []byte, opts PutOptions) (*PutResult, error) {
	consistency := opts.Consistency
	if consistency == 0 {
		consistency = c.cfg.DefaultConsistency
	}

	body, err := json.Marshal(map[string]any{
		"value":      value,
		"if_version": opts.IfVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("put: marshal: %w", err)
	}

	// Writes must go to the leader. Try each node in turn until one accepts.
	var lastErr error
	for i := 0; i < len(c.cfg.Addrs); i++ {
		addr := c.cfg.Addrs[i%len(c.cfg.Addrs)]
		url := fmt.Sprintf("%s/v1/keys/%s", addr, key)

		req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("put: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Consistency", consistency.String())

		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusServiceUnavailable {
			// Not the leader: the response includes X-Leader-ID.
			// We don't have an ID→address map here, so just try the next node.
			lastErr = fmt.Errorf("node %s is not leader", addr)
			continue
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("put: HTTP %d from %s", resp.StatusCode, addr)
		}

		var result struct {
			Version      uint64 `json:"version"`
			SessionToken string `json:"session_token"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("put: decode response: %w", err)
		}

		return &PutResult{
			Version:      result.Version,
			SessionToken: result.SessionToken,
		}, nil
	}

	return nil, fmt.Errorf("put %q: all nodes rejected: %w", key, lastErr)
}

// Get reads a value from the cluster. For ReadYourWrites and Monotonic modes, it retries on 503 with exponential
// backoff: the caller does not need to handle temporary version mismatches.
func (c *Client) Get(ctx context.Context, key string, opts GetOptions) (*GetResult, error) {
	maxRetries := opts.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	consistency := opts.Consistency
	if consistency == 0 {
		consistency = c.cfg.DefaultConsistency
	}

	backoff := 100 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := c.doGet(ctx, key, consistency, opts.SessionToken, opts.MinVersion)
		if err == nil {
			return result, nil
		}

		var retryable *RetryableError
		if errors.As(err, &retryable) && attempt < maxRetries {
			// 503: node not yet caught up. Wait and retry.
			// Use the larger of our backoff and the server's Retry-After hint.
			wait := backoff
			if retryable.RetryAfter > wait {
				wait = retryable.RetryAfter
			}
			select {
			case <-time.After(wait):
				backoff *= 2 // exponential backoff
				continue
			case <-ctx.Done():
				return nil, fmt.Errorf("get %q: %w", key, ctx.Err())
			}
		}

		return nil, fmt.Errorf("get %q: %w", key, err)
	}

	return nil, fmt.Errorf("get %q: exhausted %d retries", key, maxRetries)
}

// Performs a single GET attempt against the next node in round-robin order.
func (c *Client) doGet(
	ctx context.Context,
	key string,
	consistency store.ConsistencyMode,
	sessionToken string,
	minVersion uint64,
) (*GetResult, error) {
	addr := c.nextNode()
	url := fmt.Sprintf("%s/v1/keys/%s", addr, key)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-Consistency", consistency.String())
	if sessionToken != "" {
		req.Header.Set("X-Session-Token", sessionToken)
	}
	if minVersion > 0 {
		req.Header.Set("X-Min-Version", fmt.Sprintf("%d", minVersion))
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		rawBody, _ := io.ReadAll(resp.Body)
		retryAfter := 100 * time.Millisecond
		if s := resp.Header.Get("Retry-After"); s != "" {
			if secs, err := parseRetryAfter(s); err == nil {
				retryAfter = time.Duration(secs) * time.Second
			}
		}
		return nil, &RetryableError{
			StatusCode: resp.StatusCode,
			Message:    string(rawBody),
			RetryAfter: retryAfter,
		}
	}

	if resp.StatusCode != http.StatusOK {
		rawBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, rawBody)
	}

	var result struct {
		Value    []byte `json:"value"`
		Version  uint64 `json:"version"`
		FromNode string `json:"from_node"`
		IsStale  bool   `json:"is_stale"`
		LagMs    int64  `json:"lag_ms"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return &GetResult{
		Value:    result.Value,
		Version:  result.Version,
		FromNode: result.FromNode,
		IsStale:  result.IsStale,
		LagMs:    result.LagMs,
	}, nil
}

func (c *Client) Delete(ctx context.Context, key string) error {
	for i := 0; i < len(c.cfg.Addrs); i++ {
		addr := c.cfg.Addrs[i%len(c.cfg.Addrs)]
		url := fmt.Sprintf("%s/v1/keys/%s", addr, key)

		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return fmt.Errorf("delete: %w", err)
		}

		resp, err := c.http.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusNoContent {
			return nil
		}
		if resp.StatusCode == http.StatusServiceUnavailable {
			continue // try next node
		}
		return fmt.Errorf("delete: HTTP %d", resp.StatusCode)
	}
	return fmt.Errorf("delete %q: all nodes rejected", key)
}

// Returns all keys in [start, end) up to limit entries.
func (c *Client) Scan(ctx context.Context, start, end string, limit int) ([]KVPair, error) {
	addr := c.nextNode()
	url := fmt.Sprintf("%s/v1/keys?start=%s&end=%s&limit=%d", addr, start, end, limit)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Keys []struct {
			Key     string `json:"key"`
			Value   []byte `json:"value"`
			Version uint64 `json:"version"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	pairs := make([]KVPair, len(result.Keys))
	for i, k := range result.Keys {
		pairs[i] = KVPair{Key: k.Key, Value: k.Value, Version: k.Version}
	}
	return pairs, nil
}

// Fetches the status from a specific node address.
func (c *Client) Status(ctx context.Context, addr string) (*StatusResult, error) {
	url := fmt.Sprintf("%s/v1/status", addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		NodeID         string           `json:"node_id"`
		State          string           `json:"state"`
		CommitIndex    uint64           `json:"commit_index"`
		AppliedIndex   uint64           `json:"applied_index"`
		ReplicationLag map[string]int64 `json:"replication_lag"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &StatusResult{
		NodeID:         result.NodeID,
		State:          result.State,
		CommitIndex:    result.CommitIndex,
		AppliedIndex:   result.AppliedIndex,
		ReplicationLag: result.ReplicationLag,
	}, nil
}

func (c *Client) nextNode() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	addr := c.cfg.Addrs[c.nodeIdx%len(c.cfg.Addrs)]
	c.nodeIdx++
	return addr
}

func parseRetryAfter(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
