package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/ani03sha/kv-fabric/proto"
	"github.com/ani03sha/kv-fabric/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	// gRPC addresses of all cluster nodes, e.g. ["localhost:9001", "localhost:9002", "localhost:9003"].
	// Reads are distributed round-robin. Writes iterate until the leader accepts.
	Addrs []string

	// Default consistency mode when PutOptions/GetOptions specify none.
	DefaultConsistency store.ConsistencyMode

	// Per-request deadline. Default: 30s.
	Timeout time.Duration
}

type PutOptions struct {
	Consistency store.ConsistencyMode
	IfVersion   uint64
}

type GetOptions struct {
	Consistency  store.ConsistencyMode
	SessionToken string // RYW: token from PutResult.SessionToken
	MinVersion   uint64 // Monotonic: client watermark
	MaxRetries   int    // redirect retries (default 3)
}

type PutResult struct {
	Version      uint64
	SessionToken string
}

type GetResult struct {
	Value    []byte
	Version  uint64
	FromNode string
	IsStale  bool
	LagMs    int64
}

type KVPair struct {
	Key     string
	Value   []byte
	Version uint64
}

type FollowerStatus struct {
	NodeID     string
	MatchIndex uint64
	LagEntries int64
	LagMs      int64
}

type StatusResult struct {
	NodeID       string
	IsLeader     bool
	LeaderID     string
	CommitIndex  uint64
	AppliedIndex uint64
	Followers    []FollowerStatus
}

// Client is a thread-safe gRPC client for kv-fabric.
// Share a single instance across goroutines — gRPC multiplexes RPCs internally.
type Client struct {
	cfg   Config
	addrs []string // ordered list from Config.Addrs

	mu      sync.RWMutex
	conns   map[string]*grpc.ClientConn  // address → connection (lazy)
	stubs   map[string]pb.KVFabricClient // address → stub (lazy)
	readIdx int                          // round-robin cursor for reads
}

func NewClient(cfg Config) *Client {
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}
	c := &Client{
		cfg:   cfg,
		addrs: cfg.Addrs,
		conns: make(map[string]*grpc.ClientConn),
		stubs: make(map[string]pb.KVFabricClient),
	}
	// Pre-create stubs so the first RPC is not slower than subsequent ones.
	// grpc.NewClient is non-blocking: the connection is established on the
	// first actual RPC, not here.
	for _, addr := range cfg.Addrs {
		_, _ = c.getOrCreateStub(addr)
	}
	return c
}

// Close releases all gRPC connections.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var lastErr error
	for _, conn := range c.conns {
		if err := conn.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// ── Writes ────────────────────────────────────────────────────────────────────

// Put writes key=value to the cluster. Iterates through addresses, following
// redirect_to hints, until the leader accepts the write.
//
// In a stable 3-node cluster this is 1-2 hops: hit any follower → redirect
// to leader → success. The tried map prevents redirect loops.
func (c *Client) Put(ctx context.Context, key string, value []byte, opts PutOptions) (*PutResult, error) {
	req := &pb.PutRequest{
		Key:       key,
		Value:     value,
		IfVersion: opts.IfVersion,
	}

	tried := make(map[string]bool)
	queue := make([]string, len(c.addrs))
	copy(queue, c.addrs)

	for len(queue) > 0 {
		addr := queue[0]
		queue = queue[1:]

		if tried[addr] {
			continue
		}
		tried[addr] = true

		stub, err := c.getOrCreateStub(addr)
		if err != nil {
			continue // node unreachable; try next
		}

		ctx2, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		resp, err := stub.Put(ctx2, req)
		cancel()
		if err != nil {
			continue
		}

		if resp.RedirectTo != "" {
			// Prepend so the redirect is tried before any remaining fallbacks.
			queue = append([]string{resp.RedirectTo}, queue...)
			continue
		}

		return &PutResult{
			Version:      resp.Version,
			SessionToken: resp.SessionToken,
		}, nil
	}

	return nil, fmt.Errorf("put %q: no leader found in cluster", key)
}

// Delete removes a key from the cluster. Same redirect-following logic as Put.
func (c *Client) Delete(ctx context.Context, key string) error {
	req := &pb.DeleteRequest{Key: key}

	tried := make(map[string]bool)
	queue := make([]string, len(c.addrs))
	copy(queue, c.addrs)

	for len(queue) > 0 {
		addr := queue[0]
		queue = queue[1:]

		if tried[addr] {
			continue
		}
		tried[addr] = true

		stub, err := c.getOrCreateStub(addr)
		if err != nil {
			continue
		}

		ctx2, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		resp, err := stub.Delete(ctx2, req)
		cancel()
		if err != nil {
			continue
		}

		if resp.RedirectTo != "" {
			queue = append([]string{resp.RedirectTo}, queue...)
			continue
		}

		return nil
	}

	return fmt.Errorf("delete %q: no leader found in cluster", key)
}

// ── Reads ─────────────────────────────────────────────────────────────────────

// Get reads a value using the specified consistency mode. For strong reads and
// RYW/monotonic fallbacks, the server returns redirect_to pointing at the node
// that can satisfy the guarantee. The client follows it transparently.
func (c *Client) Get(ctx context.Context, key string, opts GetOptions) (*GetResult, error) {
	maxRetries := opts.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}

	req := &pb.GetRequest{
		Key:          key,
		Consistency:  pb.ConsistencyMode(c.resolveConsistency(opts.Consistency)),
		SessionToken: opts.SessionToken,
		MinVersion:   opts.MinVersion,
	}

	tried := make(map[string]bool)
	addr := c.nextReadAddr()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if tried[addr] {
			break
		}
		tried[addr] = true

		stub, err := c.getOrCreateStub(addr)
		if err != nil {
			addr = c.nextReadAddr()
			continue
		}

		ctx2, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
		resp, err := stub.Get(ctx2, req)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("get %q: %w", key, err)
		}

		if resp.RedirectTo != "" {
			addr = resp.RedirectTo
			continue
		}

		return &GetResult{
			Value:    resp.Value,
			Version:  resp.Version,
			FromNode: resp.FromNode,
			IsStale:  resp.IsStale,
			LagMs:    resp.LagMs,
		}, nil
	}

	return nil, fmt.Errorf("get %q: exhausted %d retries", key, maxRetries)
}

// Scan returns all key-value pairs in [start, end) up to limit entries.
// Scans always read from a single node with eventual semantics.
func (c *Client) Scan(ctx context.Context, start, end string, limit int) ([]KVPair, error) {
	stub, err := c.getOrCreateStub(c.nextReadAddr())
	if err != nil {
		return nil, fmt.Errorf("scan: %w", err)
	}

	ctx2, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	resp, err := stub.Scan(ctx2, &pb.ScanRequest{
		Start: start,
		End:   end,
		Limit: int32(limit),
	})
	if err != nil {
		return nil, fmt.Errorf("scan [%q, %q): %w", start, end, err)
	}

	pairs := make([]KVPair, len(resp.Pairs))
	for i, p := range resp.Pairs {
		pairs[i] = KVPair{Key: p.Key, Value: p.Value, Version: p.Version}
	}
	return pairs, nil
}

// Status fetches cluster status from a specific node address.
func (c *Client) Status(ctx context.Context, addr string) (*StatusResult, error) {
	stub, err := c.getOrCreateStub(addr)
	if err != nil {
		return nil, fmt.Errorf("status: connect to %s: %w", addr, err)
	}

	ctx2, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()

	resp, err := stub.Status(ctx2, &pb.StatusRequest{})
	if err != nil {
		return nil, fmt.Errorf("status from %s: %w", addr, err)
	}

	followers := make([]FollowerStatus, len(resp.Followers))
	for i, f := range resp.Followers {
		followers[i] = FollowerStatus{
			NodeID:     f.NodeId,
			MatchIndex: f.MatchIndex,
			LagEntries: f.LagEntries,
			LagMs:      f.LagMs,
		}
	}

	return &StatusResult{
		NodeID:       resp.NodeId,
		IsLeader:     resp.IsLeader,
		LeaderID:     resp.LeaderId,
		CommitIndex:  resp.CommitIndex,
		AppliedIndex: resp.AppliedIndex,
		Followers:    followers,
	}, nil
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// getOrCreateStub returns the gRPC stub for addr, creating a connection if needed.
// Double-checked locking: fast RLock on the common path (stub exists),
// write Lock only when a new connection must be created.
func (c *Client) getOrCreateStub(addr string) (pb.KVFabricClient, error) {
	c.mu.RLock()
	stub, ok := c.stubs[addr]
	c.mu.RUnlock()
	if ok {
		return stub, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if stub, ok := c.stubs[addr]; ok { // re-check after acquiring write lock
		return stub, nil
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", addr, err)
	}

	stub = pb.NewKVFabricClient(conn)
	c.conns[addr] = conn
	c.stubs[addr] = stub
	return stub, nil
}

func (c *Client) nextReadAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	addr := c.addrs[c.readIdx%len(c.addrs)]
	c.readIdx++
	return addr
}

// resolveConsistency returns the effective mode for a request.
// Zero (Strong) is indistinguishable from "not set" — if the default is also
// Strong this is a no-op, which is the common case.
func (c *Client) resolveConsistency(mode store.ConsistencyMode) store.ConsistencyMode {
	if mode == store.ConsistencyStrong {
		return c.cfg.DefaultConsistency
	}
	return mode
}
