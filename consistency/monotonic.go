package consistency

import (
	"context"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// MonotonicReader ensures that reads within a session never return data older than what the session has already seen.
//
// Mechanism: the client tracks a version watermark in their session.
//
// After each read, the client sets: watermark = max(watermark, result.Version)
// On the next read, they send: X-Min-Version: <watermark>. The serving node checks: appliedIndex >= minVersion
//
// The watermark lives in the client, not the server. The server is completely stateless: it just checks a number.
// This is what makes monotonic reads infinitely scalable: no server-side session state, no coordination between nodes.
//
// When to use monotonic reads instead of Strong:
//   - We need temporal coherence within a session (no backward jumps)
//   - But we don't need to see OTHER clients' latest writes immediately
//   - Example: a user browsing a feed: they should see a consistent view that doesn't jump backward, but seeing a
//     post from 500m ago vs right now doesn't matter
type MonotonicReader struct {
	nodeID string
	engine store.KVEngine
	raft   replication.RaftNode
	logger *zap.Logger
}

func NewMonotonicReader(
	nodeID string,
	engine store.KVEngine,
	raft replication.RaftNode,
	logger *zap.Logger,
) *MonotonicReader {
	return &MonotonicReader{
		nodeID: nodeID,
		engine: engine,
		raft:   raft,
		logger: logger,
	}
}

// Get serves a read that guarantees the result is at least as new as the client's previously seen version.
//
// opts.MinVersion carries the client's watermark (from X-Min-Version header). If MinVersion is 0, there is no constraint:
// serve as a plain local read.
//
// The MinVersion check is on the NODE's applied index, not on the key's version.
// Reason: "the node must be caught up to at least MinVersion" means it has applied all writes up to that point, so any
// key's current value is definitively up-to-date as of MinVersion.
// Whether that specific key changed since the watermark is irrelevant: the node's view is consistent.
func (r *MonotonicReader) Get(ctx context.Context, key string, opts store.GetOptions) (*store.GetResult, error) {
	if opts.MinVersion > 0 {
		appliedIndex := r.raft.AppliedIndex()

		if appliedIndex < opts.MinVersion {
			// This node hasn't caught up to the client's watermark.
			// Return 503 — client should retry or try a different node.
			r.logger.Debug("monotonic: node behind watermark, returning 503",
				zap.String("node", r.nodeID),
				zap.String("key", key),
				zap.Uint64("applied", appliedIndex),
				zap.Uint64("min_version", opts.MinVersion),
			)
			return nil, &ErrNotCaughtUp{
				Mode:            "monotonic",
				RequiredVersion: opts.MinVersion,
				CurrentVersion:  appliedIndex,
			}
		}
	}
	// Node is caught up (or no constraint). Serve from local engine.
	// Clear MinVersion before passing to the engine: the consistency check is done above; passing it into the
	// engine would trigger the engine's own MinVersion handling which is not what we want here (we want
	// "latest version", not "latest version >= MinVersion").
	engineOpts := opts
	engineOpts.MinVersion = 0

	result, err := r.engine.Get(key, opts)
	if err != nil {
		return nil, err
	}

	result.FromNode = r.nodeID
	r.logger.Debug("monotonic: served",
		zap.String("node", r.nodeID),
		zap.String("key", key),
		zap.Uint64("min_version", opts.MinVersion),
		zap.Uint64("result_version", result.Version),
	)

	return result, nil
}
