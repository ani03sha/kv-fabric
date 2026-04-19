package consistency

import (
	"context"
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// StrongReader implements linearizable reads via the ReadIndex protocol.
//
// Cost: one heartbeat round-trip to a quorum of followers per read. On a LAN under normal conditions: ~1-5ms.
// The benchmark will surface this cost explicitly against the Eventual baseline.
//
// Only the Raft leader can serve strong reads. If called on a follower,  ConfirmLeadership will fail and the
// HTTP handler redirects the client.
type StrongReader struct {
	nodeID string
	engine store.KVEngine
	raft   replication.RaftNode
	logger *zap.Logger
}

// Get performs a linearizable read using the ReadIndex protocol.
// Guarantee: the returned value reflects all writes that completed before this call started.
// No completed writes can be invisible to this read.
func (r *StrongReader) Get(ctx context.Context, key string, opts store.GetOptions) (*store.GetResult, error) {
	// Step 1: Snapshot the read fence.
	// readIndex is the minimum Raft log index we must have applied before serving this read.
	// Any write committed at index <= readIndex is guaranteed to be visible in our result.
	readIndex := r.raft.CommitIndex()

	// Step 2: Confirm we are still the leader.
	// This is the safety check. Without it, the partitioned leader could serve reads that don't
	// reflect writes committed by the new true leader.
	// ConfirmLeadership sends a heartbeat and waits for quorum ACKs. It fails if this node can't
	// reach a majority.
	if err := r.raft.ConfirmLeadership(ctx); err != nil {
		return nil, fmt.Errorf("strong read: leadership confirmation failed (this node may no longer be the leader): %w", err)
	}

	// Step 3: Wait for the engine to catch up to readIndex
	// Even on the confirmed leader, the applyLoop applies entries async. There is a gap between
	// "committed by quorum" and "applied to the engine". We must wait for the gap to close before reading.
	if err := r.waitForApplied(ctx, readIndex); err != nil {
		return nil, fmt.Errorf("strong read: timed out waiting for applied index %d: %w", readIndex, err)
	}

	// Step 4: Read from the local engine.
	// At this point, we are the confirmed leader and we have applied everything up to readIndex.
	// The read is linearizable.
	result, err := r.engine.Get(key, opts)
	if err != nil {
		return nil, fmt.Errorf("strong read: engine: %w", err)
	}

	result.FromNode = r.nodeID
	result.IsStale = false // linearizable reads are never stale by definition
	result.LagMs = 0

	r.logger.Debug("strong read served",
		zap.String("node", r.nodeID),
		zap.String("key", key),
		zap.Uint64("read_index", readIndex),
		zap.Uint64("applied_index", r.raft.AppliedIndex()),
	)

	return result, nil
}

// This function waitForApplied blocks until raft.AppliedIndex() >= target or ctx is cancelled.
//
// This uses exponential backoff polling, not a condition variable. In a high-throughput system we'd
// notify readers from the applyLoop as it advances the applied index.
//
// The polling approach is correct and simpler for this implementation — under normal load the fast path
// fires immediately because the applyLoop typically applies within microseconds of commit.
func (r *StrongReader) waitForApplied(ctx context.Context, target uint64) error {
	// Fast path: already caught up - no sleep needed
	if r.raft.AppliedIndex() >= target {
		return nil
	}

	backoff := 100 * time.Microsecond
	const maxBackoff = 5 * time.Millisecond

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for applied index: %w", ctx.Err())
		case <-time.After(backoff):
			if r.raft.AppliedIndex() >= target {
				return nil
			}
			// Exponential backoff: 0.1ms + 0.2ms + 0.4ms ... -> 5ms
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}
