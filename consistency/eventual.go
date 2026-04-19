package consistency

import (
	"context"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// EventualReader serves reads from the local engine with zero network overhead.
//
// "Eventual" is the consistency guarantee: writes will eventually appear, but the reader may see data
// from before recent writes arrived.
//
// The IsStale and LagMs fields in every GetResult are what make this mode scientifically measurable.
// The benchmark reads these fields to compute:
//   - Stale read rate: (reads where IsStale=true) / (total reads)
//   - Average staleness: mean(LagMs) across all stale reads
//   - P99 staleness: 99th percentile LagMs
//
// These numbers are the headline result of the benchmark table.
// Without these fields, "eventual consistency" is just an assertion. With them, it becomes "3.2% of reads were stale;
// median staleness 18ms."
type EventualReader struct {
	nodeID  string
	engine  store.KVEngine
	raft    replication.RaftNode
	tracker *replication.ReplicationTracker
	logger  *zap.Logger
}

// Get reads from the local engine and annotates the result with staleness data.
//
// The staleness annotation uses two data sources:
//  1. raft.AppliedIndex() vs raft.CommitIndex() — entry-count lag on this node
//  2. tracker.GetLag(nodeID) — millisecond estimate from the leader's tracker
//
// Source 2 is more accurate because it uses observed write latency (the EMA in ReplicationTracker.UpdateAvgWriteLatency).
// Source 1 is the fallback when the tracker doesn't have data for this node yet.
func (r *EventualReader) Get(ctx context.Context, key string, opts store.GetOptions) (*store.GetResult, error) {
	// Snapshot the applied and commit indices before the read.
	// We capture these first so the staleness annotation reflects the state at the moment of the read, not after.
	localApplied := r.raft.AppliedIndex()
	leaderCommit := r.raft.CommitIndex()

	// Serve from local engine: no Raft coordination, no network call.
	result, err := r.engine.Get(key, opts)
	if err != nil {
		return nil, err
	}

	result.FromNode = r.nodeID

	if localApplied < leaderCommit {
		// This node has not yet applied all committed entries.
		// The returned value may not reflect the most recent writes.
		result.IsStale = true

		//Primary source: tracker's millisecond estimate.
		// The tracker knows this node's matchIndex and uses the observed average write latency to convert entry
		// lag to time lag
		_, lagMs := r.tracker.GetLag(r.nodeID)

		if lagMs == 0 {
			// Fallback: tracker hasn't seen data for this node yet.
			// Estimate 1ms per lagging entry — conservative but reasonable.
			lagMs = int64(leaderCommit - localApplied)
		}
		result.LagMs = lagMs
	} else {
		// This node is caught up. The read is not stale from our perspective.
		// Note: the leader may have committed new entries since we read CommitIndex — there's an irreducible
		// window of potential staleness. But it's immeasurably small and unavoidable without coordination.
		result.IsStale = false
		result.LagMs = 0
	}

	r.logger.Debug("eventual read served",
		zap.String("node", r.nodeID),
		zap.String("key", key),
		zap.Bool("stale", result.IsStale),
		zap.Int64("lag_ms", result.LagMs),
		zap.Uint64("local_applied", localApplied),
		zap.Uint64("leader_commit", leaderCommit),
	)

	return result, nil
}
