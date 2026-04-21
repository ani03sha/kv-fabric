package replication

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// SemiSyncReplicator implements semi-synchronous replication with timeout fallback.
//
// In semi-sync mode, the leader waits for at least one follower to ack a write before responding to the client.
// This provides stronger durability than pure async. Under normal conditions, the write is on at least two nodes.
//
// THE TRAP: if no follower acknowledges within the timeout, the leader falls back to async and responds "success" anyway.
// The client receives a success response for a write that exists only on the leader's log.
// If the leader crashes before any follower catches up, the write is permanently lost.
//
// The fallbackCount metric is the ONLY observable signal that the trap has been triggered. Monitor it.
// If it is non-zero when a leader dies, phantom writes will be found in the post-mortem.
type SemiSyncReplicator struct {
	tracker *ReplicationTracker
	timeout time.Duration
	logger  *zap.Logger

	// Prometheus metrics: the observability surface.
	promFallbackCount prometheus.Counter
	promPhantomWrites prometheus.Counter

	// Local atomic counters mirror the Prometheus counters.
	// Prometheus counters are write-only: we cannot read their current value from the prometheus.Counter interface.
	// The atomic counters let the phantom_durability scenario read the exact count at any moment.
	fallbackCount atomic.Uint64
	phantomCount  atomic.Uint64
}

func NewSemiSyncReplicator(
	tracker *ReplicationTracker,
	timeout time.Duration,
	reg prometheus.Registerer,
	logger *zap.Logger,
) *SemiSyncReplicator {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	fallback := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kv_fabric_semisync_fallback_total",
		Help: "Total number of times semi-sync timed out and fell back to async replication. " +
			"Non-zero values mean phantom writes are possible on leader failure.",
	})
	phantom := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kv_fabric_phantom_writes_total",
		Help: "Writes acknowledged to the client that were subsequently lost after leader failure. " +
			"Detected and recorded by the phantom_durability scenario.",
	})

	reg.MustRegister(fallback, phantom)

	return &SemiSyncReplicator{
		tracker:           tracker,
		timeout:           timeout,
		logger:            logger,
		promFallbackCount: fallback,
		promPhantomWrites: phantom,
	}
}

// Blocks until at least one follower has acknowledged logIndex, or until the timeout fires, or until ctx is cancelled.
//
// Return values:
// acked=true,  fellBack=false → a follower confirmed the write in time
// acked=false, fellBack=true  → timeout or cancellation — write not on any follower
//
// When fellBack=true, the CALLER still returns success to the client.
// This is the phantom durability trap. The caller (Propose) does not change its behavior based on fellBack,
// it always returns the PutResult. The only observable signal is the fallbackCount metric.
func (s *SemiSyncReplicator) WaitForAck(ctx context.Context, logIndex uint64) (acked bool, fellback bool) {
	// Fast path: a follower may already have replicated this entry before we even started waiting
	if s.anyFollowerAt(logIndex) {
		return true, false
	}

	timer := time.NewTimer(s.timeout)
	defer timer.Stop()

	// Poll every 5ms. Given semi-sync timeouts are 100ms-1s, this gives 20-200 poll opportunities before timeout:
	// tight enough to be responsive without burning CPU.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// Client disconnected or its deadline exceeded.
			s.recordFallback(logIndex, "context cancelled")
			return false, true

		case <-timer.C:
			// ── THE TRAP FIRES HERE ──
			//
			// No follower acknowledged within the timeout.
			// We increment fallbackCount and return fellBack=true.
			// The caller (Propose) will return success to the client anyway.
			//
			// At this exact moment:
			//   - The write exists on the leader's log
			//   - The write does NOT exist on any follower's log
			//   - If the leader dies in the next few hundred milliseconds,
			//     this write will vanish from the cluster permanently
			//   - The client has no idea
			s.recordFallback(logIndex, "timeout")
			return false, true

		case <-ticker.C:
			if s.anyFollowerAt(logIndex) {
				s.logger.Debug("semi-sync: follower ACK received", zap.Uint64("log_index", logIndex))
				return true, false
			}
		}
	}
}

// RecordPhantomWrite is called by scenarios/phantom_durability.go when it detects that a write confirmed to the client
// is missing from the new leader's log after a failover.
//
// In production we cannot call this automatically: you only discover phantom writes during a post-mortem by comparing
// client-confirmed writes to the new leader's log. The scenario automates this comparison.
func (s *SemiSyncReplicator) RecordPhantomWrite() {
	s.promPhantomWrites.Inc()
	s.phantomCount.Add(1)
}

// FallbackTotal returns the total number of times semi-sync fell back to async.
// Used by the phantom_durability scenario to record the exact count at the moment the leader is killed.
func (s *SemiSyncReplicator) FallbackTotal() uint64 {
	return s.fallbackCount.Load()
}

// PhantomWriteTotal returns the total number of confirmed-but-lost writes detected.
func (s *SemiSyncReplicator) PhantomWriteTotal() uint64 {
	return s.phantomCount.Load()
}

// Returns true if any follower has a matchIndex >= logIndex.
// Reads the tracker's snapshot: safe to call on any goroutine.
func (s *SemiSyncReplicator) anyFollowerAt(logIndex uint64) bool {
	for _, state := range s.tracker.Stats() {
		if state.MatchIndex >= logIndex {
			return true
		}
	}
	return false
}

// Increments both the Prometheus counter and the local atomic.
func (s *SemiSyncReplicator) recordFallback(logIndex uint64, reason string) {
	s.promFallbackCount.Inc()
	s.fallbackCount.Add(1)
	s.logger.Warn("semi-sync: falling back to async",
		zap.Uint64("log_index", logIndex),
		zap.String("reason", reason),
		zap.Uint64("total_fallbacks", s.fallbackCount.Load()),
	)
}
