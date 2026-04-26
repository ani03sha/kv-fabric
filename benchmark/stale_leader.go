package benchmark

import "sync/atomic"

// This struct accumulates stale read measurements across concurrent goroutines.
// Every field is updated atomically — safe to call from any number of goroutines simultaneously.
//
// A read is "stale" when EventualReader sets result.IsStale = true. That flag is set when the
// serving node's applied index is behind the leader's commit index — the node hasn't yet applied
// all committed writes, so the returned value may predate recent writes.
//
// The benchmark reads these counters at the end of each run to compute:
//
//	stale_rate = stale_reads / total_reads
//	avg_lag_ms = total_lag_ms / stale_reads
type StaleTracker struct {
	totalReads atomic.Int64
	staleReads atomic.Int64
	totalLagMs atomic.Int64
}

// Record registers one read result. Call after every Get, not just stale ones.
// isStale and lagMs come directly from GetResult.IsStale and GetResult.LagMs.
func (s *StaleTracker) Record(isStale bool, lagMs int64) {
	s.totalReads.Add(1)
	if isStale {
		s.staleReads.Add(1)
		s.totalLagMs.Add(lagMs)
	}
}

// StaleRate returns the fraction of reads that returned stale data. Returns 0 if no reads recorded.
func (s *StaleTracker) StaleRate() float64 {
	total := s.totalReads.Load()
	if total == 0 {
		return 0
	}
	return float64(s.staleReads.Load()) / float64(total)
}

// AvgLagMs returns the mean observed lag across stale reads only. Returns 0 if no stale reads.
func (s *StaleTracker) AvgLagMs() float64 {
	stale := s.staleReads.Load()
	if stale == 0 {
		return 0
	}
	return float64(s.totalLagMs.Load()) / float64(stale)
}

// Reset clears all counters. Call between benchmark runs.
func (s *StaleTracker) Reset() {
	s.totalReads.Store(0)
	s.staleReads.Store(0)
	s.totalLagMs.Store(0)
}
