package replication

import (
	"sync"
	"time"
)

// This tracks how far behind each follower is relative to the leader.
// This struct is the data source for two critical features:
//  1. GetResult.IsStale and GetResult.LagMs - populated by the consistency layer when serving
//     eventual reads, so the client can observe staleness.
//  2. The booking.com scenario, where we will inject artificial lag here and show that a follower
//     340ms behind can make a confirmed write "disappear".
type ReplicationTracker struct {
	mu                sync.RWMutex
	followers         map[string]*FollowerState
	leaderCommitIndex uint64 // current leader commitIndex, updated as log advances
	avgWriteLatencyMs int64  // exponential moving average of write latency
}

type FollowerState struct {
	NodeID        string
	MatchIndex    uint64    // last log index confirmed replicated to this follower
	LastHeartbeat time.Time // last time the follower was heard from
	LagEntries    int64     // leader.commitIndex - follower.matchIndex
	LagMs         int64     // estimated ms behind: LagEntries * avgWriteLatencyMs
	StaleSince    time.Time // when this follower first started lagging
}

func NewReplicationTracker() *ReplicationTracker {
	return &ReplicationTracker{
		followers:         make(map[string]*FollowerState),
		avgWriteLatencyMs: 1, // conservative default; updated by observed write latencies
	}
}

// Registers a new follower. Call once per peer node at startup.
func (t *ReplicationTracker) Track(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.followers[nodeID] = &FollowerState{
		NodeID:        nodeID,
		LastHeartbeat: time.Now(),
	}
}

// Advances the leader's known CommitIndex. Must be called every time the Raft commitIndex advances.
// Triggers a lag recomputation across all followers.
func (t *ReplicationTracker) UpdateLeaderCommitIndex(index uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.leaderCommitIndex = index
	t.recomputeLagLocked()
}

// Records that a follower has applied up to matchIndex. Called when the leader receives confirmation
// from a follower.
func (t *ReplicationTracker) UpdateFollower(nodeID string, matchIndex uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	f, ok := t.followers[nodeID]
	if !ok {
		f = &FollowerState{NodeID: nodeID}
		t.followers[nodeID] = f
	}
	wasLagging := f.LagEntries > 0
	f.MatchIndex = matchIndex
	f.LastHeartbeat = time.Now()

	if t.leaderCommitIndex > matchIndex {
		f.LagEntries = int64(t.leaderCommitIndex - matchIndex)
		f.LagMs = f.LagEntries * t.avgWriteLatencyMs
		if !wasLagging {
			f.StaleSince = time.Now()
		}
	} else {
		f.LagEntries = 0
		f.LagMs = 0
		f.StaleSince = time.Time{} // caught up - reset staleness timer
	}
}

// Returns the current lag for one follower in entries and milliseconds.
func (t *ReplicationTracker) GetLag(nodeID string) (entries int64, estimatedMs int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	f, ok := t.followers[nodeID]
	if !ok {
		return 0, 0
	}
	return f.LagEntries, f.LagMs
}

// Returns the highest lag in milliseconds across all followers. Used by the server status endpoint and the
// semi-sync timeout decision.
func (t *ReplicationTracker) GetMaxLag() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var max int64
	for _, f := range t.followers {
		if f.LagMs > max {
			max = f.LagMs
		}
	}
	return max
}

// Returns true if the follower's lag is within maxLagMs. Used by the eventual consistency layer to decide
// whether a follower read is "fresh enough" to serve without the IsStale flag.
func (t *ReplicationTracker) IsFollowerFresh(nodeID string, maxLagMs int64) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	f, ok := t.followers[nodeID]
	if !ok {
		return false
	}
	return f.LagMs <= maxLagMs
}

// Returns a copy of all follower states for the status endpoint.
func (t *ReplicationTracker) Stats() []FollowerState {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]FollowerState, 0, len(t.followers))
	for _, f := range t.followers {
		result = append(result, *f)
	}
	return result
}

// Refines the lag-to-milliseconds estimate. Uses an exponential moving average so spikes don't dominate.
// Formula: new = 0.9*old + 0.1*sample  (90% weight on history)
func (t *ReplicationTracker) UpdateAvgWriteLatency(ms int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ms > 0 {
		t.avgWriteLatencyMs = (9*t.avgWriteLatencyMs + ms) / 10
	}
	t.recomputeLagLocked()
}

// Recalculates LagEntries and LagMs for every follower. MUST be called with t.mu held for writing.
func (t *ReplicationTracker) recomputeLagLocked() {
	for _, f := range t.followers {
		if t.leaderCommitIndex > f.MatchIndex {
			f.LagEntries = int64(t.leaderCommitIndex - f.MatchIndex)
			f.LagMs = f.LagEntries * t.avgWriteLatencyMs
		} else {
			f.LagEntries = 0
			f.LagMs = 0
		}
	}
}
