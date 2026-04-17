package store

import (
	"encoding/json"
	"fmt"
	"time"
)

// This is the wire format for an engine snapshot. Contains only the latest non-deleted
// version of each key. History (older versions) is intentionally excluded - a snapshot
// represents "current state", not full audit history.
type snapshotData struct {
	Version uint64          `json:"version"` // the Raft log index at snapshot time
	Entries []snapshotEntry `json:"entries"`
}

type snapshotEntry struct {
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
	Version   uint64    `json:"version"`
	Timestamp time.Time `json:"timestamp"`
}

// Concrete implementation of the Snapshot interface
type mvccSnapshot struct {
	data    []byte
	version uint64
}

func (s *mvccSnapshot) Data() []byte {
	return s.Data()
}

func (s *mvccSnapshot) Version() uint64 {
	return s.version
}

// Snapshot captures the current state of the engine for Raft log compaction.
//
// We hold e.mu.RLock() for the entire operation to get a consistent point-in-time
// view — no writes should appear between reading two different keys' chains.
// Individual chain read locks are taken and released per key.
//
// The snapshot does NOT include version history, only the latest value per key.
// This is a deliberate tradeoff: smaller snapshot size at the cost of losing
// the ability to do point-in-time reads older than the snapshot on a restored node.
func (e *MVCCEngine) Snapshot() (Snapshot, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	snap := snapshotData{
		Version: e.gcHorizon,
		Entries: make([]snapshotEntry, 0, len(e.data)),
	}

	for key, chain := range e.data {
		chain.mu.RLock()
		latest := chain.latestLocked()
		chain.mu.RUnlock()

		if latest == nil || latest.Deleted {
			continue // key doesn't exist or was deleted
		}

		snap.Entries = append(snap.Entries, snapshotEntry{
			Key:       key,
			Value:     makeCopy(latest.Value),
			Version:   latest.Num,
			Timestamp: latest.Timestamp,
		})
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("snapshot: marshal failed: %w", err)
	}
	return &mvccSnapshot{data: data, version: snap.Version}, nil
}

// ApplySnapshot replaces the engine's entire state with the snapshot contents.
//
// Called by the replication layer when a follower receives an InstallSnapshot RPC.
// All existing data is discarded — the snapshot is the authoritative state.
//
// After applying: the engine contains exactly one version per key (the snapshot
// version), and the GC horizon is set to the snapshot's version number.
func (e *MVCCEngine) ApplySnapshot(s Snapshot) error {
	if s == nil {
		return fmt.Errorf("apply snapshot: nil snapshot")
	}
	var snap snapshotData
	if err := json.Unmarshal(s.Data(), &snap); err != nil {
		return fmt.Errorf("apply snapshot: unmarshal failed: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// Discard all existing data and stats - snapshot is the new ground truth
	e.data = make(map[string]*VersionChain)
	e.stats = EngineStats{}
	e.gcHorizon = snap.Version

	for _, entry := range snap.Entries {
		chain := &VersionChain{
			versions: []Version{
				{
					Num:       entry.Version,
					Value:     makeCopy(entry.Value),
					Timestamp: entry.Timestamp,
				},
			},
		}
		e.data[entry.Key] = chain
		e.stats.TotalKeys++
		e.stats.TotalVersions++
	}
	return nil
}
