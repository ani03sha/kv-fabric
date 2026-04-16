package store

import (
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// This is the in-memory, multi-version key-value store. It implements KVEngine interface.
// Data model:
//
//	e.data["user:1"] → VersionChain{
//	  versions: [
//	    {Num:1, Value:"alice"},
//	    {Num:5, Value:"alice_v2"},
//	    {Num:9, Deleted:true},
//	    {Num:12, Value:"alice_v3"},
//	  ]
//	}
//
// Locking discipline (always in this order, never reversed):
//  1. e.mu  — acquire to access e.data (the map)
//  2. chain.mu — acquire to access a specific key's version list
//
// Never hold e.mu while waiting for chain.mu on a second key.
type MVCCEngine struct {
	mu        sync.RWMutex
	data      map[string]*VersionChain
	gcTicker  *time.Ticker
	gcStop    chan struct{} // closed to signal the GC goroutine to exit
	gcHorizon uint64        // GC may only collect versions with Num < gcHorizon
	stats     EngineStats
	nodeID    string // reported in GetResult.FromNode
}

// This is the ordered history of all versions for one key. Versions are stored oldest-first (index 0 = oldest)
type VersionChain struct {
	mu       sync.RWMutex
	versions []Version
	// This is the oldest version held by any active transaction on this key.
	// GC must not collect any version with Num >= pinned.
	// 0 means no transaction is currently pinning this key.
	pinned uint64
}

// This is a fallback version counter used only in unit tests.
// In production, the Raft log index is passed via PutOptions.LogIndex.
var versionSeq atomic.Uint64

// Creates a new engine and starts the background GC goroutine.
//
// gcInterval controls how often GC runs. 30s is production default.
func NewMVCCEngine(nodeID string, gcInterval time.Duration) *MVCCEngine {
	e := &MVCCEngine{
		data:   make(map[string]*VersionChain),
		gcStop: make(chan struct{}),
		nodeID: nodeID,
	}
	e.gcTicker = time.NewTicker(gcInterval)
	go e.gcLoop()
	return e
}

// Writes a new version for key.
//
// If opts.LogIndex != 0, that value is used as the version number — this is the production path where
// the replication layer passes the Raft log index.
//
// If opts.LogIndex == 0, a monotonically increasing fallback counter is used (test-only path).
//
// If opts.IfVersion != 0, this is a conditional (CAS) write: the write is applied only if the key's
// current version matches opts.IfVersion.
func (e *MVCCEngine) Put(key string, value []byte, opts PutOptions) (*PutResult, error) {
	// Step 1: get or create the chain - needs engine write lock.
	e.mu.Lock()
	chain := e.getOrCreateChain(key)
	e.mu.Unlock() // Engine lock is released. From here we only touch this one chain.

	// Step 2: write to the chain - needs chain write lock.
	chain.mu.Lock()
	defer chain.mu.Unlock()

	// CAS check: if the caller wants a conditional write, verify the current version matches
	// their expectation before proceeding.
	if opts.IfVersion != 0 {
		current := chain.latestLocked()
		currentNum := uint64(0)
		if current != nil {
			currentNum = current.Num
		}
		if currentNum != opts.IfVersion {
			return nil, fmt.Errorf("CAS failed for key %q: expected version %d, found %d", key, opts.IfVersion, currentNum)
		}
	}

	version := opts.LogIndex
	if version == 0 {
		// Fallback for standalone tests: use global atomic counter.
		version = versionSeq.Add(1)
	}

	v := Version{
		Num:       version,
		Value:     makeCopy(value), // defensive copy: caller may reuse their slice
		Timestamp: time.Now(),
	}
	chain.versions = append(chain.versions, v)

	// Update stats under the chain lock (no separate lock needed for stats fields that
	// map 1:1 with chain operations)
	e.stats.TotalVersions++
	if len(chain.versions) == 1 {
		// First version for this key
		e.stats.TotalKeys++
	}

	return &PutResult{
		Version:   version,
		AppliedAt: v.Timestamp,
	}, nil
}

func (e *MVCCEngine) Get(key string, opts GetOptions) (*GetResult, error) {
	// Step 1: look up the chain: only needs engine read lock
	e.mu.RLock()
	chain, exists := e.data[key]
	e.mu.RUnlock()

	if !exists {
		return &GetResult{FromNode: e.nodeID}, nil
	}

	// Step 2: read from the chain and only chain's read lock is sufficient.
	chain.mu.RLock()
	defer chain.mu.Unlock()

	var v *Version

	if opts.MinVersion > 0 {
		// Monotonic read: the caller's session has seen a value at MinVersion, so we must return
		// a version >= MinVersion to avoid going backward.
		v = chain.atOrAfterVersionLocked(opts.MinVersion)
		if v == nil {
			// This node hasn't applied MinVersion yet
			return nil, fmt.Errorf("monotonic read: key %q has no version >= %d (node not caught up yet)", key, opts.MinVersion)
		}
	} else {
		// Default: return the latest non-deleted version
		v = chain.latestLocked()
	}

	if v == nil || v.Deleted {
		return &GetResult{FromNode: e.nodeID}, nil
	}

	return &GetResult{
		Value:     makeCopy(v.Value), // defensive copy: caller may hold this result long-term
		Version:   v.Num,
		WrittenAt: v.Timestamp,
		FromNode:  e.nodeID,
		// IsStale and LagMs are set by the consistency layer, not here.
		// The engine doesn't know about replication — that's separation of concerns.
	}, nil
}

func (e *MVCCEngine) GetAtVersion(key string, maxVersion uint64) (*GetResult, error) {
	e.mu.RLock()
	chain, exists := e.data[key]
	e.mu.RUnlock()

	if !exists {
		return &GetResult{FromNode: e.nodeID}, nil
	}

	chain.mu.RLock()
	defer chain.mu.RUnlock()

	v := chain.atOrAfterVersionLocked(maxVersion)
	if v == nil || v.Deleted {
		return &GetResult{FromNode: e.nodeID}, nil
	}

	return &GetResult{
		Value:     makeCopy(v.Value),
		Version:   v.Num,
		WrittenAt: v.Timestamp,
		FromNode:  e.nodeID,
	}, nil
}

// This function creates a tombstone for key. The key's history is preserved; GC will clean it up eventually.
// This means a Delete followed by a Put at a higher version correctly brings the key back to life.
func (e *MVCCEngine) Delete(key string, opts DeleteOptions) error {
	e.mu.Lock()
	chain := e.getOrCreateChain(key)
	e.mu.Unlock()

	chain.mu.Lock()
	defer chain.mu.Unlock()

	if opts.IfVersion != 0 {
		current := chain.latestLocked()
		currentNum := uint64(0)
		if current != nil {
			currentNum = current.Num
		}
		if currentNum != uint64(opts.IfVersion) {
			return fmt.Errorf("CAS failed for delete of key %q: expected version %d, found %d", key, opts.IfVersion, currentNum)
		}
	}

	version := versionSeq.Add(1)

	chain.versions = append(chain.versions, Version{
		Num:       version,
		Timestamp: time.Now(),
		Deleted:   true,
	})
	e.stats.TotalVersions++
	return nil
}

// Returns the KVPairs of all live keys in [start, end) in lexicographic order. If end is empty, scan to the end of
// keyspace. If limit > 0, returns at most limit results.
func (e *MVCCEngine) Scan(start, end string, limit int) ([]KVPair, error) {
	// Snapshot the relevant keys while holding only the engine read lock.
	// We do not hold the engine lock during individual Gets — that would
	// prevent any concurrent writes for the entire scan duration.
	e.mu.RLock()
	keys := make([]string, 0)
	for k := range e.data {
		if k < start {
			continue
		}
		if end != "" && k >= end {
			continue
		}
		keys = append(keys, k)
	}
	e.mu.RUnlock()

	sort.Strings(keys)

	results := make([]KVPair, 0, len(keys))
	for _, k := range keys {
		if limit > 0 && len(results) >= limit {
			break
		}
		res, err := e.Get(k, GetOptions{})
		if err != nil {
			continue
		}
		if res.Value == nil {
			continue // key was deleted
		}
		results = append(results, KVPair{
			Key:     k,
			Value:   res.Value,
			Version: res.Version,
		})
	}
	return results, nil
}

// Returns a point-in-time snapshot of engine health. Safe to call without holding any lock:
// EngineStats fields are ints/uint64s that ard only ever incremented, so a slightly read is
// acceptable here.
func (e *MVCCEngine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.stats
}

// Stops the GC goroutine. Must be called before discarding the engine.
func (e *MVCCEngine) Close() error {
	e.gcTicker.Stop()
	close(e.gcStop) // broadcasting close to gcLoop goroutine
	return nil
}

// Snapshot and ApplySnapshot are stubs - implemented fully in Phase 3 when we wire up the Raft snapshot mechanism.
func (e *MVCCEngine) Snapshot() (Snapshot, error) {
	return nil, fmt.Errorf("snapshot: not yet implemented")
}

func (e *MVCCEngine) ApplySnapshot(s Snapshot) error {
	return fmt.Errorf("apply snapshot: not yet implemented")
}

// When a transaction begins, it pins the current version for each key it reads.
// This creates a stable read snapshot: subsequent reads in the same transaction always see the same data,
// even if newer versions are written concurrently.
//
// The pin is also what blocks GC.
//
// PinVersion prevents GC from collecting versions >= version for key. Called by the transaction layer when
// a transaction begins.
func (e *MVCCEngine) PinVersion(key string, version uint64) {
	e.mu.RLock()
	chain, ok := e.data[key]
	e.mu.RUnlock()
	if !ok {
		return
	}
	chain.mu.Lock()
	defer chain.mu.Unlock()
	// Pin the minimum of all currently pinned versions.
	// If two transactions are active, GC is blocked by the oldest one.
	if chain.pinned == 0 || version < chain.pinned {
		chain.pinned = version
	}
}

// UnpinVersion releases the pin held by a transaction. Called when the transaction commits or aborts.
func (e *MVCCEngine) UnpinVersion(key string, version uint64) {
	e.mu.RLock()
	chain, ok := e.data[key]
	e.mu.RUnlock()
	if !ok {
		return
	}
	chain.mu.Lock()
	defer chain.mu.Unlock()
	if chain.pinned == version {
		chain.pinned = 0
	}
}

// SetGCHorizon updates the global GC cutoff.
// GC may only collect versions with Num < horizon AND not pinned.
// Called by the replication layer as the cluster advances.
func (e *MVCCEngine) SetGCHorizon(horizon uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.gcHorizon = horizon
}

// This is the background goroutine that drives garbage collection.
func (e *MVCCEngine) gcLoop() {
	for {
		select {
		case <-e.gcTicker.C:
			e.runGC() // implemented in gc.go
		case <-e.gcStop:
			// Channel was closed by Close() — exit cleanly.
			return
		}
	}
}

func (e *MVCCEngine) runGC() {}

// --- Internal helpers ---

// Returns the VersionChain for key, creating it if absent. MUST be called with e.mu held for writing.
func (e *MVCCEngine) getOrCreateChain(key string) *VersionChain {
	chain, ok := e.data[key]
	if !ok {
		chain = &VersionChain{}
		e.data[key] = chain
	}
	return chain
}

// Returns a fresh copy of b. Always use this before storing a []byte you received from a caller,
// and before returning a []byte to a caller.
func makeCopy(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// --- VersionChain helpers ---
// All of these MUST be called with chain.mu held.

// Returns the most recent non-deleted version, or nil.
func (c *VersionChain) latestLocked() *Version {
	for i := len(c.versions) - 1; i >= 0; i-- {
		if !c.versions[i].Deleted {
			return &c.versions[i]
		}
	}
	return nil
}

// Returns the latest version with Num <= max, or nil. Used for snapshot reads: "what was the value of this key at version V?"
func (c *VersionChain) atOrBeforeVersionLocked(max uint64) *Version {
	for i := len(c.versions) - 1; i >= 0; i-- {
		v := &c.versions[i]
		if v.Num <= max && !v.Deleted {
			return v
		}
	}
	return nil
}

// Returns the latest version with Num >= min, or nil. Used for monotonic reads: "give me a version at least as new as V."
func (c *VersionChain) atOrAfterVersionLocked(min uint64) *Version {
	// Walk backward to find the most recent version that is >= min.
	for i := len(c.versions) - 1; i >= 0; i-- {
		v := &c.versions[i]
		if v.Num >= min && !v.Deleted {
			return v
		}
	}
	return nil
}
