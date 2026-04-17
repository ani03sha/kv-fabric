package store

import (
	"time"
)

// Observability surface for the garbage collector.
type GCStats struct {
	LastRunAt           time.Time
	LastRunDuration     time.Duration
	VersionsCollected   int64  // cumulative total across all GC runs
	BytesFreed          int64  // cumulative bytes freed
	OldestPinnedVersion uint64 // the pin currently blocking GC (0 is unblocked)
	BlockedByTxn        bool   // true when a transaction pin is constraining the horizon
}

// Returns the latest GC statistics. Safe to call from any goroutine.
func (e *MVCCEngine) GCStats() GCStats {
	e.gcStatsMu.Lock()
	defer e.gcStatsMu.Unlock()
	return e.gcStats
}

// Executes one full GC cycle. Called by gcLoop on each ticker tick.
// How it works:
//  1. Compute the effective horizon (min of replication horizon and oldest pin)
//  2. Snapshot chain pointers (releases engine lock quickly)
//  3. For each chain, collect versions below the horizon
func (e *MVCCEngine) runGC() {
	start := time.Now()

	horizon, oldestPin, blockedByTxn := e.computeEffectiveHorizon()
	if horizon == 0 {
		// SetGCHorizon has never been called - replication not yet active.
		// Nothing to collect; avoid a spurious GC run
		return
	}

	// Snapshot chain pointers under engine read lock. We release the lock before doing actual collection so that
	// writes to the engine are not blocked for the duration of GC.
	e.mu.RLock()
	chains := make([]*VersionChain, 0, len(e.data))
	for _, chain := range e.data {
		chains = append(chains, chain)
	}
	e.mu.RUnlock()

	var totalCollected int64
	var totalBytesFreed int64

	for _, chain := range chains {
		chain.mu.Lock()
		c, b := chain.collectGarbage(horizon)
		chain.mu.Unlock()
		totalCollected += c
		totalBytesFreed += b
	}

	// Update engine level stats
	e.mu.Lock()
	e.stats.TotalVersions -= totalCollected
	e.stats.GCCyclesTotal++
	if oldestPin > 0 {
		e.stats.OldestVersion = oldestPin
	}
	e.mu.Unlock()

	// Update GC specific stats
	e.gcStatsMu.Lock()
	e.gcStats.LastRunAt = time.Now()
	e.gcStats.LastRunDuration = time.Since(start)
	e.gcStats.VersionsCollected += totalCollected
	e.gcStats.BytesFreed += totalBytesFreed
	e.gcStats.OldestPinnedVersion = oldestPin
	e.gcStats.BlockedByTxn = blockedByTxn
	e.gcStatsMu.Unlock()
}

// Returns the lowest version number below which // it is safe to collect, the oldest active transaction pin, and whether
// any transaction is currently blocking collection.
//
// The effective horizon is: min(replicationHorizon, allActivePins). This ensures neither replication catch-up nor
// active transactions lose the versions they depend on.
func (e *MVCCEngine) computeEffectiveHorizon() (horizon uint64, oldestPin uint64, blocked bool) {
	// Read both the base horizon and all chain pointers in a single engine read lock.
	// We then examine each chain's pin without the engine lock.
	e.mu.RLock()
	horizon = e.gcHorizon
	chains := make([]*VersionChain, 0, len(e.data))
	for _, chain := range e.data {
		chains = append(chains, chain)
	}
	e.mu.RUnlock()

	if horizon == 0 {
		return // replication hasn't set a horizon yet
	}

	for _, chain := range chains {
		chain.mu.RLock()
		pinned := chain.pinned
		chain.mu.RUnlock()

		if pinned == 0 {
			continue // no transaction is pinning this chain
		}

		blocked = true

		// Track the globally oldest pin for observability
		if oldestPin == 0 || pinned < oldestPin {
			oldestPin = pinned
		}

		// The effective horizon cannot exceed any active pin. If a transaction is at version 50 and our horizon is
		// at 200, we can only safely collect below version 50.
		if pinned < horizon {
			horizon = pinned
		}
	}

	return
}

// Removes versions from this chain that are safely below horizon.
//
// Rules:
//   - Always keep the latest version (index len-1), even if below horizon.
//     It is the current value of the key — deleting it would make the key vanish.
//   - Keep all versions with Num >= horizon (may be needed for snapshot reads).
//   - Collect all versions with Num < horizon that are not the latest.
//
// Returns the count and bytes of versions removed. MUST be called with chain.mu held for writing.
func (c *VersionChain) collectGarbage(horizon uint64) (collected int64, bytesFreed int64) {
	if len(c.versions) == 0 {
		return
	}

	latestIndex := len(c.versions) - 1

	// Allocate a fresh slice. We don't reuse the old backing array because the old array
	// would still hold references to the collection Version values, preventing Go's runtime
	// GC freeing their Value []byte fields.
	newVersions := make([]Version, 0, len(c.versions))

	for i, v := range c.versions {
		isLatest := i == latestIndex
		if isLatest || v.Num >= horizon {
			newVersions = append(newVersions, v)
		} else {
			collected++
			bytesFreed += int64(len(v.Value))
		}
	}

	c.versions = newVersions
	return
}
