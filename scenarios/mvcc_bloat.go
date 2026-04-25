package scenarios

import (
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// This scenario demonstrates how MVCC version accumulation bloats memory, and how GC reclaims it.
//
// Every Put() creates a new version — the old one is never overwritten. Under high write rates,
// a single hot key accumulates versions fast. GC is the only reclamation mechanism, and it is
// gated on two conditions:
//  1. The replication horizon: all followers must have applied version N before GC can collect
//     versions below N (otherwise a lagging follower would read a version that no longer exists).
//  2. No active transaction pins a version below N.
//
// This scenario shows:
//   - How quickly versions accumulate
//   - How PinVersion blocks GC (simulating a long-running analytics query)
//   - How the horizon and pin interact to produce the effective GC boundary
func RunMVCCBloat(logger *zap.Logger) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║              MVCC BLOAT SCENARIO                          ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// Short GC interval so we can observe GC effects within the scenario duration.
	// Production default is 30s. We use 500ms here.
	const gcInterval = 500 * time.Millisecond
	engine := store.NewMVCCEngine("node-1", gcInterval)
	defer engine.Close()

	fmt.Printf("[setup]  GC interval = %v\n\n", gcInterval)

	// --- Phase 1: accumulate versions ---
	//
	// A hot key is one that gets written frequently. For e.g., Inventory counters, request rates, and live auction prices.
	// Each write adds one Version struct to the VersionChain. With no GC, this chain grows without bound.
	const hotKey = "product:inventory:widget-X"
	const numWrites = 500

	fmt.Printf("[phase 1] writing %d versions of key %q...\n", numWrites, hotKey)
	t0 := time.Now()
	for i := 1; i <= numWrites; i++ {
		_, _ = engine.Put(hotKey, []byte(fmt.Sprintf("stock=%d", numWrites-i+1)),
			store.PutOptions{LogIndex: uint64(i)})
	}

	// A second, less-active key. Its versions will survive GC because they are above
	// the horizon (version numbers 1001-1050, all > the GC horizon we'll set at 490).
	const coldKey = "product:inventory:gadget-Y"
	for i := 1; i <= 50; i++ {
		_, _ = engine.Put(coldKey, []byte(fmt.Sprintf("stock=%d", 50-i+1)),
			store.PutOptions{LogIndex: uint64(1000 + i)})
	}

	stats := engine.Stats()
	fmt.Printf("          %d puts in %v\n", numWrites+50, time.Since(t0))
	fmt.Printf("          TotalKeys     = %d\n", stats.TotalKeys)
	fmt.Printf("          TotalVersions = %d   ← all writes retained; GC horizon not set yet\n", stats.TotalVersions)
	fmt.Println()

	// --- Phase 2: pin a version (active analytics transaction) ---
	//
	// Imagine an analytics job that starts a scan at version 300. It calls PinVersion
	// to anchor its read snapshot: "do not GC any version at or above 300 for this key."
	// The GC horizon can't advance past 300 for this key as long as the pin holds.
	const pinVersion = uint64(300)
	engine.PinVersion(hotKey, pinVersion)
	fmt.Printf("[phase 2] analytics transaction pins version %d on %q\n", pinVersion, hotKey)
	fmt.Println("          effective GC horizon for this key is clamped to 300")
	fmt.Println()

	// --- Phase 3: advance GC horizon and run GC ---
	//
	// In production: the replication layer calls SetGCHorizon when it determines that all followers have applied log index N.
	// Here we call it directly.
	//
	// We set horizon=490 but the pin at 300 will clamp the effective horizon to 300. So GC will collect versions 1–299
	// for the hot key (299 versions). Versions 300–500 are preserved (300 is pinned; 301–500 are above the effective horizon).
	const horizon = uint64(490)
	engine.SetGCHorizon(horizon)
	fmt.Printf("[phase 3] replication horizon advanced to %d\n", horizon)
	fmt.Printf("          effective horizon for %q = min(%d, pin=%d) = %d\n",
		hotKey, horizon, pinVersion, pinVersion)
	fmt.Println("          GC may collect versions < 300 for hot key")
	fmt.Println("          waiting for GC ticker...")
	fmt.Println()

	// Wait long enough for the GC goroutine to fire at least once.
	time.Sleep(gcInterval + 300*time.Millisecond)

	stats = engine.Stats()
	gcStats := engine.GCStats()
	fmt.Println("[phase 3 result] after first GC run:")
	fmt.Printf("          TotalVersions     = %d   (was %d — collected %d)\n",
		stats.TotalVersions, numWrites+50, gcStats.VersionsCollected)
	fmt.Printf("          GCCyclesTotal     = %d\n", stats.GCCyclesTotal)
	fmt.Printf("          OldestPinnedVer   = %d\n", gcStats.OldestPinnedVersion)
	fmt.Printf("          BlockedByTxn      = %v\n", gcStats.BlockedByTxn)
	fmt.Printf("          LastRunDuration   = %v\n", gcStats.LastRunDuration)
	fmt.Println()

	// Verify: the pinned version is still readable even though versions below it were GC'd.
	pinnedRead, _ := engine.GetAtVersion(hotKey, pinVersion)
	latestRead, _ := engine.Get(hotKey, store.GetOptions{})
	fmt.Printf("          GetAtVersion(%q, %d) -> value=%q  <- pinned version preserved\n", hotKey, pinVersion, string(pinnedRead.Value))
	fmt.Printf("          Get(%q) -> value=%q  version=%d  <- latest always preserved\n", hotKey, string(latestRead.Value), latestRead.Version)
	fmt.Println()

	// --- Phase 4: unpin and run GC again ---
	//
	// The analytics job finishes. It calls UnpinVersion. Now the effective horizon
	// is the full 490. GC can collect versions 300–489 for the hot key (190 more).
	engine.UnpinVersion(hotKey, pinVersion)
	fmt.Printf("[phase 4] analytics transaction done — unpinning version %d\n", pinVersion)
	fmt.Println("          effective horizon is now 490 — no active pins")
	fmt.Println("          GC may now collect versions 300–489 for hot key")
	fmt.Println("          waiting for GC ticker...")
	fmt.Println()

	time.Sleep(gcInterval + 300*time.Millisecond)

	stats = engine.Stats()
	gcStats = engine.GCStats()
	fmt.Println("[phase 4 result] after second GC run:")
	fmt.Printf("          TotalVersions     = %d\n", stats.TotalVersions)
	fmt.Printf("          VersionsCollected = %d  (cumulative)\n", gcStats.VersionsCollected)
	fmt.Printf("          BytesFreed        = %d bytes (cumulative)\n", gcStats.BytesFreed)
	fmt.Println()

	// Expected: hot key retains versions 490–500 = 11 versions
	//           cold key retains versions 1001–1050 = 50 versions (all above horizon)
	//           Total: 61 versions

	fmt.Println("┌──────────────────────────────────────────────────────────────┐")
	fmt.Println("│  LESSON:                                                     │")
	fmt.Println("│  Version accumulation rate = write_rate × GC_interval.      │")
	fmt.Println("│  A key written 10k/s with a 30s GC interval accumulates     │")
	fmt.Println("│  300,000 versions before GC runs. Each Version struct        │")
	fmt.Println("│  holds a []byte — that's real heap pressure.                │")
	fmt.Println("│                                                              │")
	fmt.Println("│  Long-running transactions extend the GC horizon for ALL    │")
	fmt.Println("│  keys that the transaction pinned, not just the ones it      │")
	fmt.Println("│  actually reads. Scope your transaction pins carefully.      │")
	fmt.Println("│                                                              │")
	fmt.Println("│  ALERT ON: TotalVersions / TotalKeys > threshold.           │")
	fmt.Println("│  ALERT ON: OldestPinnedVersion more than N minutes old.     │")
	fmt.Println("└──────────────────────────────────────────────────────────────┘")
}
