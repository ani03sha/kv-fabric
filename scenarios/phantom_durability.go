package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// This function demonstrates the phantom durability trap.
//
// Semi-synchronous replication waits for a follower ACK before returning success. But if no follower responds
// within the timeout, the leader falls back to async and returns success ANYWAY. If the leader then crashes,
// the write is permanently lost. The client was told it succeeded. This is the phantom write.
//
// The only observable signal before the crash is the fallback_count metric going non-zero.
// If fallback_count > 0 when a leader dies, expect phantom writes in the post-mortem.
func RunPhantomDurability(logger *zap.Logger) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║            PHANTOM DURABILITY SCENARIO                    ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// --- Setup ---
	//
	// leaderEngine is the leader's MVCC store. In production it is driven by the Raft apply loop.
	// Here we drive it directly by passing explicit LogIndex values.
	leaderEngine := store.NewMVCCEngine("leader", 30*time.Second)

	tracker := replication.NewReplicationTracker()
	tracker.Track("follower-1") // registers follower; starts at MatchIndex=0

	// Use a private Prometheus registry so repeated scenario runs don't panic with "duplicate metrics" errors.
	reg := prometheus.NewRegistry()

	// This determines how long the leader waits for a follower ACK before falling back.
	// Production values are 100ms to 1s. We use 60ms so the scenario completes quickly.
	const semiSyncTimeout = 60 * time.Millisecond
	semiSync := replication.NewSemiSyncReplicator(tracker, semiSyncTimeout, reg, logger)

	fmt.Printf("[setup]  1 leader + 1 follower | semi-sync timeout = %v\n\n", semiSyncTimeout)

	// --- Step 1: client writes a booking ---
	//
	// In production: client sends PUT /v1/keys/booking:seat:1A -> handler calls leader.Propose() -> Raft commits ->
	// apply loop calls engine.Put(). Here we call engine.Put() directly with the Raft log index we'd have gotten.
	const key = "booking:flight:AA100:seat:1A"
	const value = "Alice Chen | 2024-06-15"

	fmt.Printf("[step 1] client writes  key=%q  value=%q\n", key, value)

	res, err := leaderEngine.Put(key, []byte(value), store.PutOptions{LogIndex: 42})
	if err != nil {
		fmt.Printf("         ERROR: %v\n", err)
		return
	}

	// Advance the tracker's commit index so the lag computation is accurate.
	// In production the apply loop calls tracker.UpdateLeaderCommitIndex after each entry.
	tracker.UpdateLeaderCommitIndex(32)

	fmt.Printf("         write applied to leader engine at version=%d\n", res.Version)
	fmt.Println("         Raft quorum committed (on leader only — single-node scenario)")
	fmt.Println()

	// The follower has NOT replicated entry 32 yet. tracker.UpdateFollower("follower-1", 32) is intentionally NOT called.
	// Its MatchIndex stays at 0, which is < 32.

	// --- Step 2: semi-sync waits for follower ACK ---
	//
	// anyFollowerAt(32) checks: does any follower have MatchIndex >= 32? Answer: No. follower-1 is at 0.
	// So we enter the polling loop. Every 5ms we re-check. After semiSyncTimeout, timer.C fires and we fall back.
	fmt.Printf("[step 2] semi-sync waiting up to %v for follower ACK...\n", semiSyncTimeout)

	// The context timeout is generous because we want the semiSync internal timer to fire first, not the ctx deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	acked, fellback := semiSync.WaitForAck(ctx, res.Version)

	switch {
	case acked:
		fmt.Println("         follower ACK received — write is durable (this should not happen in scenario)")
	case fellback:
		fmt.Println("         !! TIMEOUT — no follower ACK within semi-sync window")
		fmt.Println("         !! Leader falls back to async replication")
		fmt.Println("         !! Leader returns HTTP 200 to client  ←  THE TRAP IS SPRUNG")
		fmt.Printf("         !! fallback_count = %d\n", semiSync.FallbackTotal())
	}
	fmt.Println()

	// At this exact moment:
	//   > client received HTTP 200 and believes write is durable
	//   > write exists ONLY on the leader's MVCC engine in memory
	//   > follower has never seen log entry 32
	//   > if the leader process dies right now, the write is gone

	// --- Step 3: leader crashes ---
	//
	// We simulate a crash by closing the engine. The OS reclaims the process memory.
	// No graceful shutdown. No snapshot. No replication of pending entries.
	// The MVCC engine including version 32 of our booking key vanishes.
	fmt.Println("[step 3] leader crashes — engine discarded (simulated process kill)")
	leaderEngine.Close()
	// leaderEngine is now a dangling pointer. Do not use it again.
	fmt.Println()

	// --- Step 4: follower becomes new leader ---
	//
	// The follower was at MatchIndex=0. It never applied log entry 32.
	// Raft elects it as new leader (it has the longest log the quorum can agree on,
	// which does not include entry 32 — the old leader had accepted it locally but
	// had not replicated it to quorum before dying).
	//
	// We simulate the new leader by creating a fresh engine. It has NO data.
	fmt.Println("[step 4] follower elected as new leader")
	newLeaderEngine := store.NewMVCCEngine("new-leader", 30*time.Second)
	defer newLeaderEngine.Close()
	fmt.Println("         new leader engine started fresh (follower had never applied entry 32)")
	fmt.Println()

	// --- Step 5: verify the phantom write ---
	fmt.Printf("[step 5] client reads from new leader: GET %q\n", key)
	result, _ := newLeaderEngine.Get(key, store.GetOptions{})

	if result.Value == nil {
		semiSync.RecordPhantomWrite()
		fmt.Println("         result: KEY NOT FOUND")
		fmt.Println()
		fmt.Println("         Alice's booking is PERMANENTLY LOST.")
		fmt.Println("         She was told it succeeded. She arrives at the airport")
		fmt.Println("         with a confirmation email for a seat that does not exist.")
	} else {
		fmt.Printf("         result: %q — write survived (unexpected in this scenario)\n", string(result.Value))
	}
	fmt.Println()

	// ── Result ────────────────────────────────────────────────────────────────
	fmt.Println("┌──────────────────────────────────────────────────────────────┐")
	fmt.Printf("│  fallback_count  = %-5d                                      │\n", semiSync.FallbackTotal())
	fmt.Printf("│  phantom_writes  = %-5d                                      │\n", semiSync.PhantomWriteTotal())
	fmt.Println("│                                                              │")
	fmt.Println("│  MITIGATIONS:                                                │")
	fmt.Println("│  1. Alert on fallback_count > 0 before any leader shutdown  │")
	fmt.Println("│  2. Require 2+ follower ACKs (higher durability cost)       │")
	fmt.Println("│  3. Reduce semi-sync timeout to catch slow followers sooner │")
	fmt.Println("│  4. Use synchronous replication (kills tail latency)        │")
	fmt.Println("└──────────────────────────────────────────────────────────────┘")
}
