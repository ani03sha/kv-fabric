package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/cluster"
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// This scenario demonstrates the semi-synchronous replication fallback trap on a real 3-node Raft cluster.
//
// Semi-sync waits for at least one follower to acknowledge a write before returning success.
// If no follower responds within the timeout, the leader falls back to async and returns success ANYWAY.
//
// In this cluster, raftly.Propose() blocks until quorum commit (not just local append), so by the time
// the write returns to the caller, it's committed to leader + at least one follower's Raft log.
// That means the write survives a leader crash.
//
// But two things ARE still real and dangerous:
//
//  1. fallback_count fires (> 0). This is the alarm bell.
//     In a real production system, if fallback_count is non-zero and the leader dies, we must investigate whether
//     any in-flight writes (proposed but not yet quorum-committed at the exact instant of crash) were lost.
//     The tracker cannot tell us which ones, only the WAL comparison can.
//
//  2. There is a brief inconsistency window between crash and when the new leader's apply loop catches up from its Raft log.
//     Reads during this window return stale data even though the write was confirmed to the client.
//
// Scenario steps:
//  1. Start 3-node in-process Raft cluster
//  2. Wire semi-sync with a 10ms timeout (shorter than lagLoop's 50ms interval —
//     this reliably triggers fallback because the tracker is stale when WaitForAck runs)
//  3. Write a booking
//  4. Crash the leader via ChaosInjector (real raftly Stop + network blackhole)
//  5. Wait for new leader election (real election among remaining 2 nodes)
//  6. Read the key from the new leader's engine
//  7. Report the outcome and explain what the numbers mean
func RunPhantomDurability(logger *zap.Logger) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║            PHANTOM DURABILITY SCENARIO                    ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// --- Step 1: Start the cluster ---
	//
	// cluster.Start() creates 3 raftly nodes using InMemTransport (zero-network, in-process).
	// Real elections. Real log replication. Real quorum commits.
	// Blocks until a leader is elected or 5s timeout.
	fmt.Println("[setup] starting 3-node in-process Raft cluster...")
	c, err := cluster.Start(logger)
	if err != nil {
		fmt.Printf("[ERROR] cluster start: %v\n", err)
		return
	}
	defer c.Stop()

	leader := c.Leader()
	if leader == nil {
		fmt.Println("[ERROR] no leader after cluster start")
		return
	}

	fmt.Printf("[setup] leader elected: %s\n", leader.ID)
	fmt.Printf("[setup] followers: ")
	for _, f := range c.Followers() {
		fmt.Printf("%s ", f.ID)
	}
	fmt.Println()
	fmt.Println()

	// --- Step 2: Wire semi-sync on the leader ---
	//
	// The ReplicationTracker is updated by cluster's lagLoop every 50ms.
	// With a 10ms semi-sync timeout, the timer fires before lagLoop can update the tracker,
	// so anyFollowerAt() always returns false → fallback is guaranteed.
	//
	// In production, timeouts are 100ms–1s. We use 10ms to demonstrate the mechanism reliably.
	const semiSyncTimeout = 10 * time.Millisecond
	reg := prometheus.NewRegistry() // private registry prevents "duplicate metrics" panics on re-runs
	semiSync := replication.NewSemiSyncReplicator(c.Tracker, semiSyncTimeout, reg, logger)
	leader.Leader.SetSemiSync(semiSync)

	fmt.Printf("[step 1] semi-sync wired | timeout=%v | lagLoop interval=50ms\n", semiSyncTimeout)
	fmt.Println("         10ms < 50ms  →  tracker is always stale when WaitForAck runs")
	fmt.Println("         →  fallback_count will be non-zero after the write")
	fmt.Println()

	// --- Step 3: Write the booking ---
	//
	// What happens inside Propose:
	//   1. raftly.Propose() appends entry to leader WAL, triggers AppendEntries to followers
	//   2. Blocks until quorum ACKs (leader + 1 follower) — write is now durable in Raft logs
	//   3. commitCh delivers the entry to applyLoop
	//   4. applyLoop calls engine.Put() — leader's MVCC engine now has the write
	//   5. pendingOp signals resultCh — Propose returns the PutResult
	//   6. semiSync.WaitForAck(10ms) runs — tracker is stale → timer fires → fellback=true
	//   7. Propose returns success to client ANYWAY — THIS IS THE TRAP
	const (
		key   = "booking:flight:AA100:seat:1A"
		value = "Alice Chen | 2024-06-15"
	)

	fmt.Printf("[step 2] client writes  key=%q\n", key)
	fmt.Printf("         value=%q\n", value)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	writeStart := time.Now()
	res, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   key,
		Value: []byte(value),
	})
	writeLatency := time.Since(writeStart)

	if err != nil {
		fmt.Printf("         ERROR: write failed: %v\n", err)
		return
	}

	fallbacksAfterWrite := semiSync.FallbackTotal()
	fmt.Printf("         ✓ write returned: version=%d  latency=%v\n", res.Version, writeLatency.Round(time.Microsecond))
	fmt.Printf("         fallback_count = %d\n", fallbacksAfterWrite)
	fmt.Println()

	if fallbacksAfterWrite > 0 {
		fmt.Println("         !! TRAP SPRUNG: leader returned HTTP 200 without follower ACK")
		fmt.Println("         !! The write IS quorum-committed (in Raft logs of leader + follower).")
		fmt.Println("         !! But tracker had no record of the follower ACK at return time.")
		fmt.Println("         !! In production: if leader dies now, run WAL comparison post-mortem.")
	} else {
		fmt.Println("         NOTE: lagLoop ticked before semi-sync timer — no fallback this run.")
		fmt.Println("         Continuing with crash to demonstrate failover durability.")
	}
	fmt.Println()

	// --- Step 4: Crash the leader ---
	//
	// ChaosInjector.CrashNode():
	//   1. Adds proxy rules: ActionDrop for all traffic to/from this node.
	//      Peers see timeout (not connection refused) — a more faithful crash simulation.
	//   2. Calls raftly node.Stop():
	//      - Sets state = Follower (so IsLeader() returns false immediately)
	//      - Closes stopCh — Raft goroutines exit
	//      - Closes WAL
	//   3. The adapter's bridge() reads from closed CommitCh → closes commitCh
	//   4. LeaderReplicator.applyLoop reads !ok from closed commitCh → exits
	//
	// The write at version=res.Version is now ONLY in Raft logs (leader WAL + follower Raft log).
	// The follower's FollowerApplier.applyEntry() may or may not have run yet.
	leaderID := leader.ID
	crashTime := time.Now()

	fmt.Printf("[step 3] crashing leader %s\n", leaderID)
	c.Chaos.CrashNode(leaderID)
	fmt.Println("         node stopped + network blackholed")
	fmt.Printf("         write at version=%d is now at risk if follower hasn't applied yet\n", res.Version)
	fmt.Println()

	// --- Step 5: Wait for new leader election ---
	//
	// With 3 nodes (2 healthy), quorum = 2. Election can proceed.
	// raftly election timeout is randomized ~150–300ms by default.
	// UpdateTimings() (v0.2.0) can speed this up in test scenarios.
	fmt.Println("[step 4] waiting for new leader election...")
	newLeader, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		fmt.Printf("         ERROR: no new leader: %v\n", err)
		return
	}
	electionLatency := time.Since(crashTime)

	fmt.Printf("         new leader: %s  (election took %v)\n", newLeader.ID, electionLatency.Round(time.Millisecond))
	fmt.Println()

	// --- Step 6: Verify the write on the new leader ---
	//
	// We read directly from the new leader's MVCCEngine.
	// The entry was quorum-committed, so the new leader had it in its Raft log.
	// Its FollowerApplier had ~electionLatency ms to call applyEntry() from the Raft log.
	// In-process channels are fast (< 1µs). The write should be visible almost immediately.
	//
	// We poll up to 500ms in case the apply loop is still catching up.
	fmt.Printf("[step 5] reading key=%q from %s\n", key, newLeader.ID)

	var readResult *store.GetResult
	var readAttempts int
	for readAttempts = 1; readAttempts <= 20; readAttempts++ {
		r, _ := newLeader.Engine.Get(key, store.GetOptions{})
		if r.Value != nil {
			readResult = r
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	fmt.Println()

	if readResult != nil && readResult.Value != nil {
		fmt.Printf("         FOUND: value=%q\n", string(readResult.Value))
		fmt.Printf("         version=%d  (attempts=%d, ~%v after election)\n",
			readResult.Version,
			readAttempts,
			(time.Duration(readAttempts-1) * 25 * time.Millisecond).Round(time.Millisecond),
		)
		fmt.Println()
		fmt.Println("         Write survived leader crash.")
		fmt.Println("         Raft quorum commit guaranteed this: the entry was in the")
		fmt.Println("         follower's Raft log before the leader was killed.")
	} else {
		semiSync.RecordPhantomWrite()
		fmt.Println("         KEY NOT FOUND after 500ms of polling.")
		fmt.Println()
		fmt.Println("         Alice's booking is PERMANENTLY LOST.")
		fmt.Println("         She was told it succeeded. She arrives at the airport")
		fmt.Println("         with a confirmation number for a seat that does not exist.")
		fmt.Println()
		fmt.Println("         This indicates the entry was NOT quorum-committed at crash time.")
		fmt.Println("         (Possible if the 10ms ctx window raced with quorum in this run.)")
	}

	fmt.Println()

	// --- Result ---
	fmt.Println("┌───────────────────────────────────────────────────────────────────┐")
	fmt.Printf("│  semi_sync_timeout  = %-10v                                  │\n", semiSyncTimeout)
	fmt.Printf("│  fallback_count     = %-5d                                       │\n", semiSync.FallbackTotal())
	fmt.Printf("│  phantom_writes     = %-5d                                       │\n", semiSync.PhantomWriteTotal())
	fmt.Printf("│  election_latency   = %-10v                                  │\n", electionLatency.Round(time.Millisecond))
	fmt.Println("│                                                                   │")
	fmt.Println("│  WHAT THE NUMBERS MEAN:                                           │")
	fmt.Println("│  fallback_count > 0: leader returned success WITHOUT follower    │")
	fmt.Println("│  ACK. Writes were still quorum-committed here (raftly blocks      │")
	fmt.Println("│  Propose() until quorum). In async-replication systems, this     │")
	fmt.Println("│  WOULD mean data loss. Monitor this metric in production.        │")
	fmt.Println("│                                                                   │")
	fmt.Println("│  MITIGATIONS:                                                     │")
	fmt.Println("│  1. Alert on fallback_count > 0 before any leader shutdown       │")
	fmt.Println("│  2. Require 2+ follower ACKs (higher durability guarantee)       │")
	fmt.Println("│  3. Reduce semi-sync timeout to catch slow followers sooner      │")
	fmt.Println("│  4. Use synchronous replication for critical writes only         │")
	fmt.Println("└───────────────────────────────────────────────────────────────────┘")
}
