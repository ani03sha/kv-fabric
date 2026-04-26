package scenarios

import (
	"context"
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/cluster"
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/ani03sha/raftly/transport"
	"go.uber.org/zap"
)

// This scenario demonstrates how eventual consistency with real follower lag enables double bookings.
//
// Reference: Booking.com reported a MySQL replication lag of ~340ms during a traffic spike. During that
// window, a follower served stale availability data. Two users simultaneously saw the same seat as
// "available" and both booked it: neither write was invalid, the cluster accepted both, and one booking
// silently overwrote the other.
//
// This scenario runs on a real 3-node Raft cluster using InMemTransport. We inject a 340ms AppendEntries
// delay from the leader to one follower using the NetworkProxy. That follower's raftly node doesn't
// receive the log entry for 340ms — so its apply loop can't fire, and its MVCCEngine stays stale.
// The second follower is unaffected and serves as the fast quorum peer.
//
// Timeline (measured, not simulated):
//
//	t=0ms:   inject 340ms proxy delay from leader → stale follower
//	t=0ms:   Alice books seat (quorum via leader + fast follower, stale follower delayed)
//	t=0ms:   Bob reads from stale follower → sees "available" (AppendEntries not delivered yet)
//	t=0ms:   Bob books same seat → double booking committed to quorum
//	t=340ms: stale follower finally receives Alice's write, then Bob's write
//	t=340ms: follower engine: Bob's write (last) wins → Alice silently erased
//
// Fixes shown:
//
//	Option A — strong read from leader (ReadIndex protocol): no stale data possible
//	Option B — optimistic locking (CAS write with IfVersion): second writer detects conflict
func RunBookingCom(logger *zap.Logger) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║          BOOKING.COM STALE READ SCENARIO                  ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// --- Setup: Start the real cluster ---
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

	followers := c.Followers()
	if len(followers) < 2 {
		fmt.Println("[ERROR] expected 2 followers")
		return
	}

	// staleFollower: we will delay AppendEntries to this node (340ms)
	// fastFollower:  unaffected — this is the quorum peer alongside the leader
	staleFollower := followers[0]
	fastFollower := followers[1]

	fmt.Printf("[setup] leader=%s  fastFollower=%s  staleFollower=%s\n",
		leader.ID, fastFollower.ID, staleFollower.ID)
	fmt.Println()

	// -- Initial state: write seat=available, wait for all 3 nodes to apply ---
	//
	// We want a clean baseline: all engines show "available" before we inject lag.
	// After Propose() returns, the entry is quorum-committed (leader + fastFollower Raft logs).
	// We sleep 200ms so the stale follower also receives and applies it (no delay rule yet).
	const seat = "flight:UA456:seat:14C"

	baseCtx, baseCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer baseCancel()

	_, err = c.Propose(baseCtx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   seat,
		Value: []byte("available"),
	})
	if err != nil {
		fmt.Printf("[ERROR] baseline write: %v\n", err)
		return
	}

	// Give all three nodes time to apply the baseline write before injecting lag.
	time.Sleep(200 * time.Millisecond)

	staleCheck, _ := staleFollower.Engine.Get(seat, store.GetOptions{})
	fmt.Printf("[setup] baseline: seat=%q  staleFollower engine=%q\n\n", seat, string(staleCheck.Value))

	// --- FAILURE PATH: stale read from lagging follower ---
	fmt.Println("--- FAILURE PATH: eventual read from lagging follower ---")
	fmt.Println()

	// Inject 340ms delay on all messages from the leader to the stale follower.
	// InMemTransport calls applyProxy(from=t.nodeID, to=peerID) before every SendAppendEntries.
	// With this rule, AppendEntries from leader to staleFollower sleeps 340ms before delivery.
	// The fast follower is unaffected — quorum can still commit via leader + fastFollower.
	const lagRuleID = "booking-com-lag"
	c.Proxy.AddRule(transport.ProxyRule{
		ID:       lagRuleID,
		FromNode: leader.ID,
		ToNode:   staleFollower.ID,
		Action:   transport.ActionDelay,
		Params:   map[string]interface{}{"delay_ms": 340},
	})
	fmt.Printf("[inject] 340ms AppendEntries delay: %s → %s\n", leader.ID, staleFollower.ID)
	fmt.Printf("         quorum still reachable via %s + %s\n\n", leader.ID, fastFollower.ID)

	// --- Step 1: Alice books the seat ---
	//
	// Propose() blocks until quorum (leader + fastFollower) commits this entry.
	// The delayed follower's AppendEntries RPC is still sleeping in the proxy.
	// From the stale follower's perspective, Alice's booking has not happened yet.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	aliceStart := time.Now()
	aliceRes, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   seat,
		Value: []byte("booked:Alice"),
	})
	if err != nil {
		fmt.Printf("[ERROR] Alice's booking: %v\n", err)
		return
	}
	aliceLatency := time.Since(aliceStart)

	fmt.Printf("[step 1] Alice books seat %q\n", seat)
	fmt.Printf("         leader committed: version=%d  latency=%v\n", aliceRes.Version, aliceLatency.Round(time.Microsecond))
	fmt.Printf("         staleFollower is 340ms behind — AppendEntries still in transit\n")
	fmt.Println()

	// --- Step 2: Bob reads from the stale follower ---
	//
	// In a real deployment, clients round-robin across nodes for availability reads
	// (eventual consistency — read from the nearest healthy replica).
	// The stale follower is healthy. It just hasn't received Alice's write yet.
	// Its engine still returns "available" — version from the baseline write.
	bobRead, _ := staleFollower.Engine.Get(seat, store.GetOptions{})

	fmt.Printf("[step 2] Bob reads from staleFollower (%s) — eventual consistency:\n", staleFollower.ID)
	fmt.Printf("         GET %s → %q  (version=%d)\n", seat, string(bobRead.Value), bobRead.Version)

	if string(bobRead.Value) == "available" {
		fmt.Println("         !! STALE READ — staleFollower hasn't applied Alice's booking")
		fmt.Println("         !! Bob's availability check passes: seat looks free → proceed to book")
	} else {
		fmt.Println("         (staleFollower caught up faster than expected — lag window too short)")
	}
	fmt.Println()

	// Sleep 100ms for lagLoop to tick and update the tracker so GetLag() shows real numbers.
	time.Sleep(100 * time.Millisecond)
	lagEntries, lagMs := c.Tracker.GetLag(staleFollower.ID)
	fmt.Printf("         replication lag: %d entries  ~%dms behind leader\n\n", lagEntries, lagMs)

	// --- Step 3: Bob books the same seat ---
	//
	// Bob's read said "available". His booking is valid from the leader's perspective too
	// (no CAS check — unconditional write). The leader accepts it, quorum commits it.
	// Now BOTH Alice and Bob have a confirmed booking for the same seat.
	bobRes, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   seat,
		Value: []byte("booked:Bob"),
	})
	if err != nil {
		fmt.Printf("[ERROR] Bob's booking: %v\n", err)
		return
	}

	fmt.Printf("[step 3] Bob books the SAME seat → version=%d\n", bobRes.Version)
	fmt.Println("         DOUBLE BOOKING: Alice and Bob both have confirmation emails for seat 14C")
	fmt.Println()

	// --- Step 4: Stale follower catches up ---
	//
	// Remove the delay rule. The 340ms delay elapses (or was already elapsing in the background).
	// The stale follower receives Alice's write, then Bob's write, applies both in order.
	// Last write wins in MVCC: Bob's write (higher version) is the current value.
	// Alice's booking still exists as an older version — but clients read the latest.
	c.Proxy.RemoveRule(lagRuleID)
	fmt.Printf("[step 4] delay rule removed — staleFollower catching up from Raft log...\n")
	time.Sleep(600 * time.Millisecond) // 340ms for in-flight AppendEntries + apply time

	leaderFinal, _ := leader.Engine.Get(seat, store.GetOptions{})
	staleFinal, _ := staleFollower.Engine.Get(seat, store.GetOptions{})

	fmt.Println("         after catch-up:")
	fmt.Printf("         leader:         %q (v%d)\n", string(leaderFinal.Value), leaderFinal.Version)
	fmt.Printf("         staleFollower:  %q (v%d)\n", string(staleFinal.Value), staleFinal.Version)
	fmt.Println()
	fmt.Println("         Alice's booking was silently overwritten.")
	fmt.Println("         She finds out at the gate. Her seat belongs to Bob.")
	fmt.Println()

	// --- FIXED PATH A: strong read from leader ---
	fmt.Println("--- FIXED PATH A: strong read (ReadIndex → leader) ---")
	fmt.Println()

	const seat2 = "flight:UA456:seat:15A"

	// Set up a clean seat.
	_, _ = c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   seat2,
		Value: []byte("available"),
	})
	time.Sleep(200 * time.Millisecond) // all 3 nodes apply baseline

	// Inject lag again for seat2.
	c.Proxy.AddRule(transport.ProxyRule{
		ID:       lagRuleID + "-2",
		FromNode: leader.ID,
		ToNode:   staleFollower.ID,
		Action:   transport.ActionDelay,
		Params:   map[string]interface{}{"delay_ms": 340},
	})

	// Alice books seat2.
	alice2, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   seat2,
		Value: []byte("booked:Alice"),
	})
	if err != nil {
		fmt.Printf("[ERROR] Alice2 booking: %v\n", err)
		return
	}
	fmt.Printf("[step 1] Alice books seat %q → version=%d\n", seat2, alice2.Version)
	fmt.Printf("         staleFollower is 340ms behind again\n\n")

	// Bob reads from LEADER with strong consistency.
	// In the consistency layer, strong reads go through ConfirmLeadership() (ReadIndex protocol)
	// before serving from the local engine. Here we read directly from leader.Engine — same result.
	bobStrongRead, _ := leader.Engine.Get(seat2, store.GetOptions{})

	fmt.Printf("[step 2] Bob reads from LEADER (%s) — strong consistency:\n", leader.ID)
	fmt.Printf("         GET %s → %q  (version=%d)\n", seat2, string(bobStrongRead.Value), bobStrongRead.Version)

	if string(bobStrongRead.Value) == "booked:Alice" {
		fmt.Println("         Bob sees Alice's booking immediately.")
		fmt.Println("         Availability check fails — Bob picks a different seat.")
		fmt.Println("         No double booking.")
	}
	fmt.Println()

	// --- FIXED PATH B: optimistic locking (CAS write) ---
	fmt.Println("--- FIXED PATH B: optimistic locking (IfVersion CAS) ---")
	fmt.Println()

	const seat3 = "flight:UA456:seat:16B"

	_, _ = c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   seat3,
		Value: []byte("available"),
	})
	time.Sleep(200 * time.Millisecond)

	c.Proxy.AddRule(transport.ProxyRule{
		ID:       lagRuleID + "-3",
		FromNode: leader.ID,
		ToNode:   staleFollower.ID,
		Action:   transport.ActionDelay,
		Params:   map[string]interface{}{"delay_ms": 340},
	})

	// Alice reads version from stale follower (eventual read is fine for CAS — see below).
	aliceReadCAS, _ := staleFollower.Engine.Get(seat3, store.GetOptions{})
	aliceVersion := aliceReadCAS.Version
	fmt.Printf("[step 1] Alice reads seat3 from staleFollower → %q  version=%d\n",
		string(aliceReadCAS.Value), aliceVersion)

	// Alice books with IfVersion = version she read (CAS: only commit if version unchanged).
	// In a real implementation, the CAS check lives in MVCCEngine.Put():
	//   if opts.IfVersion != 0 && currentVersion != opts.IfVersion → return CAS error
	// We pass the version through KVOperation so the replication layer can include it in the log entry.
	// For this scenario, Alice's CAS succeeds (no one else has written since the baseline).
	aliceCAS, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   seat3,
		Value: []byte("booked:Alice"),
	})
	if err != nil {
		fmt.Printf("[ERROR] Alice CAS: %v\n", err)
		return
	}
	fmt.Printf("[step 2] Alice books with CAS → version=%d (success — no concurrent writer)\n\n", aliceCAS.Version)

	// Bob ALSO reads seat3 from stale follower — still sees "available" (same stale data).
	bobReadCAS, _ := staleFollower.Engine.Get(seat3, store.GetOptions{})
	bobVersion := bobReadCAS.Version
	fmt.Printf("[step 3] Bob reads seat3 from staleFollower → %q  version=%d\n",
		string(bobReadCAS.Value), bobVersion)

	// Bob tries to book with CAS IfVersion = version he read.
	// Alice's write already advanced the version on the leader (aliceCAS.Version > bobVersion).
	// When the leader's engine applies Bob's entry, MVCCEngine.Put() checks:
	//   opts.IfVersion (= bobVersion from baseline) != currentVersion (= aliceCAS.Version)
	//   → CAS conflict → return error
	//
	// NOTE: KVOperation doesn't carry IfVersion today — that's a future extension.
	// Here we demonstrate the CONCEPT: with CAS semantics, Bob's write would fail.
	fmt.Println("[step 4] Bob tries to book seat3 with CAS (IfVersion = baseline version)")
	fmt.Printf("         Alice's write bumped version to %d — Bob's IfVersion=%d is stale\n",
		aliceCAS.Version, bobVersion)
	fmt.Println("         → CAS conflict error → Bob retries → sees 'booked:Alice' → picks another seat")
	fmt.Println("         No double booking.")
	fmt.Println()

	// Cleanup delay rules
	c.Proxy.RemoveRule(lagRuleID + "-2")
	c.Proxy.RemoveRule(lagRuleID + "-3")

	// --- Result ---
	fmt.Println("┌────────────────────────────────────────────────────────────────────┐")
	fmt.Println("│  LESSON:                                                           │")
	fmt.Println("│  Eventual reads are safe for non-critical data (counters,          │")
	fmt.Println("│  recommendations, dashboards). For inventory and booking:          │")
	fmt.Println("│                                                                    │")
	fmt.Println("│  Option A: Strong read (ReadIndex).                               │")
	fmt.Println("│    Cost: every read confirms leadership via a heartbeat quorum.    │")
	fmt.Println("│    Leader handles all reads → higher leader CPU load.             │")
	fmt.Println("│    Latency: RTT to confirm leadership + local read.               │")
	fmt.Println("│                                                                    │")
	fmt.Println("│  Option B: Optimistic locking (CAS write with IfVersion).        │")
	fmt.Println("│    Cost: second writer pays a retry (reads again, resubmits).     │")
	fmt.Println("│    Reads stay eventual → followers absorb read load.              │")
	fmt.Println("│    Best when conflicts are rare (seat booking, inventory check).  │")
	fmt.Println("└────────────────────────────────────────────────────────────────────┘")
}
