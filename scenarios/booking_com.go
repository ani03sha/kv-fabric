package scenarios

import (
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// This demonstrates how eventual consistency with follower lag enables double bookings.
//
// Reference: Booking.com reported a MySQL replication lag of ~340ms during a traffic spike. During that window,
// a follower served stale availability data. Two users simultaneously saw the same seat as "available" and both booked it:
// neither write was invalid, the cluster accepted both, and one booking silently overwrote the other.
//
// This scenario shows:
//   - The failure path: stale read from lagging follower -> double booking
//   - The fix: strong read from leader, OR optimistic locking (CAS write)
func RunBookingCom(logger *zap.Logger) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║          BOOKING.COM STALE READ SCENARIO                  ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	// --- Setup ---
	leader := store.NewMVCCEngine("leader", 30*time.Second)
	defer leader.Close()

	// The follower is an independent engine representing a replica node that lags behind.
	// In production, each node has its own MVCC engine driven by its own apply loop.
	// Here we drive both engines manually to simulate what each node's apply loop would have applied at any given moment.
	follower := store.NewMVCCEngine("follower-1", 30*time.Second)
	defer follower.Close()

	tracker := replication.NewReplicationTracker()
	tracker.Track("follower-1")

	const seat = "flight:UA456:seat:14C"

	// Initial state: seat is available. Both leader and follower have applied this write
	// (log index 100). They are fully synchronized.
	_, _ = leader.Put(seat, []byte("available"), store.PutOptions{LogIndex: 100})
	_, _ = follower.Put(seat, []byte("available"), store.PutOptions{LogIndex: 100})
	tracker.UpdateLeaderCommitIndex(100)
	tracker.UpdateFollower("follower-1", 100)

	fmt.Printf("[setup]  seat %s = \"available\" on both leader and follower (log index 100)\n\n", seat)

	// --- Failure path ---
	fmt.Println("--- FAILURE PATH: eventual reads from lagging follower ---")
	fmt.Println()

	// Simulate 340 entries of lag at the default 1ms per entry = 340ms behind.
	// In the Booking.com incident: a traffic spike caused the follower to fall 340ms behind the leader.
	// All reads that round-robin'd to that follower saw stale data for the entire 340ms window.
	//
	// We advance the leader's commit index to 440 WITHOUT advancing the follower. This creates a gap of 340 entries
	// (100 -> 440).
	tracker.UpdateLeaderCommitIndex(440)
	// follower-1 stays at MatchIndex=100

	lagEntries, lagMs := tracker.GetLag("follower-1")
	fmt.Printf("[lag]    leader at 440, follower at 100 -> %d entries behind, ~%dms lag\n\n", lagEntries, lagMs)

	// Alice books the seat. Write goes to leader (version 441).
	// In production: PUT /v1/keys/seat with strong consistency -> Raft -> leader engine.
	aliceWrite, _ := leader.Put(seat, []byte("booked:Alice"), store.PutOptions{LogIndex: 441})
	tracker.UpdateLeaderCommitIndex(441)
	fmt.Printf("[step 1] Alice books seat -> leader version=%d value=%q\n", aliceWrite.Version, "booked:Alice")
	fmt.Println("         follower has NOT applied this — still shows \"available\"")
	fmt.Println()

	// Bob's client round-robins to the FOLLOWER for its availability read.
	// Eventual consistency means: read from the nearest healthy replica.
	// The follower is healthy - it's just lagging.
	bobRead, _ := follower.Get(seat, store.GetOptions{})
	fmt.Printf("[step 2] Bob reads from follower (eventual consistency):\n")
	fmt.Printf("         GET %s -> value=%q  version=%d\n", seat, string(bobRead.Value), bobRead.Version)
	fmt.Println("         !! STALE DATA — follower hasn't applied Alice's booking")
	fmt.Println("         !! Bob's availability check passes: \"available\" -> proceed to book")
	fmt.Println()

	// Bob's booking is accepted by the leader (version 442).
	// The leader has no way to know Bob is acting on stale data and his PUT is valid.
	bobWrite, _ := leader.Put(seat, []byte("booked:Bob"), store.PutOptions{LogIndex: 442})
	tracker.UpdateLeaderCommitIndex(442)
	fmt.Printf("[step 3] Bob books the SAME seat → leader version=%d value=%q\n",
		bobWrite.Version, "booked:Bob")
	fmt.Println("         DOUBLE BOOKING: both Alice and Bob have confirmation emails for seat 14C")
	fmt.Println()

	// The follower eventually catches up, both writes replicate in order.
	// Last write wins in MVCC: Bob's booking (version 442) is now the current value.
	_, _ = follower.Put(seat, []byte("booked:Alice"), store.PutOptions{LogIndex: 441})
	_, _ = follower.Put(seat, []byte("booked:Bob"), store.PutOptions{LogIndex: 442})
	tracker.UpdateFollower("follower-1", 442)

	leaderFinal, _ := leader.Get(seat, store.GetOptions{})
	followerFinal, _ := follower.Get(seat, store.GetOptions{})
	fmt.Println("[result] after follower catches up:")
	fmt.Printf("         leader:   %s = %q (v%d)\n", seat, string(leaderFinal.Value), leaderFinal.Version)
	fmt.Printf("         follower: %s = %q (v%d)\n", seat, string(followerFinal.Value), followerFinal.Version)
	fmt.Println("         Alice's booking was silently overwritten. She finds out at the gate.")
	fmt.Println()

	// --- Fixed path ---
	fmt.Println("── FIXED PATH: strong read from leader ────────────────────────")
	fmt.Println()

	// Different seat so we have a clean initial state.
	const seat2 = "flight:UA456:seat:15A"
	_, _ = leader.Put(seat2, []byte("available"), store.PutOptions{LogIndex: 500})
	_, _ = follower.Put(seat2, []byte("available"), store.PutOptions{LogIndex: 500})
	tracker.UpdateLeaderCommitIndex(500)
	tracker.UpdateFollower("follower-1", 500)

	// Alice books seat2.
	alice2Write, _ := leader.Put(seat2, []byte("booked:Alice"), store.PutOptions{LogIndex: 501})
	tracker.UpdateLeaderCommitIndex(501)
	fmt.Printf("[step 1] Alice books seat %s -> leader version=%d\n", seat2, alice2Write.Version)
	fmt.Println("         follower is lagging again — still shows \"available\"")
	fmt.Println()

	// Bob reads with STRONG consistency. In our architecture, strong reads go to the
	// LEADER (via the ReadIndex protocol). The leader has the current committed state.
	// There is no way for a strong read to return stale data.
	bobStrongRead, _ := leader.Get(seat2, store.GetOptions{})
	fmt.Printf("[step 2] Bob reads from LEADER (strong consistency):\n")
	fmt.Printf("         GET %s -> value=%q  version=%d\n", seat2, string(bobStrongRead.Value), bobStrongRead.Version)
	fmt.Println("         Bob sees Alice's booking immediately.")
	fmt.Println("         His availability check fails — he picks a different seat.")
	fmt.Println("         No double booking.")
	fmt.Println()

	fmt.Println("┌──────────────────────────────────────────────────────────────┐")
	fmt.Println("│  LESSON:                                                     │")
	fmt.Println("│  Eventual reads are safe for non-critical data (counters,    │")
	fmt.Println("│  recommendations, dashboards). For inventory and booking:    │")
	fmt.Println("│                                                              │")
	fmt.Println("│  Option A: Read from leader (strong consistency).           │")
	fmt.Println("│            Cost: higher leader load, slightly higher latency │")
	fmt.Println("│                                                              │")
	fmt.Println("│  Option B: Optimistic locking. Eventual read is OK, but    │")
	fmt.Println("│            the write uses IfVersion (CAS). If two writers   │")
	fmt.Println("│            race, the second write fails with a CAS error.   │")
	fmt.Println("│            Client retries and sees the current state.       │")
	fmt.Println("└──────────────────────────────────────────────────────────────┘")
}
