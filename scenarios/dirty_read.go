package scenarios

import (
	"fmt"
	"time"

	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// This scenario demonstrates that MVCC prevents dirty reads by providing snapshot isolation.
//
// A dirty read is reading data written by a concurrent operation that has not yet completed.
// In a traditional lock-based database: a write operation acquires a row lock, updates the
// row in place, and holds the lock until commit. A dirty read happens when another transaction
// reads the modified row before the writer commits — it sees uncommitted, potentially inconsistent intermediate state.
//
// Example: transferring $200 from Alice to Bob.
//
//	Without MVCC:  writer debits Alice (alice=800). A concurrent reader reads alice=800, bob=500.
//	               $200 has "disappeared" — inconsistent view of the world.
//	With MVCC:     writer creates alice@v12=800. The old alice@v10=1000 still exists.
//	               A reader pinned at v10 reads alice@v10=1000, bob@v11=500. Consistent.
//	               The dirty state (alice=800 without bob updated) is never visible to snapshots.
//
// This scenario shows both paths back to back.
func RunDirtyRead(logger *zap.Logger) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║           DIRTY READ / SNAPSHOT ISOLATION SCENARIO        ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	engine := store.NewMVCCEngine("node-1", 30*time.Second)
	defer engine.Close()

	// --- Initial state ---
	// Alice has $1000 (version 10). Bob has $500 (version 11).
	// These are separate log entries — each key's write is its own Raft entry.
	_, _ = engine.Put("account:alice", []byte("$1000"), store.PutOptions{LogIndex: 10})
	_, _ = engine.Put("account:bob", []byte("$500"), store.PutOptions{LogIndex: 11})

	fmt.Println("[initial state]")
	alice0, _ := engine.Get("account:alice", store.GetOptions{})
	bob0, _ := engine.Get("account:bob", store.GetOptions{})
	fmt.Printf("  account:alice = %s  (version=%d)\n", string(alice0.Value), alice0.Version)
	fmt.Printf("  account:bob   = %s  (version=%d)\n", string(bob0.Value), bob0.Version)
	fmt.Println()

	// --- Path A: reading without a snapshot (dirty read exposure) ---
	//
	// A transfer of $200 from Alice to Bob is two separate writes.
	// Between write 1 (debit Alice) and write 2 (credit Bob), the world is inconsistent:
	// $200 exists nowhere — it left Alice but hasn't reached Bob yet.
	//
	// Without a snapshot: a reader that runs between these two writes sees that gap.
	fmt.Println("----- PATH A: reading without snapshot isolation -----")
	fmt.Println()
	fmt.Println("[transfer $200 from Alice to Bob — two separate writes]")
	fmt.Println()

	// Write 1: debit Alice. alice goes from $1000 (v10) to $800 (v12).
	_, _ = engine.Put("account:alice", []byte("$800"), store.PutOptions{LogIndex: 12})
	fmt.Println("[write 1 complete] alice debited: alice@v12 = $800")

	// A reader runs HERE — before write 2 credits Bob. It reads the LATEST version of each key. alice=v12=$800, bob=v11=$500.
	// Total: $800 + $500 = $1300. But total should be $1500. $200 is missing. This is the dirty read — the reader saw intermediate state.
	midAlice, _ := engine.Get("account:alice", store.GetOptions{})
	midBob, _ := engine.Get("account:bob", store.GetOptions{})
	fmt.Println()
	fmt.Println("[reader runs between write 1 and write 2 — no snapshot]")
	fmt.Printf("  account:alice = %s  (version=%d)\n", string(midAlice.Value), midAlice.Version)
	fmt.Printf("  account:bob   = %s  (version=%d)\n", string(midBob.Value), midBob.Version)
	fmt.Println("  !! $200 has disappeared from the system")
	fmt.Println("  !! alice+bob = $1300, should be $1500")
	fmt.Println("  !! This reader observed a dirty (inconsistent) intermediate state")
	fmt.Println()

	// Write 2: credit Bob. bob goes from $500 (v11) to $700 (v13).
	_, _ = engine.Put("account:bob", []byte("$700"), store.PutOptions{LogIndex: 13})
	fmt.Println("[write 2 complete] bob credited: bob@v13 = $700")
	fmt.Println("                   transfer complete on leader: alice=$800, bob=$700, total=$1500")
	fmt.Println()

	// --- Path B: reading with a snapshot (MVCC isolation) ---
	//
	// Reset: a new transfer starts. This time the reader pins a snapshot version BEFORE the transfer begins.
	// It uses GetAtVersion to read at that snapshot, never seeing the intermediate state.
	fmt.Println("---- PATH B: reading with snapshot isolation (MVCC) ----")
	fmt.Println()

	// New transfer: $100 from Alice to Bob.
	// Alice is currently at v12=$800. After this transfer she'll be at v14=$700.
	// Bob is currently at v13=$700. After transfer he'll be at v15=$800.
	//
	// The reader pins the PRE-TRANSFER snapshot: alice@v12, bob@v13.
	// It will read those exact versions, even while the transfer is in progress.
	engine.PinVersion("account:alice", 12)
	engine.PinVersion("account:bob", 13)
	fmt.Println("[snapshot] reader pins alice@v12 and bob@v13 before transfer begins")
	fmt.Printf("           alice@v12 = $800, bob@v13 = $700  (total = $1500)\n")
	fmt.Println()

	// Write 1: debit Alice. alice@v14=$700. (alice@v12=$800 still exists — MVCC never overwrites)
	_, _ = engine.Put("account:alice", []byte("$700"), store.PutOptions{LogIndex: 14})
	fmt.Println("[write 1 complete] alice debited: alice@v14 = $700  (alice@v12 still exists)")

	// Reader reads its pinned snapshot — sees alice@v12=$800, not the dirty alice@v14=$700.
	snapAlice, _ := engine.GetAtVersion("account:alice", 12)
	snapBob, _ := engine.GetAtVersion("account:bob", 13)
	fmt.Println()
	fmt.Println("[reader reads its pinned snapshot — between write 1 and write 2]")
	fmt.Printf("  GetAtVersion(account:alice, 12) → %s  ← pre-transfer value, NOT the dirty write\n", string(snapAlice.Value))
	fmt.Printf("  GetAtVersion(account:bob,   13) → %s  ← pre-transfer value\n", string(snapBob.Value))
	fmt.Println(" alice + bob = $1500 — CONSISTENT snapshot, no dirty read")
	fmt.Println()

	// Write 2: credit Bob. bob@v15=$800.
	_, _ = engine.Put("account:bob", []byte("$800"), store.PutOptions{LogIndex: 15})
	fmt.Println("[write 2 complete] bob credited: bob@v15 = $800")
	fmt.Println("                   transfer complete: alice=$700, bob=$800, total=$1500")
	fmt.Println()

	// Release the pins. GC is now free to collect versions 12 and 13.
	engine.UnpinVersion("account:alice", 12)
	engine.UnpinVersion("account:bob", 13)
	fmt.Println("[snapshot released] alice@v12 and bob@v13 unpinned — GC can now collect them")
	fmt.Println()

	// Final state
	finalAlice, _ := engine.Get("account:alice", store.GetOptions{})
	finalBob, _ := engine.Get("account:bob", store.GetOptions{})
	fmt.Println("[final state]")
	fmt.Printf("  account:alice = %s  (version=%d)\n", string(finalAlice.Value), finalAlice.Version)
	fmt.Printf("  account:bob   = %s  (version=%d)\n", string(finalBob.Value), finalBob.Version)
	fmt.Println()

	fmt.Println("┌──────────────────────────────────────────────────────────────┐")
	fmt.Println("│  LESSON:                                                     │")
	fmt.Println("│  MVCC prevents dirty reads by never overwriting old versions │")
	fmt.Println("│  A reader that pins a snapshot always sees a consistent      │")
	fmt.Println("│  view of the world as of that snapshot point — regardless    │")
	fmt.Println("│  of how many writes are in-flight concurrently.              │")
	fmt.Println("│                                                              │")
	fmt.Println("│  The cost: old versions accumulate until GC can reclaim     │")
	fmt.Println("│  them (after all readers unpin). This is the MVCC bloat    │")
	fmt.Println("│  tradeoff: isolation guarantees vs. memory overhead.        │")
	fmt.Println("└──────────────────────────────────────────────────────────────┘")
}
