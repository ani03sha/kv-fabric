package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// TestSnapshotEngineRoundTrip is the correctness foundation for all snapshotting.
// It verifies that engine.Snapshot() + engine.ApplySnapshot() is a lossless
// round-trip: every live key with its value and version survives.
//
// This test uses no disk and no cluster — it exercises the engine interface
// in isolation. If this fails, disk persistence and cluster recovery will also
// fail, so it is the right place to start diagnosing.
func TestSnapshotEngineRoundTrip(t *testing.T) {
	t.Parallel()

	src := store.NewMVCCEngine("src", time.Minute)
	defer src.Close()

	// Write a mix of keys with explicit log indices (simulating the Raft path).
	writes := []struct {
		key    string
		value  string
		logIdx uint64
	}{
		{"user:alice", "alice-data", 1},
		{"user:bob", "bob-data", 2},
		{"config:timeout", "30s", 3},
	}
	for _, w := range writes {
		if _, err := src.Put(w.key, []byte(w.value), store.PutOptions{LogIndex: w.logIdx}); err != nil {
			t.Fatalf("Put %q: %v", w.key, err)
		}
	}

	// Also write and then delete a key. The deleted key must NOT appear in the
	// restored engine — snapshot captures live state only.
	if _, err := src.Put("deleted:key", []byte("gone"), store.PutOptions{LogIndex: 4}); err != nil {
		t.Fatalf("Put deleted:key: %v", err)
	}
	if err := src.Delete("deleted:key", store.DeleteOptions{}); err != nil {
		t.Fatalf("Delete deleted:key: %v", err)
	}

	snap, err := src.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if snap.Version() == 0 {
		t.Fatal("snapshot version must be non-zero after writes with LogIndex")
	}

	// Apply the snapshot to a brand-new engine.
	dst := store.NewMVCCEngine("dst", time.Minute)
	defer dst.Close()

	if err := dst.ApplySnapshot(snap); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	// All live keys must be present with correct values and versions.
	for _, w := range writes {
		r, err := dst.Get(w.key, store.GetOptions{})
		if err != nil {
			t.Errorf("Get %q: %v", w.key, err)
			continue
		}
		if r.Value == nil {
			t.Errorf("Get %q: key missing after snapshot restore", w.key)
			continue
		}
		if string(r.Value) != w.value {
			t.Errorf("Get %q: got value %q, want %q", w.key, r.Value, w.value)
		}
		if r.Version != w.logIdx {
			t.Errorf("Get %q: got version %d, want %d", w.key, r.Version, w.logIdx)
		}
	}

	// Deleted key must NOT be visible.
	r, err := dst.Get("deleted:key", store.GetOptions{})
	if err != nil {
		t.Fatalf("Get deleted:key: %v", err)
	}
	if r.Value != nil {
		t.Errorf("deleted:key still visible after restore: got %q", r.Value)
	}

	t.Logf("snapshot version=%d, entries=%d", snap.Version(), dst.Stats().TotalKeys)
}

// TestSnapshotManagerPersistsToDisk verifies the crash-recovery path end-to-end:
//
//  1. A node writes data and its SnapshotManager saves a snapshot to disk.
//  2. The process "crashes" (engine is discarded in-memory).
//  3. A new engine + new SnapshotManager load the snapshot from the same directory.
//  4. The new engine has all the original data.
//
// This is the guarantee that makes kv-fabric safe to restart: a node that crashes
// after a snapshot can rejoin the cluster without replaying its entire Raft log
// from index 0.
func TestSnapshotManagerPersistsToDisk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const nodeID = "test-node"

	// --- Phase 1: write data and save snapshot ---

	engine1 := store.NewMVCCEngine(nodeID, time.Minute)
	defer engine1.Close()

	kvs := map[string]string{
		"accounts:1001": "balance=500",
		"accounts:1002": "balance=750",
		"sessions:abc":  "user=alice",
	}
	var maxIdx uint64
	idx := uint64(0)
	for key, val := range kvs {
		idx++
		if _, err := engine1.Put(key, []byte(val), store.PutOptions{LogIndex: idx}); err != nil {
			t.Fatalf("Put %q: %v", key, err)
		}
		if idx > maxIdx {
			maxIdx = idx
		}
	}

	// SnapshotManager with a very high threshold and interval so it never fires
	// automatically — we call TakeSnapshot() manually to test that code path directly.
	sm1 := replication.NewSnapshotManager(
		nodeID, engine1, dir,
		1_000_000,    // threshold: won't auto-fire
		24*time.Hour, // interval: won't auto-fire
		zap.NewNop(),
	)

	if err := sm1.TakeSnapshot(); err != nil {
		t.Fatalf("TakeSnapshot: %v", err)
	}

	savedVer := sm1.LastSnapshotVersion()
	if savedVer == 0 {
		t.Fatal("LastSnapshotVersion must be non-zero after TakeSnapshot with LogIndex writes")
	}

	// Verify the snapshot files are actually on disk.
	for _, fname := range []string{"snapshot.bin", "snapshot.meta.json"} {
		if _, err := os.Stat(filepath.Join(dir, fname)); os.IsNotExist(err) {
			t.Errorf("expected snapshot file %q not found on disk", fname)
		}
	}

	// --- Phase 2: "crash" — discard the in-memory engine ---
	// engine1 goes out of scope after this block. The only surviving record
	// of what was written is the snapshot on disk.

	// --- Phase 3: recover from disk ---

	engine2 := store.NewMVCCEngine(nodeID, time.Minute)
	defer engine2.Close()

	sm2 := replication.NewSnapshotManager(
		nodeID, engine2, dir,
		1_000_000,
		24*time.Hour,
		zap.NewNop(),
	)

	loadedVer, err := sm2.LoadLatestSnapshot()
	if err != nil {
		t.Fatalf("LoadLatestSnapshot: %v", err)
	}
	if loadedVer != savedVer {
		t.Errorf("loaded version %d, want %d", loadedVer, savedVer)
	}

	// All keys must be present with the correct values in the recovered engine.
	for key, wantVal := range kvs {
		r, err := engine2.Get(key, store.GetOptions{})
		if err != nil {
			t.Errorf("Get %q after recovery: %v", key, err)
			continue
		}
		if r.Value == nil {
			t.Errorf("Get %q after recovery: key missing", key)
			continue
		}
		if string(r.Value) != wantVal {
			t.Errorf("Get %q after recovery: got %q, want %q", key, r.Value, wantVal)
		}
	}

	t.Logf("snapshot version=%d recovered successfully", loadedVer)
}

// TestSnapshotThresholdTriggers verifies that MaybeTakeSnapshot fires exactly
// when the applied index crosses the threshold boundary — not before, not once
// per entry after.
//
// The threshold is the primary knob that controls snapshot frequency. Too low
// and you snapshot constantly (write amplification). Too high and a restarting
// node replays a long log tail. This test verifies the boundary behaviour.
func TestSnapshotThresholdTriggers(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	const threshold = uint64(5)

	engine := store.NewMVCCEngine("thresh-node", time.Minute)
	defer engine.Close()

	sm := replication.NewSnapshotManager(
		"thresh-node", engine, dir,
		threshold,
		24*time.Hour, // wall-clock interval never fires in this test
		zap.NewNop(),
	)
	// No sm.Start() — we drive MaybeTakeSnapshot manually to test the threshold logic
	// without goroutine scheduling non-determinism.

	// Write and apply entries 1..threshold-1 — must NOT trigger a snapshot.
	for i := uint64(1); i < threshold; i++ {
		_, err := engine.Put(fmt.Sprintf("key:%d", i), []byte("v"), store.PutOptions{LogIndex: i})
		if err != nil {
			t.Fatalf("Put %d: %v", i, err)
		}
		sm.MaybeTakeSnapshot(i)
	}

	if v := sm.LastSnapshotVersion(); v != 0 {
		t.Errorf("snapshot triggered too early: LastSnapshotVersion=%d after %d entries (threshold=%d)",
			v, threshold-1, threshold)
	}

	// Apply entry at exactly threshold — must trigger.
	_, err := engine.Put(fmt.Sprintf("key:%d", threshold), []byte("v"), store.PutOptions{LogIndex: threshold})
	if err != nil {
		t.Fatalf("Put threshold entry: %v", err)
	}
	sm.MaybeTakeSnapshot(threshold)

	if v := sm.LastSnapshotVersion(); v == 0 {
		t.Errorf("snapshot did not trigger at threshold=%d: LastSnapshotVersion still 0", threshold)
	}

	// Apply one more entry — snapshot should NOT retrigger until another full
	// threshold's worth of entries have accumulated.
	prevVer := sm.LastSnapshotVersion()
	_, err = engine.Put(fmt.Sprintf("key:%d", threshold+1), []byte("v"), store.PutOptions{LogIndex: threshold + 1})
	if err != nil {
		t.Fatalf("Put post-threshold entry: %v", err)
	}
	sm.MaybeTakeSnapshot(threshold + 1)

	if v := sm.LastSnapshotVersion(); v != prevVer {
		t.Errorf("snapshot retriggered one entry after threshold: got version %d, want %d",
			v, prevVer)
	}

	t.Logf("threshold=%d: first snapshot at version=%d", threshold, sm.LastSnapshotVersion())
}
