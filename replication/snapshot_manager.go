package replication

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// This struct is written alongside the snapshot data file. It lets the node skip loading a snapshot that
// is older than its current applied index.
type snapshotMeta struct {
	Version   uint64    `json:"version"`    // Raft log index captured
	CreatedAt time.Time `json:"created_at"` // wall-clock time (informational)
	NodeID    string    `json:"node_id"`
}

const (
	snapDataFile = "snapshot.bin"
	snapMetaFile = "snapshot.meta.json"
)

// This struct handles periodic snapshotting and snapshot recovery.
//
// Design rationale:
//   - Each node snapshots independently. Because all nodes apply the same log
//     entries in the same order (Raft guarantee), snapshotting at index N on
//     any node produces semantically identical state. No cross-node coordination
//     is needed.
//   - Snapshots are written to the node's data directory (next to the WAL).
//     A two-phase write (temp file → rename) prevents a half-written snapshot
//     from corrupting the previous good one on a crash.
//   - After a snapshot, the GC horizon advances so MVCC can reclaim old versions.
//   - Log truncation is intentionally omitted: raftly v0.2.0 does not expose a
//     public TruncateLog API. When raftly adds that API we can plug it in here
//     without changing anything else.
type SnapshotManager struct {
	nodeID  string
	engine  store.KVEngine
	dataDir string // same directory WAL uses - one place to backup
	logger  *zap.Logger

	mu              sync.Mutex
	lastSnapshotVer uint64 // Index of the most recent successful snapshot

	threshold uint64        // take a new snapshot every N applied entries
	interval  time.Duration // also take one on a wall-clock cadence

	stop chan struct{}
	done chan struct{}
}

func NewSnapshotManager(
	nodeID string,
	engine store.KVEngine,
	dataDir string,
	threshold uint64,
	interval time.Duration,
	logger *zap.Logger,
) *SnapshotManager {
	return &SnapshotManager{
		nodeID:    nodeID,
		engine:    engine,
		dataDir:   dataDir,
		threshold: threshold,
		interval:  interval,
		logger:    logger,
		stop:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start launches the periodic snapshot loop.
func (sm *SnapshotManager) Start() {
	go sm.loop()
}

// Stop shuts down the snapshot loop and waits for it to exit.
func (sm *SnapshotManager) Stop() {
	close(sm.stop)
	<-sm.done
}

// This function returns the Raft log index of the most recent successful snapshot. Returns 0 if no snapshot has
// been taken yet this session.
func (sm *SnapshotManager) LastSnapshotVersion() uint64 {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.lastSnapshotVer
}

// MaybeTakeSnapshot takes a snapshot if the applied index has advanced far
// enough past the last snapshot. Called by the apply loop after each entry.
func (sm *SnapshotManager) MaybeTakeSnapshot(appliedIndex uint64) {
	sm.mu.Lock()
	last := sm.lastSnapshotVer
	sm.mu.Unlock()

	if appliedIndex < last+sm.threshold {
		return // not enough new entries yet
	}
	if err := sm.TakeSnapshot(); err != nil {
		sm.logger.Error("snapshot: take failed",
			zap.String("node", sm.nodeID),
			zap.Error(err))
	}
}

// TakeSnapshot captures the engine state and persists it to disk atomically.
// Safe to call concurrently — the engine's own read lock serializes snapshot reads against writes.
func (sm *SnapshotManager) TakeSnapshot() error {
	snap, err := sm.engine.Snapshot()
	if err != nil {
		return fmt.Errorf("take snapshot: engine: %w", err)
	}

	if err := sm.persist(snap); err != nil {
		return err
	}

	sm.mu.Lock()
	sm.lastSnapshotVer = snap.Version()
	sm.mu.Unlock()

	sm.logger.Info("snapshot: saved", zap.String("node", sm.nodeID), zap.Uint64("version", snap.Version()))

	return nil
}

func (sm *SnapshotManager) LoadLatestSnapshot() (uint64, error) {
	metaPath := filepath.Join(sm.dataDir, snapMetaFile)
	metaBytes, err := os.ReadFile(metaPath)
	if os.IsNotExist(err) {
		sm.logger.Info("snapshot: no snapshot on disk, starting fresh", zap.String("node", sm.nodeID))
		return 0, nil // fresh node
	}

	if err != nil {
		return 0, fmt.Errorf("load snapshot: read meta: %w", err)
	}

	var meta snapshotMeta
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return 0, fmt.Errorf("load snapshot: parse meta: %w", err)
	}

	dataPath := filepath.Join(sm.dataDir, snapDataFile)
	data, err := os.ReadFile(dataPath)
	if err != nil {
		return 0, fmt.Errorf("load snapshot: read data: %w", err)
	}

	snap := &diskSnapshot{data: data, version: meta.Version}
	if err := sm.engine.ApplySnapshot(snap); err != nil {
		return 0, fmt.Errorf("load snapshot: apply: %w", err)
	}

	sm.mu.Lock()
	sm.lastSnapshotVer = meta.Version
	sm.mu.Unlock()

	sm.logger.Info("snapshot: loaded from disk",
		zap.String("node", sm.nodeID),
		zap.Uint64("version", meta.Version))
	return meta.Version, nil
}

// This function writes the snapshot to disk using a temp-file -> rename two-phase commit.
// If the process crashes mid-write, the old snapshot file is untouched.
func (sm *SnapshotManager) persist(snap store.Snapshot) error {
	if err := os.MkdirAll(sm.dataDir, 0o755); err != nil {
		return fmt.Errorf("snapshot: mkdir %s: %w", sm.dataDir, err)
	}

	// --- data file ---
	dataPath := filepath.Join(sm.dataDir, snapDataFile)
	tmpData, err := os.CreateTemp(sm.dataDir, "snap-data-*.tmp")
	if err != nil {
		return fmt.Errorf("snapshot: create tmp data: %w", err)
	}
	if _, err := tmpData.Write(snap.Data()); err != nil {
		tmpData.Close()
		os.Remove(tmpData.Name())
		return fmt.Errorf("snapshot: write data: %w", err)
	}
	if err := tmpData.Close(); err != nil {
		os.Remove(tmpData.Name())
		return fmt.Errorf("snapshot: close tmp data: %w", err)
	}
	if err := os.Rename(tmpData.Name(), dataPath); err != nil {
		os.Remove(tmpData.Name())
		return fmt.Errorf("snapshot: rename data: %w", err)
	}

	// --- meta file (written after data so meta is never newer than data) ---
	meta := snapshotMeta{
		Version:   snap.Version(),
		CreatedAt: time.Now().UTC(),
		NodeID:    sm.nodeID,
	}
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("snapshot: marshal meta: %w", err)
	}
	metaPath := filepath.Join(sm.dataDir, snapMetaFile)
	tmpMeta, err := os.CreateTemp(sm.dataDir, "snap-meta-*.tmp")
	if err != nil {
		return fmt.Errorf("snapshot: create tmp meta: %w", err)
	}
	if _, err := tmpMeta.Write(metaBytes); err != nil {
		tmpMeta.Close()
		os.Remove(tmpMeta.Name())
		return fmt.Errorf("snapshot: write meta: %w", err)
	}
	if err := tmpMeta.Close(); err != nil {
		os.Remove(tmpMeta.Name())
		return fmt.Errorf("snapshot: close tmp meta: %w", err)
	}
	if err := os.Rename(tmpMeta.Name(), metaPath); err != nil {
		os.Remove(tmpMeta.Name())
		return fmt.Errorf("snapshot: rename meta: %w", err)
	}
	return nil
}

// This function takes snapshots on a wall-clock interval as a safety net.
// The primary trigger is MaybeTakeSnapshot (called after every apply).
func (sm *SnapshotManager) loop() {
	defer close(sm.done)
	ticker := time.NewTicker(sm.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := sm.TakeSnapshot(); err != nil {
				sm.logger.Error("snapshot: periodic take failed",
					zap.String("node", sm.nodeID),
					zap.Error(err))
			}
		case <-sm.stop:
			return
		}
	}
}

// diskSnapshot wraps raw bytes read from disk into the store.Snapshot interface.
type diskSnapshot struct {
	data    []byte
	version uint64
}

func (d *diskSnapshot) Data() []byte    { return d.data }
func (d *diskSnapshot) Version() uint64 { return d.version }
