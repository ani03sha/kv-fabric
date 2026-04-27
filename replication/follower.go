package replication

import (
	"encoding/json"
	"fmt"
	"sync/atomic"

	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// This runs on follower nodes and applies the same committed entries as the leader, in the same order,
// to keep the local engine in sync with the cluster.
//
// Followers do NOT propose writes. Clients that hit a follower are redirected to the leader (or served stale
// reads, depending on consistency mode).
//
// The structural similarity to LeaderReplicator's applyLoop is intentional: both are state machine appliers.
// The difference is:
//   - No pending ops  (followers don't serve client writes directly)
//   - No tracker updates (followers don't track other followers)
//   - No semi-sync waiting (only the leader waits for follower ACKs)
type FollowerApplier struct {
	nodeID       string
	engine       store.KVEngine
	raft         RaftNode
	logger       *zap.Logger
	appliedIndex atomic.Uint64 // the highest index this follower has applied
	snapMgr      *SnapshotManager

	stop chan struct{}
	done chan struct{}
}

func NewFollowerApplier(
	nodeID string,
	engine store.KVEngine,
	raft RaftNode,
	logger *zap.Logger,
) *FollowerApplier {
	return &FollowerApplier{
		nodeID: nodeID,
		engine: engine,
		raft:   raft,
		logger: logger,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
}

// Begins the apply loop
func (f *FollowerApplier) Start() {
	go f.applyLoop()
}

// Shuts down the applier and waits for the apply loop to exit
func (f *FollowerApplier) Stop() {
	close(f.stop)
	<-f.done
}

// Add setter (after Stop()):
func (f *FollowerApplier) SetSnapshotManager(sm *SnapshotManager) {
	f.snapMgr = sm
}

// Returns the highest log index this follower has applied. The leader reads this to compute replication lag
// in the ReplicationTracker.
func (f *FollowerApplier) AppliedIndex() uint64 {
	return f.appliedIndex.Load()
}

func (f *FollowerApplier) applyLoop() {
	defer close(f.done)

	entries := f.raft.CommittedEntries()
	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				return
			}
			if err := f.applyEntry(entry); err != nil {
				f.logger.Error("follower: failed to apply entry",
					zap.String("node", f.nodeID),
					zap.Uint64("index", entry.Index),
					zap.Error(err),
				)
			}
		case <-f.stop:
			return
		}
	}
}

func (f *FollowerApplier) applyEntry(entry CommittedEntry) error {
	var op KVOperation
	if err := json.Unmarshal(entry.Data, &op); err != nil {
		return fmt.Errorf("follower %s: apply entry %d: unmarshal: %w", f.nodeID, entry.Index, err)
	}

	switch op.Type {
	case OpPut:
		if _, err := f.engine.Put(op.Key, op.Value, store.PutOptions{
			LogIndex:  entry.Index,
			IfVersion: op.IfVersion,
		}); err != nil {
			return fmt.Errorf("follower %s: apply entry %d: put %q: %w", f.nodeID, entry.Index, op.Key, err)
		}
	case OpDelete:
		if err := f.engine.Delete(op.Key, store.DeleteOptions{}); err != nil {
			return fmt.Errorf("follower %s: apply entry %d: delete %q: %w", f.nodeID, entry.Index, op.Key, err)
		}
	}
	// Record how far we have applied so the leader can track our lag.
	f.appliedIndex.Store(entry.Index)

	// Trigger snapshot if enough entries have accumulated.
	if f.snapMgr != nil {
		f.snapMgr.MaybeTakeSnapshot(entry.Index)
	}

	f.logger.Debug("follower: applied entry",
		zap.String("node", f.nodeID),
		zap.Uint64("index", entry.Index),
		zap.String("key", op.Key),
	)

	return nil
}
