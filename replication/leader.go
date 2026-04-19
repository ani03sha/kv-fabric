package replication

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// OpType describes the kind of separation encoded in a log entry
type OpType int

const (
	OpPut OpType = iota
	OpDelete
	OpBeginTxn
	OpCommitTxn
)

// This is the unit that travels through Raft log.
//
// Every client write is encoded as a KVOperation, proposed to Raft, and delivered to every node's
// apply loop in the same order.
// This is what makes the state machine consistent: same inputs, same order. This is what makes the state machine:
// same order, same state on every node.
type KVOperation struct {
	Type  OpType `json:"type"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`
	TxnID uint64 `json:"txn_id,omitempty"`
}

// This is the log entry that Raft has committed to the quorum. The index becomes the
// version number in the MVCC engine.
type CommittedEntry struct {
	Index uint64 // Raft log index - used directly as MVCC version number.
	Data  []byte // serialized KVOperation
}

// This comes from the Raft library. The concrete raftly node satisfies this in main.go.
// Everything in the replication package talks to this interface - never to raftly types directly.
type RaftNode interface {
	// Propose submits a command to the Raft log. Returns the assigned log index once Raft has accepted the
	// proposal (before quorum commit). Returns an error if this node is not the leader.
	Propose(ctx context.Context, data []byte) (index uint64, err error)

	// CommittedEntries returns a channel of entries committed by the quorum. Both leaders and followers receive
	// the same entry in the same order.
	CommittedEntries() <-chan CommittedEntry

	// CommitIndex returns the highest log index committed by the quorum.
	CommitIndex() uint64

	// AppliedIndex returns the highest log index this node has applied.
	AppliedIndex() uint64

	// IsLeader returns true if this node is the current Raft leader.
	IsLeader() bool

	// LeaderID returns the node ID of the current Raft cluster.
	LeaderID() string

	// Follower progress returns a map of nodeID -> matchIndex for all peers. Only meaningful on the leader;
	// followers return an empty map.
	FollowerProgress() map[string]uint64

	// Sends a heartbeat to a quorum of followers and confirms this node is still the leader.
	// Returns an error if leadership cannot be confirmed, e.g., partitioned or a new leader was elected.
	// This is the safety check in the ReadIndex protocol.
	ConfirmLeadership(ctx context.Context) error
}

// This is a write that has been proposed to Raft but not yet applied. The calling goroutine (HTTP handler)
// blocks on result until applyLoop signals it.
type pendingOp struct {
	// Buffered channel of size 1; applyLoop can always send without blocking, even if the caller has already
	// given up due to context cancellation.
	result chan applyResult
}

type applyResult struct {
	putResult *store.PutResult
	err       error
}

// This runs on the leader node. It proposes client writes to Raft and applies committed entries to the engine.
type LeaderReplicator struct {
	nodeID  string
	engine  store.KVEngine
	raft    RaftNode
	tracker *ReplicationTracker
	logger  *zap.Logger

	pendingMu sync.Mutex
	pending   map[uint64]*pendingOp // log index -> blocked client goroutine

	stop chan struct{}
	done chan struct{}
}

func NewLeaderReplicator(
	nodeID string,
	engine store.KVEngine,
	raft RaftNode,
	tracker *ReplicationTracker,
	logger *zap.Logger,
) *LeaderReplicator {
	return &LeaderReplicator{
		nodeID:  nodeID,
		engine:  engine,
		raft:    raft,
		tracker: tracker,
		logger:  logger,
		pending: make(map[uint64]*pendingOp),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Launches the apply loop and follower tracking loop. Must be called before Propose().
func (l *LeaderReplicator) Start() {
	go l.applyLoop()
	go l.trackFollowersLoop()
}

// Shuts down the replicator and waits for apply loop to exit.
func (l *LeaderReplicator) Stop() {
	close(l.stop)
	<-l.done
}

// Propose submits a write to Raft and blocks until it is committed and applied.
//
// This is the critical path for every client write:
//  1. Encode the operation
//  2. Submit to Raft (returns after leader accepts, before quorum commits)
//  3. Register a pending op so applyLoop can signal us
//  4. Block on the result channel — or unblock early if ctx is cancelled
//
// The ctx carries the client's deadline. If the client disconnects or times out,
// ctx.Done() is closed and Propose returns an error immediately. The pending op
// is cleaned up so it doesn't leak.
func (l *LeaderReplicator) Propose(ctx context.Context, op KVOperation) (*store.PutResult, error) {
	if l.raft.IsLeader() {
		return nil, fmt.Errorf("not the leader: redirect to %s", l.raft.LeaderID())
	}

	data, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("propose: marshal: %w", err)
	}

	// Submit to Raft. This returns the log index once the leader has accepted the entry locally.
	// It has not yet been committed to quorum at this point.
	index, err := l.raft.Propose(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("propose: raft rejected: %w", err)
	}

	// Register this goroutine as the waiter for log index `index`. When applyLoop commits and
	// applies this entry, it sends the result here.
	resultCh := make(chan applyResult, 1) // buffered: applyLoop never blocks on send
	l.pendingMu.Lock()
	l.pending[index] = &pendingOp{result: resultCh}
	l.pendingMu.Unlock()

	select {
	case res := <-resultCh:
		return res.putResult, res.err
	case <-ctx.Done():
		// Client gave up. Remove the pending entry so applyLoop doesn't try to signal a goroutine that's
		// no longer listening
		l.pendingMu.Lock()
		delete(l.pending, index)
		l.pendingMu.Unlock()
		return nil, fmt.Errorf("propose: %w", ctx.Err())
	}
}

// This is the core of the state machine on the leader. It reads committed entries from Raft
// in order and applies each one.
func (l *LeaderReplicator) applyLoop() {
	defer close(l.done)

	entries := l.raft.CommittedEntries()
	for {
		select {
		case entry, ok := <-entries:
			if !ok {
				return // Raft channel is closed: node is shutting down
			}
			if err := l.applyEntry(entry); err != nil {
				l.logger.Error("leader: failed to apply entry",
					zap.String("node", l.nodeID),
					zap.Uint64("index", entry.Index),
					zap.Error(err))
			}
		case <-l.stop:
			return
		}
	}
}

// Decodes and applies one committed log entry. Signals the waiting client goroutine
// (if any) with the result.
func (l *LeaderReplicator) applyEntry(entry CommittedEntry) error {
	var op KVOperation
	if err := json.Unmarshal(entry.Data, &op); err != nil {
		return fmt.Errorf("apply entry %d: unmarshal: %w", entry.Index, err)
	}

	start := time.Now()
	var result applyResult

	switch op.Type {
	case OpPut:
		r, err := l.engine.Put(op.Key, op.Value, store.PutOptions{
			// LogIndex makes the Raft log index the version number.
			// Every node applies this same entry with this same index,
			// so every node's engine assigns version = entry.Index to this write.
			LogIndex: entry.Index,
		})
		result = applyResult{putResult: r, err: err}
	case OpDelete:
		err := l.engine.Delete(op.Key, store.DeleteOptions{})
		result = applyResult{err: err}
	default:
		// TODO
		result = applyResult{}
	}

	// Advance the lag tracker's view of where the cluster is.
	l.tracker.UpdateLeaderCommitIndex(entry.Index)
	l.tracker.UpdateAvgWriteLatency(time.Since(start).Milliseconds())

	l.logger.Debug("leader: applied entry",
		zap.String("node", l.nodeID),
		zap.Uint64("index", entry.Index),
		zap.String("key", op.Key))

	// Wake up the client goroutine that's blocked in Propose() for this index.
	l.pendingMu.Lock()
	if p, ok := l.pending[entry.Index]; ok {
		delete(l.pending, entry.Index)
		p.result <- result // never blocks - channel is buffered
	}
	l.pendingMu.Unlock()

	return result.err
}

// Polls Raft for follower progress and updates the tracker. Runs in a separate goroutine. 50ms poll
// interval keeps lag estimates fresh.
func (l *LeaderReplicator) trackFollowersLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			for nodeID, matchIndex := range l.raft.FollowerProgress() {
				l.tracker.UpdateFollower(nodeID, matchIndex)
			}
			l.tracker.UpdateLeaderCommitIndex(l.raft.CommitIndex())
		case <-l.stop:
			return
		}
	}
}
