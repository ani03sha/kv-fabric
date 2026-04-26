package replication

import (
	"context"
	"fmt"

	"github.com/ani03sha/raftly/raft"
)

// This struct wraps raftly's *raft.RaftNode to satisfy our RaftNode interface.
//
// API differences bridged here:
//   - Propose: raftly has no context parameter: we race it against ctx.Done()
//   - CommittedEntries: raftly uses LogEntry; we translate to CommittedEntry
//   - FollowerProgress: raftly does not expose per-follower matchIndex publicly;
//     the cluster package feeds it via UpdateFollower on the shared tracker instead
//   - ConfirmLeadership: raftly has no explicit ReadIndex; we approximate by
//     proposing an empty entry: if it commits, we are still the quorum leader
type RaftlyAdapter struct {
	node     *raft.RaftNode
	commitCh chan CommittedEntry
	stopCh   chan struct{}
}

func NewRaftlyAdapter(node *raft.RaftNode) *RaftlyAdapter {
	a := &RaftlyAdapter{
		node:     node,
		commitCh: make(chan CommittedEntry),
		stopCh:   make(chan struct{}),
	}
	go a.bridge()
	return a
}

func (a *RaftlyAdapter) Close() {
	close(a.stopCh)
}

// This function translates raftly's committed LogEntry stream into our CommittedEntry stream.
// Entries with empty Data are leader no-ops that raftly writes on election win.
// We skip them because applyEntry in both LeaderReplicator and FollowerApplied would fail trying to JSON-unmarshal
// nil bytes into a KVOperation.
func (a *RaftlyAdapter) bridge() {
	for {
		select {
		case entry, ok := <-a.node.CommitCh():
			if !ok {
				close(a.commitCh)
				return
			}
			if len(entry.Data) == 0 {
				continue // skip leader no-ops — raftly notifies Propose() callers internally
			}
			select {
			case a.commitCh <- CommittedEntry{Index: entry.Index, Data: entry.Data}:
			case <-a.stopCh:
				return
			}
		case <-a.stopCh:
			return
		}
	}
}

// Propose submits data to the Raft cluster. Blocks until the entry is committed by a quorum or until ctx is cancelled.
//
// raftly's Propose has no context parameter. We run it in a goroutine and race it against ctx.Done().
// If ctx fires first we return immediately, but the goroutine keeps running until raftly's internal proposal either
// commits or the node stops: a known goroutine lifetime tradeoff until raftly adds context support
func (a *RaftlyAdapter) Propose(ctx context.Context, data []byte) (uint64, error) {
	type outcome struct {
		index uint64
		err   error
	}
	ch := make(chan outcome, 1)

	go func() {
		index, _, err := a.node.Propose(data)
		ch <- outcome{index, err}
	}()

	select {
	case o := <-ch:
		return o.index, o.err
	case <-ctx.Done():
		return 0, fmt.Errorf("propose: %w", ctx.Err())
	}
}

// Returns the channel of committed entries destined for the apply loop.
func (a *RaftlyAdapter) CommittedEntries() <-chan CommittedEntry {
	return a.commitCh
}

// Returns the highest log index committed by the quorum on this node.
func (a *RaftlyAdapter) CommitIndex() uint64 {
	return a.node.Status().CommitIndex
}

// Returns the highest log index this node has applied to its state machine.
func (a *RaftlyAdapter) AppliedIndex() uint64 {
	return a.node.Status().LastApplied
}

func (a *RaftlyAdapter) IsLeader() bool {
	return a.node.IsLeader()
}

func (a *RaftlyAdapter) LeaderID() string {
	return a.node.LeaderID()
}

// Raftly's peer matchIndex tracking is internal to RaftNode and not exposed publicly.
// The cluster package compensates by reading each FollowerApplier's AppliedIndex directly and calling tracker.UpdateFollower,
// giving the tracker real per-follower lag data without needing raftly to expose it.
func (a *RaftlyAdapter) FollowerProgress() map[string]uint64 {
	return map[string]uint64{}
}

// ConfirmLeadership verifies this node is still the active Raft leader by proposing an empty no-op entry and waiting
// for quorum to commit it.
//
// This is the ReadIndex safety check for strong consistency reads: if another node has become leader, Propose will
// return a "not leader" error, and the read is rejected. The empty Data entry is filtered by the bridge and never
// reaches applyEntry.
func (a *RaftlyAdapter) ConfirmLeadership(ctx context.Context) error {
	if !a.node.IsLeader() {
		return fmt.Errorf("not leader: redirect to %s", a.node.LeaderID())
	}
	_, err := a.Propose(ctx, nil) // nil → skipped by bridge, committed by raftly internally
	return err
}
