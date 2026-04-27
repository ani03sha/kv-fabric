package cluster

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/ani03sha/raftly/chaos"
	"github.com/ani03sha/raftly/raft"
	"github.com/ani03sha/raftly/transport"
	"go.uber.org/zap"
)

// Node is one kv-fabric node running inside the in-process cluster. Every node gets a LeaderReplicator AND a
// FollowerApplier wired up. LeaderReplicator.Propose() checks IsLeader() before doing anything, so it is safe to
// call on any node: non-leaders return a redirect error. This means leader failover is handled naturally: after election,
// Propose() on the new leader just works.
type Node struct {
	ID       string
	Raft     *raft.RaftNode
	Adapter  *replication.RaftlyAdapter
	Engine   *store.MVCCEngine
	Leader   *replication.LeaderReplicator
	Follower *replication.FollowerApplier
	SnapMgr  *replication.SnapshotManager
	walDir   string
}

// Cluster is a fully wired in-process kv-fabric cluster backed by raftly InMemTransport.
// It is the shared foundation for all scenarios and the benchmark harness.
//
// In-process vs Docker: InMemTransport lets us run real Raft consensus — elections, log replication, commit quorum
// with zero network overhead.
//
// ChaosInjector injects the same failure modes (crash, partition, lag) that would happen on real hardware.
// The scenarios produce real numbers because the apply path, MVCC engine, and GC are all exercised exactly as they would be in production.
type Cluster struct {
	Nodes   map[string]*Node
	Proxy   *transport.NetworkProxy
	Chaos   *chaos.ChaosInjector
	Tracker *replication.ReplicationTracker

	stopLag chan struct{}
	logger  *zap.Logger
}

// This function builds and starts a 3-node in-process cluster. It blocks until a leader is elected or until the
// timeout expires.
//
// Each node gets its own:
//   - WAL in a temporary directory (cleaned up by Stop())
//   - MVCCEngine with a 10-minute GC interval (won't fire during short scenario runs)
//   - RaftlyAdapter bridging raftly to our RaftNode interface
//   - LeaderReplicator and FollowerApplier (both always running)
//
// The shared NetworkProxy allows ChaosInjector to intercept any Raft message between any pair of nodes: the same control
// surface used by raftly's own scenarios.
func Start(logger *zap.Logger) (*Cluster, error) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}

	proxy := transport.NewNetworkProxy()
	registry := transport.NewInMemRegistry()
	tracker := replication.NewReplicationTracker()

	for _, id := range nodeIDs {
		tracker.Track(id)
	}

	nodes := make(map[string]*Node, 3)
	raftNodes := make(map[string]*raft.RaftNode, 3)

	// Build peer configs - each node knows about all others.
	peers := make([]raft.PeerConfig, 0, 2)
	for _, id := range nodeIDs {
		peers = append(peers, raft.PeerConfig{ID: id, Address: id})
	}

	for _, id := range nodeIDs {
		walDir, err := os.MkdirTemp("", "kv-fabric-wal-"+id+"-*")
		if err != nil {
			return nil, fmt.Errorf("create WAL dir for %s: %w", id, err)
		}

		cfg := raft.DefaultConfig(id)
		cfg.DataDir = walDir

		// Peer list excludes self
		cfg.Peers = cfg.Peers[:0]
		for _, p := range peers {
			if p.ID != id {
				cfg.Peers = append(cfg.Peers, p)
			}
		}

		xport := transport.NewInMemTransport(id, proxy, registry)
		raftNode, err := raft.NewRaftNode(cfg, xport)
		if err != nil {
			return nil, fmt.Errorf("create raft node %s: %w", id, err)
		}

		adapter := replication.NewRaftlyAdapter(raftNode)
		engine := store.NewMVCCEngine(id, 10*time.Minute)

		// One SnapshotManager per node. 1000-entry threshold is appropriate for
		// scenario runs (each scenario writes ~100–500 entries). The 5-minute
		// interval is a fallback; threshold is the hot path.
		snapMgr := replication.NewSnapshotManager(
			id,
			engine,
			walDir, // same dir as the WAL — one place to back up
			1000,   // threshold: snapshot every 1000 applied entries
			5*time.Minute,
			logger,
		)

		// Load any snapshot from a previous run. For fresh nodes this is a no-op.
		// Must happen before raftNode.Start() so the engine is populated before
		// the first committed entry arrives.
		if _, err := snapMgr.LoadLatestSnapshot(); err != nil {
			return nil, fmt.Errorf("load snapshot for %s: %w", id, err)
		}

		leaderRepl := replication.NewLeaderReplicator(id, engine, adapter, tracker, logger)
		followerAppl := replication.NewFollowerApplier(id, engine, adapter, logger)

		leaderRepl.SetSnapshotManager(snapMgr)
		followerAppl.SetSnapshotManager(snapMgr)

		node := &Node{
			ID:       id,
			Raft:     raftNode,
			Adapter:  adapter,
			Engine:   engine,
			Leader:   leaderRepl,
			Follower: followerAppl,
			SnapMgr:  snapMgr,
			walDir:   walDir,
		}
		nodes[id] = node
		raftNodes[id] = raftNode
	}

	// nodeFactory is used by ChaosInjector.RestartNode to recreate a crashed node.
	// The new node opens the same WAL dir, recovering term, votedFor, and log.
	nodeFactory := func(nodeID string) (*raft.RaftNode, error) {
		node := nodes[nodeID]
		cfg := raft.DefaultConfig(nodeID)
		cfg.DataDir = node.walDir
		cfg.Peers = cfg.Peers[:0]
		for _, p := range peers {
			if p.ID != nodeID {
				cfg.Peers = append(cfg.Peers, p)
			}
		}
		xport := transport.NewInMemTransport(nodeID, proxy, registry)
		return raft.NewRaftNode(cfg, xport)
	}

	injector := chaos.NewChaosInjector(proxy, raftNodes, nodeFactory)

	c := &Cluster{
		Nodes:   nodes,
		Proxy:   proxy,
		Chaos:   injector,
		Tracker: tracker,
		stopLag: make(chan struct{}),
		logger:  logger,
	}

	// Start all raft nodes and apply loops.
	for _, node := range nodes {
		if err := node.Raft.Start(); err != nil {
			return nil, fmt.Errorf("start raft node %s: %w", node.ID, err)
		}
		node.Leader.Start()
		node.SnapMgr.Start()
	}

	// Wait for leader election before returning.
	if err := c.waitForLeader(5 * time.Second); err != nil {
		return nil, fmt.Errorf("no leader elected: %w", err)
	}

	// lagLoop reads each follower's applied index every 50ms and pushes it into the shared tracker.
	// This compensates for raftly not exposing per-follower matchIndex in its public API.
	go c.lagLoop()

	return c, nil
}

// Leader returns the node that is currently the Raft leader, or nil if there is none.
func (c *Cluster) Leader() *Node {
	for _, node := range c.Nodes {
		if node.Adapter.IsLeader() {
			return node
		}
	}
	return nil
}

// Followers returns all nodes that are not currently the Raft leader.
func (c *Cluster) Followers() []*Node {
	var followers []*Node
	for _, node := range c.Nodes {
		if !node.Adapter.IsLeader() {
			followers = append(followers, node)
		}
	}
	return followers
}

// WaitForLeader blocks until a leader is elected or timeout expires.
// Use this after a chaos injection (CrashNode, PartitionNode) to wait for re-election.
func (c *Cluster) WaitForLeader(timeout time.Duration) (*Node, error) {
	if err := c.waitForLeader(timeout); err != nil {
		return nil, err
	}
	return c.Leader(), nil
}

// Propose writes a KVOperation to the cluster via whoever is currently the leader.
// Returns an error if no leader is available.
func (c *Cluster) Propose(ctx context.Context, op replication.KVOperation) (*store.PutResult, error) {
	leader := c.Leader()
	if leader == nil {
		return nil, fmt.Errorf("no leader")
	}
	return leader.Leader.Propose(ctx, op)
}

// Stop shuts down all nodes and cleans up temporary WAL directories.
func (c *Cluster) Stop() {
	close(c.stopLag)

	for _, node := range c.Nodes {
		node.SnapMgr.Stop()
		node.Leader.Stop()
		node.Raft.Stop()
		node.Adapter.Close()
		node.Engine.Close()
		os.RemoveAll(node.walDir)
	}
}

// lagLoop reads each node's applied index every 50ms and updates the shared tracker.
// This gives the consistency layer accurate replication lag data even though
// raftly doesn't expose per-follower matchIndex in its public API.
func (c *Cluster) lagLoop() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			leader := c.Leader()
			if leader != nil {
				c.Tracker.UpdateLeaderCommitIndex(leader.Adapter.CommitIndex())
			}
			for _, node := range c.Nodes {
				if !node.Adapter.IsLeader() {
					c.Tracker.UpdateFollower(node.ID, node.Adapter.AppliedIndex())
				}
			}
		case <-c.stopLag:
			return
		}
	}
}

// waitForLeader polls until a leader appears or timeout fires.
func (c *Cluster) waitForLeader(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Leader() != nil {
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("no leader after %v", timeout)
}
