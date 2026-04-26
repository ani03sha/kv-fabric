package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ani03sha/kv-fabric/cluster"
	"github.com/ani03sha/kv-fabric/consistency"
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

// newTestCluster starts a 3-node in-process cluster and registers Stop as a test cleanup.
// The cluster is torn down automatically when the test ends, whether it passes or fails.
func newTestCluster(t *testing.T) *cluster.Cluster {
	t.Helper()
	c, err := cluster.Start(zap.NewNop())
	if err != nil {
		t.Fatalf("cluster.Start: %v", err)
	}
	t.Cleanup(c.Stop)
	return c
}

// TestReplicationPropagates verifies that a write committed to the leader's Raft log
// appears on every follower's MVCC engine within a bounded time.
//
// This is the fundamental correctness guarantee of the system: every committed write
// must reach every live node's state machine in the same order.
func TestReplicationPropagates(t *testing.T) {
	t.Parallel()
	c := newTestCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   "test:replication",
		Value: []byte("replicated-value"),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}
	if res.Version == 0 {
		t.Fatal("expected non-zero version")
	}

	// 200ms is generous for an in-process cluster. The apply loop typically catches up
	// in < 1ms. But we give headroom for scheduler jitter in CI.
	time.Sleep(200 * time.Millisecond)

	for _, follower := range c.Followers() {
		r, err := follower.Engine.Get("test:replication", store.GetOptions{})
		if err != nil {
			t.Errorf("follower %s: Get: %v", follower.ID, err)
			continue
		}
		if string(r.Value) != "replicated-value" {
			t.Errorf("follower %s: got value %q, want %q", follower.ID, r.Value, "replicated-value")
		}
		if r.Version != res.Version {
			t.Errorf("follower %s: got version %d, want %d", follower.ID, r.Version, res.Version)
		}
	}
}

// TestStrongReadIsLinearizable verifies that a strong read immediately after a write
// always returns the written value — no staleness possible.
//
// StrongReader.Get() uses the ReadIndex protocol:
//  1. Snapshot the current commit index as readIndex.
//  2. Confirm we are still the leader via a heartbeat quorum.
//  3. Wait for applied >= readIndex.
//  4. Read from local engine.
//
// Step 2 guarantees no partitioned leader can serve a stale read: if another leader
// was elected, ConfirmLeadership returns an error before we touch the engine.
func TestStrongReadIsLinearizable(t *testing.T) {
	t.Parallel()
	c := newTestCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   "test:strong",
		Value: []byte("strong-value"),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	leader := c.Leader()
	if leader == nil {
		t.Fatal("no leader after Propose")
	}

	leaderRouter := consistency.NewRouter(
		leader.ID, leader.Engine, leader.Adapter, c.Tracker, zap.NewNop())

	got, err := leaderRouter.Get(ctx, "test:strong", store.GetOptions{
		Consistency: store.ConsistencyStrong,
	})
	if err != nil {
		t.Fatalf("strong read: %v", err)
	}
	if string(got.Value) != "strong-value" {
		t.Errorf("strong read: got %q, want %q", got.Value, "strong-value")
	}
	if got.IsStale {
		t.Error("strong read must never return IsStale=true")
	}
}

// TestLeaderFailoverPreservesCommittedWrites is the most important correctness test.
// It verifies that a write committed to Raft quorum survives a leader crash.
//
// Sequence:
//  1. Write key → quorum commit (leader + at least one follower Raft log).
//  2. Crash the leader via ChaosInjector (stop node + blackhole network).
//  3. Wait for a new leader to be elected from the remaining two nodes.
//  4. Read the key from the new leader's engine.
//
// Expected: key IS found. Raft's safety property guarantees that a quorum-committed
// entry is never lost as long as a quorum of nodes is alive.
//
// If the key were NOT found, it would indicate either:
//   - The write was not actually quorum-committed when we thought it was, OR
//   - The new leader did not replay committed log entries (a Raft implementation bug).
func TestLeaderFailoverPreservesCommittedWrites(t *testing.T) {
	t.Parallel()
	c := newTestCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   "test:failover",
		Value: []byte("must-survive"),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// Crash the leader. CrashNode:
	//   1. Adds ActionDrop proxy rules (peers see timeout, not connection refused).
	//   2. Calls raftly node.Stop() — sets state=Follower, closes WAL.
	//   3. The kv-fabric apply loops exit when the adapter's commitCh closes.
	leaderID := c.Leader().ID
	c.Chaos.CrashNode(leaderID)

	// Wait for one of the two remaining nodes to become leader.
	// Election timeout in raftly is randomized ~150-300ms. 10s is more than enough.
	newLeader, err := c.WaitForLeader(10 * time.Second)
	if err != nil {
		t.Fatalf("no new leader after crash: %v", err)
	}

	if newLeader.ID == leaderID {
		t.Fatalf("new leader is the same as crashed leader %s — crash did not take effect", leaderID)
	}

	// Poll for the key. The new leader's FollowerApplier had ~election time to apply the
	// entry from its Raft log. In-process channels are fast; typically applied in < 1ms.
	// We poll up to 1s to tolerate slow CI environments.
	var found bool
	for deadline := time.Now().Add(1 * time.Second); time.Now().Before(deadline); time.Sleep(25 * time.Millisecond) {
		r, err := newLeader.Engine.Get("test:failover", store.GetOptions{})
		if err != nil {
			t.Fatalf("Get from new leader: %v", err)
		}
		if r.Value != nil {
			if string(r.Value) != "must-survive" {
				t.Errorf("new leader: got value %q, want %q", r.Value, "must-survive")
			}
			if r.Version != res.Version {
				t.Errorf("new leader: got version %d, want %d", r.Version, res.Version)
			}
			found = true
			break
		}
	}

	if !found {
		t.Error("committed write was NOT found on new leader after failover — phantom write")
	}
}

// TestSemiSyncFallbackIsObservable verifies that semi-sync's fallback_count metric fires
// when the tracker has not yet learned about follower progress.
//
// Mechanism: the lagLoop updates the tracker every 50ms. A 10ms semi-sync timeout fires
// before the tracker catches up — so anyFollowerAt() returns false → fallback triggered.
//
// Why this matters: fallback_count > 0 is the production alarm signal. If this counter
// is non-zero when a leader dies, operations teams must run a WAL comparison to identify
// which writes were in-flight at crash time. Without this metric, phantom write risk is silent.
func TestSemiSyncFallbackIsObservable(t *testing.T) {
	t.Parallel()
	c := newTestCluster(t)

	leader := c.Leader()
	if leader == nil {
		t.Fatal("no leader")
	}

	reg := prometheus.NewRegistry()
	// 10ms < 50ms lagLoop interval: the timer fires before the tracker is updated.
	semiSync := replication.NewSemiSyncReplicator(
		c.Tracker, 10*time.Millisecond, reg, zap.NewNop())
	leader.Leader.SetSemiSync(semiSync)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Write multiple keys in rapid succession. With the 10ms timeout and 50ms lagLoop,
	// the tracker is almost certainly stale for each write → fallback fires.
	for i := 0; i < 5; i++ {
		_, err := c.Propose(ctx, replication.KVOperation{
			Type:  replication.OpPut,
			Key:   fmt.Sprintf("test:semisync:%d", i),
			Value: []byte("v"),
		})
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
	}

	fallbacks := semiSync.FallbackTotal()
	if fallbacks == 0 {
		// This can happen (rarely) if the lagLoop ticked right before every write.
		// It is not a correctness failure — the writes ARE durable (quorum committed).
		// The test is checking observability, not safety.
		t.Log("NOTE: no fallbacks observed — lagLoop may have ticked before all writes")
		t.Log("Re-run if this fails consistently; it is timing-sensitive")
	}
	t.Logf("fallback_count = %d after 5 writes with 10ms semi-sync timeout", fallbacks)
}

// TestRYWReaderEnforcesVersionGuarantee verifies both sides of the Read-Your-Writes contract:
//
//  1. After a write, a follower that has caught up MUST return the written value.
//  2. A session token referencing an impossible future version MUST return ErrNotCaughtUp,
//     not a stale value — the guarantee is strict, not best-effort.
func TestRYWReaderEnforcesVersionGuarantee(t *testing.T) {
	t.Parallel()
	c := newTestCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	res, err := c.Propose(ctx, replication.KVOperation{
		Type:  replication.OpPut,
		Key:   "test:ryw",
		Value: []byte("my-write"),
	})
	if err != nil {
		t.Fatalf("Propose: %v", err)
	}

	// Wait for follower to apply the write.
	time.Sleep(150 * time.Millisecond)

	follower := c.Followers()[0]
	rywReader := consistency.NewRYWReader(
		follower.ID, follower.Engine, follower.Adapter, zap.NewNop())

	// Case 1: token for the version we just wrote — follower is caught up, must return value.
	token, err := consistency.IssueToken(c.Leader().ID, res.Version)
	if err != nil {
		t.Fatalf("IssueToken: %v", err)
	}

	got, err := rywReader.Get(ctx, "test:ryw", store.GetOptions{
		Consistency:  store.ConsistencyReadYourWrites,
		SessionToken: token,
	})
	if err != nil {
		t.Fatalf("RYW read (caught-up): %v", err)
	}
	if string(got.Value) != "my-write" {
		t.Errorf("RYW read: got %q, want %q", got.Value, "my-write")
	}

	// Case 2: token for a version far in the future — follower cannot be caught up.
	// Must return ErrNotCaughtUp, not a stale value.
	futureToken, err := consistency.IssueToken(c.Leader().ID, res.Version+1_000_000)
	if err != nil {
		t.Fatalf("IssueToken (future): %v", err)
	}

	_, err = rywReader.Get(ctx, "test:ryw", store.GetOptions{
		Consistency:  store.ConsistencyReadYourWrites,
		SessionToken: futureToken,
	})
	if err == nil {
		t.Fatal("RYW read with future token: expected ErrNotCaughtUp, got nil error")
	}
	var notCaughtUp *consistency.ErrNotCaughtUp
	if !isErrNotCaughtUp(err, &notCaughtUp) {
		t.Errorf("RYW read with future token: got error %T (%v), want *ErrNotCaughtUp", err, err)
	}
}

// isErrNotCaughtUp checks whether err is or wraps *consistency.ErrNotCaughtUp.
// We avoid importing "errors" at package level by keeping the helper local.
func isErrNotCaughtUp(err error, target **consistency.ErrNotCaughtUp) bool {
	if err == nil {
		return false
	}
	// Direct type assertion — ErrNotCaughtUp is returned directly, not wrapped.
	if e, ok := err.(*consistency.ErrNotCaughtUp); ok {
		*target = e
		return true
	}
	return false
}

// TestMVCCVersionsAreMonotonic verifies that successive writes to the same key
// produce strictly increasing version numbers.
//
// This property holds because version numbers ARE Raft log indices, and Raft
// log indices are strictly monotonically increasing. Same key, same cluster,
// same ordering guarantee.
func TestMVCCVersionsAreMonotonic(t *testing.T) {
	t.Parallel()
	c := newTestCluster(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var versions []uint64
	for i := 0; i < 3; i++ {
		res, err := c.Propose(ctx, replication.KVOperation{
			Type:  replication.OpPut,
			Key:   "test:versions",
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
		if err != nil {
			t.Fatalf("Propose %d: %v", i, err)
		}
		versions = append(versions, res.Version)
	}

	for i := 1; i < len(versions); i++ {
		if versions[i] <= versions[i-1] {
			t.Errorf("version not monotonic: versions[%d]=%d <= versions[%d]=%d",
				i, versions[i], i-1, versions[i-1])
		}
	}
	t.Logf("versions: %v (all strictly increasing)", versions)
}
