# KV Fabric

KV Fabric (or kv-fabric) is a distributed key-value store that treats consistency as a **measurable engineering tradeoff**, not a binary switch. Every read in **kv-fabric** carries a consistency mode, and the benchmark harness produces hard numbers for what each mode costs.

**What this project proves:**

| Claim                                               | How it is proven                                                       |
| --------------------------------------------------- | ---------------------------------------------------------------------- |
| Linearizable reads cost a full Raft RTT             | `strong` vs `eventual` throughput in `make bench`                      |
| Semi-sync replication has a phantom durability trap | `make scenario-phantom` crashes the leader and shows the missing write |
| A 340 ms follower lag causes double-bookings        | `make scenario-booking` reproduces the race with real network delay    |
| MVCC version accumulation bloats memory             | `make scenario-mvcc-bloat` shows version counts before and after GC    |
| MVCC prevents dirty reads                           | `make scenario-dirty-read` shows snapshot isolation end-to-end         |

---

## Architecture

### Write path

```
Client
  │
  ▼
c.Propose(ctx, KVOperation)           ← cluster.go picks the current leader
  │
  ▼
LeaderReplicator.Propose()            ← replication/leader.go
  │  1. Pre-register pendingOp keyed by reqID = fmt.Sprintf("%p", resultCh)
  │  2. json.Marshal(KVOperation{..., RequestID: reqID})
  │  3. raft.Propose(ctx, data)        ← submits to Raft, blocks until quorum commit
  │  4. block on resultCh
  │
  ▼
RaftlyAdapter.Propose()               ← replication/raftly_adapter.go
  │  wraps raftly.RaftNode.Propose()
  │  returns assigned log index
  │
  ▼
Raft quorum commit
  │  Leader + 1 follower must accept AppendEntries
  │  Committed entry flows to all nodes via CommittedEntries() channel
  │
  ▼
LeaderReplicator.applyLoop()          ← replication/leader.go
  │  applyEntry(CommittedEntry)
  │  → engine.Put(key, value, PutOptions{LogIndex: entry.Index})
  │  → tracker.UpdateLeaderCommitIndex(entry.Index)
  │  → SnapshotManager.MaybeTakeSnapshot(entry.Index)  ← log compaction trigger
  │  → pending[op.RequestID].result <- applyResult{putResult, nil}
  │
  ▼
resultCh unblocks in Propose()
Client receives PutResult{Version: entry.Index}
```

The Raft log index **is** the MVCC version number. Every node that applies this entry calls `engine.Put(..., LogIndex: entry.Index)`, so they all assign the same version to the same write. Same inputs, same order, same state — this is what makes the state machine consistent.

### Read path (per consistency mode)

```
Client
  │
  ├─ strong ──────────────────────────────────────────────────────────────┐
  │    StrongReader (consistency/strong.go)                               │
  │    1. readIndex = raft.CommitIndex()  ← snapshot                     │
  │    2. raft.ConfirmLeadership(ctx)     ← nil Propose, heartbeat quorum │
  │    3. waitForApplied(readIndex)       ← poll with exponential backoff │
  │    4. engine.Get(key)                 ← local read, guaranteed fresh  │
  │                                                                        │
  ├─ eventual ─────────────────────────────────────────────────────────────┤
  │    EventualReader (consistency/eventual.go)                           │
  │    1. engine.Get(key)                 ← local read, zero RTT          │
  │    2. tag IsStale, LagMs from tracker ← observability surface         │
  │                                                                        │
  ├─ read-your-writes ─────────────────────────────────────────────────────┤
  │    RYWReader (consistency/readyourwrites.go)                          │
  │    1. decode SessionToken (base64 JSON: {nodeID, writeVersion})       │
  │    2. if raft.AppliedIndex() >= token.WriteVersion → local read       │
  │    3. else → ErrNotCaughtUp(503) → caller falls back to leader        │
  │                                                                        │
  └─ monotonic ────────────────────────────────────────────────────────────┘
       MonotonicReader (consistency/monotonic.go)
       1. if raft.AppliedIndex() >= opts.MinVersion → local read
       2. else → ErrNotCaughtUp(503) → caller falls back to leader
       (watermark lives in the client; server is stateless)
```

### Cluster topology

**In-process (scenarios and benchmarks)**

```
┌────────────────────────────────────────────────────────────────────────┐
│                          OS process                                    │
│                                                                        │
│  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐               │
│  │   node-1     │   │   node-2     │   │   node-3     │               │
│  │  (leader)    │   │  (follower)  │   │  (follower)  │               │
│  │              │   │              │   │              │               │
│  │ MVCCEngine   │   │ MVCCEngine   │   │ MVCCEngine   │               │
│  │ LeaderRepl   │   │ LeaderRepl   │   │ LeaderRepl   │               │
│  │ SnapManager  │   │ SnapManager  │   │ SnapManager  │               │
│  │ RaftlyAdapter│   │ RaftlyAdapter│   │ RaftlyAdapter│               │
│  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘               │
│         │                  │                  │                        │
│         └──────────────────┴──────────────────┘                        │
│                    InMemTransport / NetworkProxy                        │
│                    (ChaosInjector intercepts here)                      │
└────────────────────────────────────────────────────────────────────────┘
```

**Networked (`make cluster-start` / `make docker-cluster-start`)**

```
  kvctl (gRPC client)
     │  :9001 / :9002 / :9003
     │
     ▼
┌──────────────┐   Raft gRPC   ┌──────────────┐   Raft gRPC   ┌──────────────┐
│   node-1     │◄─────────────►│   node-2     │◄─────────────►│   node-3     │
│  :7001 raft  │               │  :7002 raft  │               │  :7003 raft  │
│  :9001 kv    │               │  :9002 kv    │               │  :9003 kv    │
│              │               │              │               │              │
│ GRPCTransport│               │ GRPCTransport│               │ GRPCTransport│
│ MVCCEngine   │               │ MVCCEngine   │               │ MVCCEngine   │
│ LeaderRepl   │               │ LeaderRepl   │               │ LeaderRepl   │
│ SnapManager  │               │ SnapManager  │               │ SnapManager  │
│ KVServer     │               │ KVServer     │               │ KVServer     │
└──────────────┘               └──────────────┘               └──────────────┘
   data/node-1/                  data/node-2/                  data/node-3/
   (WAL + snapshots)             (WAL + snapshots)             (WAL + snapshots)
```

Non-leader nodes return a `redirect_to` address in the gRPC response. `kvctl` follows the redirect and retries against the leader — no internal server-to-server forwarding, which keeps traces unambiguous.

InMemTransport carries real Raft consensus — elections, log replication, commit quorum — with zero network overhead. ChaosInjector injects the same failure modes (crash, partition, lag) that happen on real hardware by intercepting messages at the NetworkProxy layer.

---

## Package dependency graph

```
cmd/scenarios  cmd/benchmark  cmd/kvctl  cmd/server
      │               │            │          │
      └───────┬────────┘            │          │
              ▼                     ▼          ▼
           cluster              client      server ──── proto/
              │                                │
              ▼                                │
        replication ◄──────────────── consistency
              │                                │
              ▼                                │
            store ◄─────────────────────────────┘
              │
              ▼
         raftly (external: github.com/ani03sha/raftly)
```

**Rule**: lower packages never import upper ones. `store` knows nothing about Raft. `replication` knows nothing about HTTP or gRPC. `consistency` imports `store` and `replication` but not `cluster`. This keeps each layer testable in isolation.

---

## Packages

### `store/` — MVCC storage engine

The core storage contract is `KVEngine`:

```go
type KVEngine interface {
    Put(key string, value []byte, opts PutOptions) (*PutResult, error)
    Get(key string, opts GetOptions) (*GetResult, error)
    Delete(key string, opts DeleteOptions) error
    Scan(start, end string, limit int) ([]KVPair, error)
    Snapshot() (Snapshot, error)
    ApplySnapshot(Snapshot) error
    Stats() EngineStats
    Close() error
}
```

`MVCCEngine` implements this. Each `Put` appends a new `Version` to the key's chain; old versions are never overwritten. This is what enables snapshot reads and prevents dirty reads.

**Version numbering**: when `PutOptions.LogIndex` is non-zero (the replication path), the engine uses the Raft log index as the version number. When zero (unit tests), it uses an internal monotonic counter. Because every node applies the same log entry with the same index, every node assigns the same version to the same write. Consistency without coordination.

**GC**: the GC goroutine runs on a configurable interval. The effective GC horizon is `min(replicationHorizon, oldestActivePin)`. The replication horizon advances when all followers have applied at least that version. The pin mechanism lets long-running scan transactions anchor a snapshot: GC will not collect any version at or above the pin until `UnpinVersion` is called.

**Snapshots**: `Snapshot()` serializes the current live state (latest non-deleted version of each key) into a portable byte blob. The snapshot version is `max(gcHorizon, highest live entry version)` — this ensures the version reflects the actual applied index, not just the GC watermark (which lags behind when GC hasn't fired recently).

### `replication/` — Raft integration and log compaction

| File                    | Responsibility                                                                    |
| ----------------------- | --------------------------------------------------------------------------------- |
| `leader.go`             | Propose writes to Raft; apply committed entries; signal waiting client goroutines |
| `follower.go`           | Apply committed entries on non-leader nodes                                       |
| `snapshot_manager.go`   | Periodic snapshots to disk; atomic two-phase write; crash recovery via load       |
| `semisync.go`           | Wait for at least one follower ACK; increment fallbackCount on timeout            |
| `lag.go`                | Track follower match indices; compute LagEntries and LagMs                        |
| `raftly_adapter.go`     | Bridge raftly's concrete types to the `RaftNode` interface                        |

**The TOCTOU race and its fix**: raftly's internal `commitCh` is buffered (capacity 256). This means `applyCommitted()` can send an entry to `commitCh` and call `notifyProposal` before `l.raft.Propose()` has returned to the caller. If `l.pending[reqID]` is registered _after_ `l.raft.Propose()` returns, `applyEntry` signals a missing pending op and the client times out. The fix: pre-register the pending op **before** calling `l.raft.Propose()`. The pending map key is `fmt.Sprintf("%p", resultCh)` — the channel pointer is unique per proposal within the process lifetime without coordination.

**The mutex non-reentrancy trap**: Go's `sync.Mutex` is NOT reentrant. Calling `l.raft.Propose()` with `l.pendingMu` held causes `applyEntry` to block forever on `l.pendingMu.Lock()`. When the client's context eventually fires, `ctx.Done()` tries to re-acquire the same lock on the same goroutine — permanent deadlock, zero output. Always unlock before crossing a subsystem boundary.

**Log compaction**: `SnapshotManager` runs on every node independently. Because all nodes apply the same log entries in the same order (Raft guarantee), snapshotting at index N on any node produces semantically equivalent state. No cross-node coordination needed. Snapshots use a temp-file → rename two-phase write so a crash mid-write leaves the previous snapshot intact. On startup, `LoadLatestSnapshot()` restores the engine state before the Raft apply loop starts, ensuring entries are always applied on top of a consistent baseline.

### `consistency/` — Read routing

Four readers, one router. `Router.Get()` dispatches to the right reader based on `GetOptions.Consistency`. Each reader may fall back to the leader (`leaderRouter`) when the local node is behind the required version.

| Mode               | Guarantee               | Mechanism                                                            | Cost                                          |
| ------------------ | ----------------------- | -------------------------------------------------------------------- | --------------------------------------------- |
| `strong`           | Linearizable            | ReadIndex: heartbeat quorum, then local read                         | 1 Raft RTT per read                           |
| `eventual`         | Best-effort staleness   | Local engine read, tagged with `IsStale`/`LagMs`                     | Zero network overhead                         |
| `read-your-writes` | See your own writes     | Session token carries `writeVersion`; compare against `appliedIndex` | Zero on warm follower; 1 RTT on fallback      |
| `monotonic`        | Reads never go backward | Client-side watermark; compare against `appliedIndex`                | Zero on caught-up follower; 1 RTT on fallback |

**Session token**: RYW uses a base64-encoded JSON struct `{nodeID, writeVersion}`, not a JWT. It encodes the Raft log index of the last write the client made. A follower that has applied at least that index can serve the read locally; one that hasn't sends back `ErrNotCaughtUp` (HTTP 503), and the client falls back to the leader.

### `server/` — HTTP REST and gRPC APIs

The `server` package exposes two independent APIs on the same node:

**HTTP REST** (`server/server.go`, `server/api.go`)

| Endpoint                | Method | Description                                                        |
| ----------------------- | ------ | ------------------------------------------------------------------ |
| `/v1/keys/{key}`        | PUT    | Write a value; returns `session_token` for RYW reads               |
| `/v1/keys/{key}`        | GET    | Read with `X-Consistency` header (`strong`/`eventual`/`ryw`/`monotonic`) |
| `/v1/keys/{key}`        | DELETE | Delete a key; redirects to leader if called on a follower          |
| `/v1/keys`              | GET    | Range scan with `?start=&end=&limit=` query params                 |
| `/v1/status`            | GET    | Node state, commit index, applied index, per-follower replication lag |
| `/v1/debug/gc`          | GET    | GC stats: horizon, blocked-by-txn flag, cycles run                 |
| `/metrics` (separate port) | GET | Prometheus text format — separate listener, never mixed with KV traffic |

The `X-Consistency` middleware parses the header once and injects `ConsistencyMode` into the request context; handlers never parse headers directly. The `WithMetrics` middleware wraps every handler and records latency histograms, op counters, and stale read counts.

**gRPC** (`server/kv_server.go`, `proto/kv_fabric.proto`)

The `KVFabric` service is the client-facing API for `cmd/server` (the networked deployment). `kvctl` uses this API.

```protobuf
service KVFabric {
    rpc Put(PutRequest)       returns (PutResponse);
    rpc Get(GetRequest)       returns (GetResponse);
    rpc Delete(DeleteRequest) returns (DeleteResponse);
    rpc Scan(ScanRequest)     returns (ScanResponse);
    rpc Status(StatusRequest) returns (StatusResponse);
}
```

Non-leader nodes populate `redirect_to` in the response instead of returning a gRPC error. `kvctl` follows the redirect by prepending the leader address to its retry queue.

**Prometheus metrics** (`server/metrics.go`)

| Metric                              | Type      | Description                                            |
| ----------------------------------- | --------- | ------------------------------------------------------ |
| `kv_fabric_puts_total`              | counter   | PUT operations by consistency mode                     |
| `kv_fabric_gets_total`              | counter   | GET operations by consistency mode                     |
| `kv_fabric_stale_reads_total`       | counter   | Reads served from a lagging follower                   |
| `kv_fabric_session_token_rejects_total` | counter | RYW/monotonic reads rejected — node not caught up yet  |
| `kv_fabric_replication_lag_ms`      | gauge     | Per-follower estimated lag in milliseconds             |
| `kv_fabric_mvcc_versions_total`     | gauge     | Total MVCC versions in engine (bloat indicator)        |
| `kv_fabric_mvcc_gc_blocked`         | gauge     | 1 if GC is blocked by a long-running transaction       |
| `kv_fabric_op_duration_ms`          | histogram | Latency by op type and consistency mode                |
| `kv_fabric_semisync_fallback_total` | counter   | Semi-sync timeouts where write fell back to async      |
| `kv_fabric_phantom_writes_total`    | counter   | Writes acknowledged but lost after leader crash        |

### `cluster/` — In-process cluster wiring

`cluster.Start()` builds a 3-node cluster: temporary WAL directories, `MVCCEngine` per node, `RaftlyAdapter` per node, `LeaderReplicator`, `FollowerApplier`, and `SnapshotManager` per node, shared `NetworkProxy` and `ChaosInjector`. Both `LeaderReplicator.Propose()` and `FollowerApplier.applyLoop()` are always running on every node; `IsLeader()` gates which path is active.

`lagLoop()` polls each node's `adapter.AppliedIndex()` every 50 ms and pushes it into the shared `ReplicationTracker`. This compensates for raftly not exposing per-follower matchIndex in its public API.

### `scenarios/` — Documented failure modes

| Scenario             | What it demonstrates                                                                                         |
| -------------------- | ------------------------------------------------------------------------------------------------------------ |
| `phantom_durability` | Semi-sync fallback trap: write acknowledged by leader, falls back to async, leader crashes, write is gone    |
| `booking_com`        | Stale read leads to double-booking: follower 340 ms behind confirms available seats that were already taken  |
| `mvcc_bloat`         | Version accumulation: hot key at high write rate, pin blocking GC horizon, GC reclaims after unpin           |
| `dirty_read`         | Snapshot isolation: MVCC prevents a concurrent reader from seeing intermediate state of a two-write transfer |

### `benchmark/` — 80-run measurement harness

Five workloads × four consistency modes × four concurrency levels = 80 runs. Each run lasts 3 seconds. The cluster is started once and reused — restarting per run would add ~300 ms of election overhead, turning a 4-minute benchmark into a 40-minute one.

| Workload      | Write% | Read% | Scan% | What it shows                                                                           |
| ------------- | ------ | ----- | ----- | --------------------------------------------------------------------------------------- |
| `write-only`  | 100    | 0     | 0     | Raft pipeline saturation; all modes have identical throughput (mode only affects reads) |
| `read-heavy`  | 5      | 95    | 0     | ReadIndex tax: every strong read is a Raft proposal                                     |
| `mixed`       | 50     | 50    | 0     | RYW and monotonic catch-up retry overhead under write pressure                          |
| `scan-heavy`  | 10     | 10    | 80    | Scans are always eventual; strong and eventual converge                                 |
| `write-heavy` | 80     | 20    | 0     | High write pressure keeps followers behind; eventual shows highest stale rate           |

Worker goroutines maintain per-goroutine session state (`lastWriteVersion`, `watermark`), mirroring real client behavior. Servers are stateless; any node can serve any request.

---

## Failure scenarios in depth

### Phantom durability (`make scenario-phantom`)

**Setup**: semi-sync replication with a 10 ms timeout. The timeout is intentionally shorter than the 50 ms lagLoop polling interval, so the tracker almost never shows a follower ACK in time.

**The trap**: `SemiSyncReplicator.WaitForAck()` polls follower matchIndex every 5 ms. When the timeout fires, `fallbackCount` increments and the leader returns success to the client anyway. At this exact moment the write exists only on the leader's log. The scenario then calls `chaos.CrashNode(leaderID)`, waits for re-election, and reads the key from the new leader's engine. The key is gone. `phantomCount` increments. The client received a success response for a write that no longer exists anywhere.

**The observable signal**: `kv_fabric_semisync_fallback_total` and `kv_fabric_phantom_writes_total` Prometheus counters. If `fallbackCount > 0` when a leader dies, expect phantom writes in the post-mortem.

### Booking.com double-booking (`make scenario-booking`)

**Setup**: a `transport.ProxyRule` with `ActionDelay` and `delay_ms: 340` is applied to all AppendEntries messages destined for one follower. This puts that follower 340 ms behind the leader.

**Path A (the bug)**: two concurrent clients both read seat availability from the lagging follower. Both see `seats=1`. Both book. The second booking overwrites the first. Inventory goes to `seats=-1`. This is a stale read leading to a lost update.

**Fix A**: route availability reads to the leader with `ConsistencyStrong`. ConfirmLeadership ensures the read reflects all committed writes.

**Fix B**: use optimistic concurrency — `PutOptions{IfVersion: v}`. The second writer's `IfVersion` check fails because the first writer already incremented the version. The second writer gets a conflict error and retries.

### MVCC bloat (`make scenario-mvcc-bloat`)

Shows the three-phase lifecycle of MVCC memory pressure:

1. **Accumulate**: 500 writes to a hot key → 500 versions in memory; no GC yet
2. **Pin**: an analytics transaction calls `PinVersion(key, 300)`; the GC horizon is clamped to 300 even after the replication horizon advances to 490
3. **Unpin**: the analytics job finishes; the GC horizon is now free to advance to 490; GC collects 300–489

**Production alert thresholds**: `TotalVersions / TotalKeys > N` (version bloat ratio); `OldestPinnedVersion` older than N minutes (stuck transaction).

### Dirty read / snapshot isolation (`make scenario-dirty-read`)

Shows the same money transfer twice — once without snapshot isolation (reader sees intermediate state, $200 disappears) and once with `GetAtVersion` pinning the pre-transfer snapshot (reader always sees a consistent view, total is always $1500).

---

## Getting started

### Prerequisites

- Go 1.21+
- Docker (for `make docker-*`)
- GNU Make

### Build

```bash
make build
# Produces: bin/scenarios  bin/benchmark  bin/kvctl  bin/server
```

### Run all failure scenarios

```bash
make scenarios
# Equivalent: ./bin/scenarios all
# Runs: phantom → booking → mvcc-bloat → dirty-read
```

Run a single scenario:

```bash
make scenario-phantom
make scenario-booking
make scenario-mvcc-bloat
make scenario-dirty-read
```

### Run the benchmark

```bash
make bench
# 80 runs × 3s = ~4 minutes
# Prints a markdown table + summary to stdout
```

### Run tests

```bash
# Integration tests: real 3-node clusters, data race detector
make test

# Unit tests: store/, replication/, consistency/ in isolation
make test-unit
```

The integration tests start real in-process Raft clusters. Election + apply overhead adds ~300 ms per test; `-timeout 120s` gives each test room to breathe. `-race` is mandatory — this project is all goroutines and channels.

### Run a networked 3-node cluster (localhost)

```bash
make cluster-start
# Starts 3 nodes in the background:
#   node-1: raft=:7001  kv=:9001  log=/tmp/kv-fabric-node-1.log
#   node-2: raft=:7002  kv=:9002  log=/tmp/kv-fabric-node-2.log
#   node-3: raft=:7003  kv=:9003  log=/tmp/kv-fabric-node-3.log
# WAL + snapshots: data/node-{1,2,3}/

# Use kvctl against the live cluster
./bin/kvctl put mykey myvalue --nodes localhost:9001,localhost:9002,localhost:9003
./bin/kvctl get mykey --nodes localhost:9001,localhost:9002,localhost:9003
./bin/kvctl status --nodes localhost:9001,localhost:9002,localhost:9003

make cluster-stop   # send SIGTERM to all nodes
make cluster-clean  # stop + delete data/ directories
```

On startup each node loads its snapshot from disk (if one exists), then replays only the Raft log entries after the snapshot version. A node that restarts after accumulating 10 000+ entries will catch up in milliseconds rather than replaying the full log from index 0.

### Run a containerised cluster (Docker)

```bash
make docker-build          # build kv-fabric:latest

make docker-cluster-start  # bring up 3-node cluster (docker compose)
                           # exposes kv ports 9001-9003 on the host

make docker-cluster-stop   # stop containers, keep WAL volumes
make docker-cluster-clean  # stop containers, delete WAL volumes
```

### Docker (in-process scenarios / benchmark)

```bash
make docker-scenarios  # run all failure scenarios inside Docker
make docker-bench      # run the 80-run benchmark inside Docker
```

The Docker image is a two-stage build: `golang:1.21-alpine` builder (CGO_ENABLED=0, fully static binary), `alpine:3.20` runtime. The ENTRYPOINT is `./scenarios`; `make docker-scenarios` uses the default `CMD ["all"]`. `make docker-bench` overrides ENTRYPOINT to `./benchmark`.

---

## Benchmark

Run `make bench` to generate results on your hardware. The benchmark prints a markdown table at the end. Below is a guide to interpreting each column.

| Column     | Meaning                                                      |
| ---------- | ------------------------------------------------------------ |
| `Workload` | Operation mix (write%, read%, scan%)                         |
| `Mode`     | Consistency mode for reads                                   |
| `Conc`     | Number of concurrent goroutines                              |
| `ops/s`    | Total successful operations per second                       |
| `p50 ms`   | Median latency                                               |
| `p99 ms`   | 99th percentile latency                                      |
| `p999 ms`  | 99.9th percentile latency                                    |
| `stale%`   | Fraction of reads where `IsStale=true` (eventual reads only) |

**Key findings from the 80-run matrix:**

**Finding 1: Consistency mode does not affect write throughput.**
The `write-only` workload produces identical ops/s across all four modes at every concurrency level. Mode is a read-side contract; writes always go through `LeaderReplicator.Propose()` regardless of mode.

**Finding 2: The ReadIndex tax is real and measurable.**
In `read-heavy` workload, `strong` mode issues a Raft heartbeat for every read (ConfirmLeadership). This appears as roughly 1 Raft RTT of additional latency per read and a corresponding throughput reduction versus `eventual`. The RTT is in-process (InMemTransport), so on a real network with 1–5 ms RTT the gap would be larger.

**Finding 3: Monotonic and RYW read throughput converges with `strong` at high concurrency under write pressure.**
In `write-heavy` workload at high concurrency, followers are perpetually behind the leader. Both `monotonic` and `read-your-writes` modes fall back to the leader on every read (their watermarks and session tokens always exceed the follower's applied index). Effective throughput matches `strong`. This is the correct behavior — the modes degrade gracefully rather than serving stale data below the client's guaranteed version.

**Finding 4: Eventual reads in `write-heavy` have the highest stale rate and longest estimated lag.**
This is expected: the mode makes no catch-up guarantee. `stale%` and `LagMs` in the output are the observability surface — they let you measure how stale your eventual reads actually are in production before deciding whether to pay the linearizability tax.

---

## Design decisions

**Raft log index as MVCC version number.** The version number assigned to a write is `entry.Index` — the index of the committed Raft log entry. This eliminates the need for a separate version counter or a distributed ID generator. Because all nodes apply the same entry with the same index, all engines assign the same version. Version numbers are globally ordered, monotonically increasing, and free.

**`KVEngine` as an interface.** Every layer above storage (`replication`, `consistency`, HTTP, gRPC) depends on `KVEngine`, not on `MVCCEngine`. This makes unit tests possible: a test can inject a fake engine without starting a Raft cluster. The consistency package's unit tests never touch Raft.

**`pendingOp` keyed by `fmt.Sprintf("%p", resultCh)`.** The channel pointer is unique per proposal within the process lifetime. This avoids a coordination point for generating unique IDs, and it makes the key stable even if the Raft log index is not yet known at registration time (necessary for pre-registration before `l.raft.Propose()` returns).

**Both `LeaderReplicator` and `FollowerApplier` run on every node.** After a leader election, the new leader's `LeaderReplicator` starts accepting proposals immediately — no wiring change needed. `IsLeader()` gates the proposal path; the apply loop is always running. On a node that is not the leader, `LeaderReplicator.Propose()` returns a redirect error immediately.

**`lagLoop` instead of raftly `FollowerProgress`.** raftly's `FollowerProgress` map returns the leader's view of peer match indices, which is accurate but only meaningful on the leader. `lagLoop` polls every node's `adapter.AppliedIndex()` every 50 ms and pushes it into the shared `ReplicationTracker`. This gives every node — including followers — accurate lag data for serving `IsStale` and `LagMs` on eventual reads.

**In-process cluster for scenarios and benchmarks.** InMemTransport carries real Raft consensus with zero network overhead. ChaosInjector injects the same failure modes as real hardware by intercepting messages at the NetworkProxy layer. The scenarios produce real numbers because the apply path, MVCC engine, and GC are exercised exactly as in production — just without the network RTT variable.

**gRPC redirect, not server-to-server forwarding.** When a non-leader node receives a write or a strong read, it returns `redirect_to` (the leader's KV address) in the response body rather than proxying the request internally. This keeps the data path transparent: every hop is visible in client logs, latency is attributable to the correct node, and the leader's goroutine pool is not inflated by proxy connections. The client library handles retry transparently.

**Snapshot version = max(gcHorizon, highest live entry version).** The snapshot version determines where Raft log replay starts after a crash recovery. Using `gcHorizon` alone is wrong: it is only advanced by GC, which runs on a 10-minute ticker and may never fire in short-lived test runs. A node that crashes before GC fires would record version 0, then replay the full log from index 0 on restart — defeating the point. Using the highest version among live entries gives the correct recovery starting point without requiring any additional tracking in the engine.

**Two-phase atomic snapshot write.** `SnapshotManager.persist()` writes data to a temp file, then `os.Rename`s it to the final path. `os.Rename` is atomic on POSIX — a crash between write and rename leaves the previous snapshot intact. The meta file is written after the data file, so if you see a meta file on disk, the data file behind it is always complete.

---

## Dependencies

| Package                               | Version | Role                                                                     |
| ------------------------------------- | ------- | ------------------------------------------------------------------------ |
| `github.com/ani03sha/raftly`          | v0.2.0  | Raft consensus library (leader election, log replication, commit quorum) |
| `go.uber.org/zap`                     | v1.27.1 | Structured logging                                                       |
| `github.com/prometheus/client_golang` | v1.23.2 | Metrics: op counters, latency histograms, replication lag gauges         |
| `github.com/spf13/cobra`              | v1.10.2 | `kvctl` CLI                                                              |
| `google.golang.org/grpc`              | v1.80.0 | Client-facing KV API and inter-node Raft transport                       |
| `google.golang.org/protobuf`          | v1.36.11 | Protocol Buffers for `proto/kv_fabric.proto`                            |
