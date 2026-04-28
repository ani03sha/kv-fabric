package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/server"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/ani03sha/raftly/raft"
	"github.com/ani03sha/raftly/transport"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	nodeID := flag.String("id", "", "Node ID, e.g. node-1 (required)")
	raftAddr := flag.String("raft-addr", "", "Raft gRPC listen address, e.g. :7001 (required)")
	kvAddr := flag.String("kv-addr", "", "KV gRPC listen address, e.g. :9001 (required)")
	metricsAddr := flag.String("metrics-addr", "", "Prometheus /metrics listen address, e.g. :9101 (optional)")
	dataDir := flag.String("data-dir", "", "WAL directory (default: data/<id>)")
	raftPeers := flag.String("raft-peers", "", `Raft peer addresses: "node-2=:7002,node-3=:7003"`)
	kvPeers := flag.String("kv-peers", "", `KV peer addresses:   "node-2=:9002,node-3=:9003"`)
	flag.Parse()

	if *nodeID == "" || *raftAddr == "" || *kvAddr == "" {
		fmt.Fprintln(os.Stderr, "usage: server --id <id> --raft-addr <addr> --kv-addr <addr> [options]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	logger := zap.Must(zap.NewProduction())
	defer logger.Sync()

	dir := *dataDir
	if dir == "" {
		dir = "data/" + *nodeID
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		logger.Fatal("create data dir", zap.String("path", dir), zap.Error(err))
	}

	// ── Raft transport ────────────────────────────────────────────────────────
	//
	// GRPCTransport plays two roles simultaneously:
	//   server role: binds raftAddr; receives RequestVote / AppendEntries from peers
	//   client role: dials peers lazily on first RPC; caches connections
	//
	// NetworkProxy sits in front of all outbound RPCs. With no rules it is a
	// pass-through. Rules can be added at runtime — same API as ChaosInjector.
	raftPeerMap := parsePeers(*raftPeers)
	proxy := transport.NewNetworkProxy()
	xport := transport.NewGRPCTransport(*nodeID, *raftAddr, raftPeerMap, proxy)

	// Bind the port BEFORE starting the Raft node. Peers may have already
	// started and will attempt RequestVote RPCs immediately after election
	// timeout. If the port is not open yet, those RPCs fail and the cluster
	// takes an extra election cycle to converge.
	if err := xport.Start(); err != nil {
		logger.Fatal("start raft transport", zap.String("addr", *raftAddr), zap.Error(err))
	}

	// ── Raft node ─────────────────────────────────────────────────────────────
	cfg := raft.DefaultConfig(*nodeID)
	cfg.DataDir = dir
	for id, addr := range raftPeerMap {
		cfg.Peers = append(cfg.Peers, raft.PeerConfig{ID: id, Address: addr})
	}

	raftNode, err := raft.NewRaftNode(cfg, xport)
	if err != nil {
		logger.Fatal("create raft node", zap.Error(err))
	}

	// ── kv-fabric layer ───────────────────────────────────────────────────────
	adapter := replication.NewRaftlyAdapter(raftNode)
	engine := store.NewMVCCEngine(*nodeID, 10*time.Minute)

	// 10k-entry threshold: in a production workload at 10k writes/sec this fires once per second.
	// Tune upward for lower write rates. 10-minute wall-clock interval: safety net if writes are infrequent.
	snapMgr := replication.NewSnapshotManager(
		*nodeID,
		engine,
		dir,
		10_000,
		10*time.Minute,
		logger,
	)
	// Load snapshot BEFORE starting Raft. If the node is recovering after a crash,
	// the engine is pre-populated here. Raft then replays only the entries after
	// the snapshot version, not the full log from index 0.
	snapVersion, err := snapMgr.LoadLatestSnapshot()
	if err != nil {
		logger.Fatal("load snapshot", zap.Error(err))
	}
	if snapVersion > 0 {
		logger.Info("recovered from snapshot",
			zap.String("node", *nodeID),
			zap.Uint64("snapshot_version", snapVersion))
	}
	tracker := replication.NewReplicationTracker()

	// Pre-register all nodes in the tracker so GetLag and Stats have entries.
	// trackFollowersLoop calls UpdateFollower as FollowerProgress returns data.
	//
	// TODO: raftly v0.2.0 does not expose per-follower matchIndex through
	// FollowerProgress in the multi-process path. LagMs will read as 0 until
	// a future version adds this. IsStale is unaffected — it is computed from
	// appliedIndex vs commitIndex, not the tracker.
	tracker.Track(*nodeID)
	for id := range raftPeerMap {
		tracker.Track(id)
	}

	leaderRepl := replication.NewLeaderReplicator(*nodeID, engine, adapter, tracker, logger)
	leaderRepl.SetSnapshotManager(snapMgr)

	// ── Prometheus metrics ────────────────────────────────────────────────────
	//
	// NewMetrics registers gauges and histograms into the default Prometheus
	// registry. An observability goroutine samples the engine and tracker every
	// 5 seconds and updates those metrics. The HTTP server exposes /metrics at
	// --metrics-addr if the flag is set; otherwise metrics are collected but not
	// scraped (useful for embedding in tests).
	metrics := server.NewMetrics(nil)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			metrics.ObserveEngine(engine.Stats(), store.GCStats{})
			metrics.ObserveTracker(tracker)
		}
	}()
	var metricsSrv *http.Server
	if *metricsAddr != "" {
		metricsSrv = &http.Server{Addr: *metricsAddr, Handler: promhttp.Handler()}
		go func() {
			logger.Info("metrics server listening", zap.String("addr", *metricsAddr))
			if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.Error("metrics server error", zap.Error(err))
			}
		}()
	}

	// ── Start in dependency order ─────────────────────────────────────────────
	//
	// LeaderReplicator runs on every node, leader and follower alike.
	// On followers, Propose() returns a redirect immediately; applyEntry still
	// runs and keeps the local engine in sync with the Raft log.
	//
	// FollowerApplier is NOT started. Both it and LeaderReplicator consume the
	// same CommittedEntries channel. Running both simultaneously would race for
	// entries — each entry would be applied by whichever goroutine wins the
	// channel receive, not both.
	if err := raftNode.Start(); err != nil {
		logger.Fatal("start raft node", zap.Error(err))
	}
	leaderRepl.Start()
	snapMgr.Start()

	kvPeerMap := parsePeers(*kvPeers)
	kvServer := server.NewKVServer(*nodeID, adapter, leaderRepl, engine, tracker, kvPeerMap, logger)
	grpcSrv, err := kvServer.Start(*kvAddr)
	if err != nil {
		logger.Fatal("start kv server", zap.String("addr", *kvAddr), zap.Error(err))
	}

	logger.Info("node ready",
		zap.String("id", *nodeID),
		zap.String("raft-addr", *raftAddr),
		zap.String("kv-addr", *kvAddr),
		zap.String("data-dir", dir),
	)

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	//
	// Shutdown order is the reverse of startup:
	//   1. Stop accepting new client RPCs; drain in-flight ones
	//   2. Stop the apply loop (waits for the current applyEntry to finish)
	//   3. Stop Raft (closes CommittedEntries channel; no more entries delivered)
	//   4. Close transport connections
	//   5. Close storage
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down", zap.String("id", *nodeID))
	if metricsSrv != nil {
		metricsSrv.Close()
	}
	grpcSrv.GracefulStop()
	snapMgr.Stop()
	leaderRepl.Stop()
	raftNode.Stop()
	xport.Close()
	adapter.Close()
	engine.Close()
}

// parsePeers converts "node-2=:7002,node-3=:7003" into map[string]string.
// Both --raft-peers and --kv-peers use this format.
func parsePeers(s string) map[string]string {
	m := make(map[string]string)
	if s == "" {
		return m
	}
	for _, part := range strings.Split(s, ",") {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			m[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return m
}
