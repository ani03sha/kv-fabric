package server

import (
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics owns all Prometheus metrics for the server layer.
// Semi-sync metrics (fallback_total, phantom_writes_total) are owned by SemiSyncReplicator: they are NOT registered here
// to avoid duplicates.
//
// Call ObserveEngine and ObserveTracker periodically (every 5s) from a background goroutine to keep gauges up to date.
type Metrics struct {
	putsTotal           *prometheus.CounterVec
	getsTotal           *prometheus.CounterVec
	staleReadsTotal     prometheus.Counter
	sessionTokenRejects prometheus.Counter
	replicationLagMs    *prometheus.GaugeVec
	mvccVersionsTotal   prometheus.Gauge
	mvccGCBlocked       prometheus.Gauge
	opDurationMs        *prometheus.HistogramVec
}

// Creates and registers all server metrics. Pass prometheus.NewRegistry() in tests to avoid duplicate registration panics.
// Pass nil (or prometheus.DefaultRegisterer) for production.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}

	m := &Metrics{}

	m.putsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_fabric_puts_total",
		Help: "Total PUT operations, labelled by consistency mode.",
	}, []string{"consistency_mode"})

	m.getsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kv_fabric_gets_total",
		Help: "Total GET operations, labelled by consistency mode.",
	}, []string{"consistency_mode"})

	m.staleReadsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kv_fabric_stale_reads_total",
		Help: "GET operations that returned data from a lagging follower.",
	})

	m.sessionTokenRejects = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "kv_fabric_session_token_rejects_total",
		Help: "Reads rejected because the node hadn't applied the session's write version yet.",
	})

	m.replicationLagMs = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kv_fabric_replication_lag_ms",
		Help: "Estimated replication lag per follower in milliseconds.",
	}, []string{"follower_id"})

	m.mvccVersionsTotal = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kv_fabric_mvcc_versions_total",
		Help: "Total MVCC versions currently stored. Grows when GC is blocked by a long-running transaction.",
	})

	m.mvccGCBlocked = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kv_fabric_mvcc_gc_blocked",
		Help: "1 if GC is blocked by a long-running transaction, 0 otherwise.",
	})

	// Histogram buckets are tuned for a LAN cluster: sub-millisecond to multi-second.
	// The benchmark table p50/p95/p99 columns are computed from this histogram.
	m.opDurationMs = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "kv_fabric_op_duration_ms",
		Help:    "Operation latency in milliseconds, by op type and consistency mode.",
		Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500},
	}, []string{"op_type", "consistency_mode"})

	reg.MustRegister(
		m.putsTotal,
		m.getsTotal,
		m.staleReadsTotal,
		m.sessionTokenRejects,
		m.replicationLagMs,
		m.mvccVersionsTotal,
		m.mvccGCBlocked,
		m.opDurationMs,
	)

	return m
}

// RecordPut increments the put counter for the given consistency mode.
func (m *Metrics) RecordPut(mode string) {
	m.putsTotal.WithLabelValues(mode).Inc()
}

// RecordGet increments the get counter and optionally the stale read counter.
func (m *Metrics) RecordGet(mode string, isStale bool) {
	m.getsTotal.WithLabelValues(mode).Inc()
	if isStale {
		m.staleReadsTotal.Inc()
	}
}

// RecordOpDuration records a latency observation for the histogram.
func (m *Metrics) RecordOpDuration(opType, mode string, durationMs float64) {
	m.opDurationMs.WithLabelValues(opType, mode).Observe(durationMs)
}

// RecordSessionReject increments the session token rejection counter.
func (m *Metrics) RecordSessionReject() {
	m.sessionTokenRejects.Inc()
}

// ObserveEngine updates engine-level gauges from a stats snapshot.
// Call this every ~5s from a background goroutine.
func (m *Metrics) ObserveEngine(stats store.EngineStats, gcStats store.GCStats) {
	m.mvccVersionsTotal.Set(float64(stats.TotalVersions))
	if gcStats.BlockedByTxn {
		m.mvccGCBlocked.Set(1)
	} else {
		m.mvccGCBlocked.Set(0)
	}
}

// ObserveTracker updates per-follower replication lag gauges.
// Call this every ~5s from a background goroutine.
func (m *Metrics) ObserveTracker(tracker *replication.ReplicationTracker) {
	for _, state := range tracker.Stats() {
		m.replicationLagMs.WithLabelValues(state.NodeID).Set(float64(state.LagMs))
	}
}
