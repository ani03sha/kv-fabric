package server

import (
	"context"
	"net/http"
	"time"

	"github.com/ani03sha/kv-fabric/consistency"
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

// Config holds the runtime configuration for one cluster node's server.
type Config struct {
	NodeID      string
	HTTPAddr    string // e.g. 8001: serves the KV api
	MetricsAddr string // e.g. 9001: serves only /metrics for prometheus
}

// Server is the top-level wiring struct for one kv-fabric node. It owns the HTTP listener and the background
// observability loop. All business logic lives in the layers below it.
type Server struct {
	cfg     Config
	engine  store.KVEngine
	leader  *replication.LeaderReplicator
	tracker *replication.ReplicationTracker
	router  *consistency.Router
	metrics *Metrics
	logger  *zap.Logger

	apiServer     *http.Server
	metricsServer *http.Server
	stop          chan struct{}
}

func NewServer(
	cfg Config,
	engine store.KVEngine,
	raft replication.RaftNode,
	leader *replication.LeaderReplicator,
	tracker *replication.ReplicationTracker,
	logger *zap.Logger,
) *Server {
	metrics := NewMetrics(nil) // nil = prometheus.DefaultRegisterer

	router := consistency.NewRouter(cfg.NodeID, engine, raft, tracker, logger)
	router.RegisterRYW(consistency.NewRYWReader(cfg.NodeID, engine, raft, logger))
	router.RegisterMonotonic(consistency.NewMonotonicReader(cfg.NodeID, engine, raft, logger))

	s := &Server{
		cfg:     cfg,
		engine:  engine,
		leader:  leader,
		tracker: tracker,
		router:  router,
		metrics: metrics,
		logger:  logger,
		stop:    make(chan struct{}),
	}

	h := &handler{
		nodeID:  cfg.NodeID,
		engine:  engine,
		router:  router,
		leader:  leader,
		tracker: tracker,
		metrics: metrics,
		logger:  logger,
	}

	s.apiServer = &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      s.buildAPIHandler(h),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	s.metricsServer = &http.Server{
		Addr:    cfg.MetricsAddr,
		Handler: promhttp.Handler(), // exposes all registered Prometheus metrics
	}

	return s
}

// This function wires up routes and middleware for the KV API.
//
// Middleware chain (outermost first):
//
//	WithConsistencyMode → parse X-Consistency header into context
//	WithMetrics         → record latency and op counts
//	actual handler
func (s *Server) buildAPIHandler(h *handler) http.Handler {
	mux := http.NewServeMux()

	// Key operations
	mux.HandleFunc("PUT /v1/keys/{key}", h.putKey)
	mux.HandleFunc("GET /v1/keys/{key}", h.getKey)
	mux.HandleFunc("DELETE /v1/keys/{key}", h.deleteKey)

	// Scan — note: no {key} path param; uses query parameters instead
	mux.HandleFunc("GET /v1/keys", h.scanKeys)

	// Observability
	mux.HandleFunc("GET /v1/status", h.getStatus)
	mux.HandleFunc("GET /v1/debug/gc", h.getGCDebug)

	// Apply middleware.
	// Order matters: WithConsistencyMode MUST run before WithMetrics so
	// that the metrics middleware can read the consistency mode from context.
	var root http.Handler = mux
	root = WithMetrics(s.metrics, s.logger)(root)
	root = WithConsistencyMode(root)

	return root
}

// Begins listening on both the API port and the metrics port. Blocks until both servers fail or Stop() is called.
func (s *Server) Start() error {
	// Start the background metrics collection loop.
	go s.observabilityLoop()

	errCh := make(chan error, 2)

	go func() {
		s.logger.Info("API server listening",
			zap.String("node", s.cfg.NodeID),
			zap.String("addr", s.cfg.HTTPAddr),
		)
		if err := s.apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	go func() {
		s.logger.Info("metrics server listening",
			zap.String("node", s.cfg.NodeID),
			zap.String("addr", s.cfg.MetricsAddr),
		)
		if err := s.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	return <-errCh
}

// Stop gracefully shuts down both HTTP servers.
// Allows in-flight requests up to 15 seconds to complete.
func (s *Server) Stop(ctx context.Context) error {
	close(s.stop)

	shutdownCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := s.apiServer.Shutdown(shutdownCtx); err != nil {
		s.logger.Error("API server shutdown error", zap.Error(err))
	}
	if err := s.metricsServer.Shutdown(shutdownCtx); err != nil {
		s.logger.Error("metrics server shutdown error", zap.Error(err))
	}
	return nil
}

// Updates gauge metrics from live system state every 5 seconds.
// Counters and histograms are updated inline on each request.
// Gauges (replication lag, MVCC versions) require periodic polling.
func (s *Server) observabilityLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			engineStats := s.engine.Stats()
			var gcStats store.GCStats
			if eng, ok := s.engine.(*store.MVCCEngine); ok {
				gcStats = eng.GCStats()
			}
			s.metrics.ObserveEngine(engineStats, gcStats)
			s.metrics.ObserveTracker(s.tracker)

		case <-s.stop:
			return
		}
	}
}
