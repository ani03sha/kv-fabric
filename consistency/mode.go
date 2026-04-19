package consistency

import (
	"context"
	"fmt"

	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

// This is the interface every consistency mode implements. The router holds one Reader per mode
// and dispatches Get calls to the right one.
// Why and interface here?
// Each mode has completely different logic: strong does network round-trips, eventual does a local
// read, RYW parses a session token.
// The interface lets the Router to be a pure dispatch table - no conditionals embedded in each reader,
// and each reader can be tested in isolation.
type Reader interface {
	Get(ctx context.Context, key string, opts store.GetOptions) (*store.GetResult, error)
}

// Dispatches reads to the appropriate consistency implementation based on the mode negotiated from the
// X-Consistency request header.
// Write path: all writes go through LeaderReplicator.Propose() regardless of consistency mode: they are always
// Raft-committed (strong). The consistency mode only controls how READS are served.
// Exception: SemiSync changes write acknowledgment behavior
type Router struct {
	nodeID    string
	strong    Reader
	eventual  Reader
	ryw       Reader
	monotonic Reader
	logger    *zap.Logger
}

func NewRouter(
	nodeID string,
	engine store.KVEngine,
	raft replication.RaftNode,
	tracker *replication.ReplicationTracker,
	logger *zap.Logger,
) *Router {
	return &Router{
		nodeID: nodeID,
		strong: &StrongReader{
			nodeID: nodeID,
			engine: engine,
			raft:   raft,
			logger: logger,
		},
		eventual: &EventualReader{
			nodeID:  nodeID,
			engine:  engine,
			raft:    raft,
			tracker: tracker,
			logger:  logger,
		},
		logger: logger,
	}
}

// Plugs in "Read Your Writes" reader
func (r *Router) RegisterRYW(reader Reader) {
	r.ryw = reader
}

// Plugs in Monotonic reader
func (r *Router) RegisterMonotonic(reader Reader) {
	r.monotonic = reader
}

// Get routes the read to the correct implementation. Unrecognized modes fall back to Strong which is the safest default.
// Defaulting to Eventual would be a dangerous surprise: callers would silently get stale data without opting in.
func (r *Router) Get(ctx context.Context, key string, opts store.GetOptions) (*store.GetResult, error) {
	switch opts.Consistency {
	case store.ConsistencyStrong:
		return r.strong.Get(ctx, key, opts)
	case store.ConsistencyEventual:
		return r.eventual.Get(ctx, key, opts)
	case store.ConsistencyReadYourWrites:
		if r.ryw == nil {
			return nil, fmt.Errorf("read-your-writes: not yet registered")
		}
		return r.ryw.Get(ctx, key, opts)
	case store.ConsistencyMonotonic:
		if r.monotonic == nil {
			return nil, fmt.Errorf("monotonic: not yet registered")
		}
		return r.monotonic.Get(ctx, key, opts)
	default:
		r.logger.Warn("unrecognized consistency mode, defaulting to strong",
			zap.String("node", r.nodeID),
			zap.Int("mode", int(opts.Consistency)),
		)
		return r.strong.Get(ctx, key, opts)
	}
}

func (r *Router) ParseConsistencyMode(header string) store.ConsistencyMode {
	switch header {
	case "strong", "":
		return store.ConsistencyStrong
	case "eventual":
		return store.ConsistencyEventual
	case "read-your-writes", "ryw":
		return store.ConsistencyReadYourWrites
	case "monotonic":
		return store.ConsistencyMonotonic
	default:
		return store.ConsistencyStrong
	}
}
