package store

import "time"

// This is declared here (not in consistency/ package) to avoid import cycles.
// The consistency/ package imports the store/, not the reverse.
type ConsistencyMode int

const (
	ConsistencyStrong         ConsistencyMode = iota // linearizable
	ConsistencyEventual                              // stale reads are OK
	ConsistencyReadYourWrites                        // session: see own writes
	ConsistencyMonotonic                             // session: reads never go backward
)

// This makes ConsistencyMode implement fmt.Stringer — useful in logs and metrics.
func (c ConsistencyMode) String() string {
	switch c {
	case ConsistencyStrong:
		return "strong"
	case ConsistencyEventual:
		return "eventual"
	case ConsistencyReadYourWrites:
		return "read-your-writes"
	case ConsistencyMonotonic:
		return "monotonic"
	default:
		return "unknown"
	}
}

type PutOptions struct {
	TTL         time.Duration // 0 = no expiry
	IfVersion   uint64        // 0 = unconditional, non-zero = CAS check
	Consistency ConsistencyMode
	// LogIndex: when non-zero, the replication layer sets this to the Raft log
	// index so all nodes assign the same version number to the same write.
	// When zero (standalone tests), the engine uses an internal counter.
	LogIndex uint64
}

// This is what a successful write returns to the caller.
type PutResult struct {
	Version   uint64 // Raft log index assigned to this write
	AppliedAt time.Time
}

// This carries all metadata a caller can attach to a read.
type GetOptions struct {
	Consistency  ConsistencyMode
	MinVersion   uint64 // monotonic reads: reject if node hasn't applied this version yet
	SessionToken string // read-your-writes: opaque token from the last Put
}

// This is what a read returns.
type GetResult struct {
	Value     []byte    // nil if key doesn't exist
	Version   uint64    // version number of the value returned
	WrittenAt time.Time // when the version was written

	// Observability — populated by the consistency layer, not the storage layer.
	FromNode string // which cluster node served this read
	IsStale  bool   // true if served from a follower that lags the leader
	LagMs    int64  // estimated milliseconds behind the leader at read time
}

type DeleteOptions struct {
	IfVersion   int64
	Consistency ConsistencyMode
}

// Single result entry from a Scan.
type KVPair struct {
	Key     string
	Value   []byte
	Version uint64
}

// This is a point-in-time health snapshot of the engine. TotalVersions is the key metric for detecting MVCC bloat.
type EngineStats struct {
	TotalKeys     int64
	TotalVersions int64  // grows unboundedly when GC is blocked
	OldestVersion uint64 // the lowest version number still alive
	GCCyclesTotal int64
	BytesTotal    int64
}

// Snapshot is an opaque, serializable point-in-time view of the engine. Used by Raft for log compaction: instead of
// replaying 1M log entries on a new node, we ship a snapshot of the current state and only replay the delta.
type Snapshot interface {
	Data() []byte    // serialized bytes suitable for Raft InstallSnapshot RPC
	Version() uint64 // the Raft log index this snapshot represents
}

// This is the core storage contract. Every layer above (replication, consistency, HTTP) depends on this interface,
// not on any concrete implementation.
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
