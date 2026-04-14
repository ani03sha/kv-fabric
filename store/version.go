package store

import "time"

// Describes what kind of operation created a version.
type OpType int

const (
	OpPut    OpType = iota // a value was written
	OpDelete               // a tombstone - key was logically deleted
)

/*
Version is one immutable snapshot of a key's value at a point in Raft's history.

Once written, a version is never modified. This is what makes MVCC safe for concurrent access.
Multiple goroutines can hold a *Version pointer and read it freely. No lock needed.

The version chain for a key looks like this over time:
key "user:1" → [V1: "alice"] → [V5: "alice_v2"] → [V9: deleted] → [V12: "alice_v3"]

A read at version 6 returns "alice_v2". A read at version 10 returns nil (deleted). A read at
version 13 returns "alice_v3". The full history is always available until GC collects it.
*/
type Version struct {
	Num       uint64    // Raft log index - position in global write order
	Value     []byte    // raw bytes of the value; nil for tombstones
	Timestamp time.Time // wall clock when this was applied (informational only)
	TxnID     uint64    // transaction that created this version; 0 = non-transactional
	Deleted   bool      // true = this is a tombstone
}
