package benchmark

// This is the kind of work one benchmark iteration performs
type OpType int

const (
	OpWrite OpType = iota // Unconditional Put via Raft.Propose()
	OpRead                // Get with the run's consistency mode
	OpScan                // Scan a key range - always local, eventual semantics
)

// This struct defines the operation mix for one benchmark workload. WritePct + ReadPct + ScanPct must sum to 100.
type WorkloadSpec struct {
	Name     string
	WritePct int
	ReadPct  int
	ScanPct  int
}

// AllWorkloads are the five workloads in the 80-run matrix.
//
//	WriteOnly:  100% writes. Saturates the Raft log pipeline. All 4 modes have identical write
//	            throughput (mode only affects reads). Proves: consistency mode ≠ write cost.
//
//	ReadHeavy:  5/95 write/read. Typical OLTP shape. Strong mode funnels all reads through
//	            ConfirmLeadership — every read creates a Raft proposal. Eventual reads are local.
//	            The throughput delta between Strong and Eventual is the ReadIndex tax.
//
//	Mixed:      50/50. Forces the consistency layer to interleave with the write pipeline.
//	            RYW and Monotonic show their catch-up retry overhead here.
//
//	ScanHeavy:  10/10/80 write/read/scan. Scans are always eventual (no ReadIndex for multi-key
//	            ranges). Shows that Eventual and Strong converge when most ops are scans.
//
//	WriteHeavy: 80/20 write/read. High write pressure keeps followers perpetually behind.
//	            Eventual reads show the highest stale rate and longest lag in this workload.
var AllWorkloads = []WorkloadSpec{
	{Name: "write-only", WritePct: 100, ReadPct: 0, ScanPct: 0},
	{Name: "read-heavy", WritePct: 5, ReadPct: 95, ScanPct: 0},
	{Name: "mixed", WritePct: 50, ReadPct: 50, ScanPct: 0},
	{Name: "scan-heavy", WritePct: 10, ReadPct: 10, ScanPct: 80},
	{Name: "write-heavy", WritePct: 80, ReadPct: 20, ScanPct: 0},
}

func NextOp(spec WorkloadSpec, roll int) OpType {
	if roll < spec.WritePct {
		return OpWrite
	}
	if roll < spec.WritePct+spec.ReadPct {
		return OpRead
	}
	return OpScan
}
