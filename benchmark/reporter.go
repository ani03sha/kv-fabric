package benchmark

import (
	"fmt"
	"sort"
	"time"
)

// Captures all measurements from one (workload, mode, concurrency) combination.
type RunResult struct {
	Workload    string
	Mode        string
	Concurrency int
	Duration    time.Duration

	TotalOps  int64
	OpsPerSec float64

	// Latency distribution in milliseconds.
	P50Ms  float64 // median — what most ops feel like
	P99Ms  float64 // 99th percentile — the tail users notice
	P999Ms float64 // 99.9th — worst-of-worst

	// Staleness observability (meaningful only for Eventual mode).
	StaleRate float64 // fraction of reads that returned stale data [0.0, 1.0]
	AvgLagMs  float64 // mean staleness lag across stale reads
}

// This function sorts raw nanosecond latency samples and returns p50/p99/p999 in milliseconds.
// Sorting is O(n log n) but done once per run, never in the hot path.
func ComputePercentiles(samplesNs []int64) (p50, p99, p999 float64) {
	if len(samplesNs) == 0 {
		return 0, 0, 0
	}

	sorted := make([]int64, len(samplesNs))
	copy(sorted, samplesNs)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	clamp := func(idx int) int {
		if idx >= len(sorted) {
			return len(sorted) - 1
		}
		return idx
	}

	toMs := func(ns int64) float64 { return float64(ns) / 1e6 }

	return toMs(sorted[clamp(int(float64(len(sorted))*0.50))]),
		toMs(sorted[clamp(int(float64(len(sorted))*0.99))]),
		toMs(sorted[clamp(int(float64(len(sorted))*0.999))])
}

// This function writes the full result table to stdout in GitHub-flavored markdown.
// Results are printed in the order they were appended (workload → mode → concurrency).
func PrintMarkdown(results []RunResult) {
	fmt.Println("## Benchmark Results")
	fmt.Println()
	fmt.Println("| Workload | Mode | Conc | Ops/s | P50 (ms) | P99 (ms) | P999 (ms) | Stale% | Avg Lag ms |")
	fmt.Println("|----------|------|-----:|------:|---------:|---------:|----------:|-------:|-----------:|")

	for _, r := range results {
		staleStr := fmt.Sprintf("%.1f%%", r.StaleRate*100)
		lagStr := fmt.Sprintf("%.1f", r.AvgLagMs)
		if r.StaleRate == 0 {
			staleStr = "0%"
			lagStr = "—"
		}
		fmt.Printf("| %-11s | %-8s | %4d | %6.0f | %8.2f | %8.2f | %9.2f | %6s | %10s |\n",
			r.Workload, r.Mode, r.Concurrency,
			r.OpsPerSec, r.P50Ms, r.P99Ms, r.P999Ms,
			staleStr, lagStr,
		)
	}
	fmt.Println()
}

// This function distills three headline insights from the full result set.
func PrintSummary(results []RunResult) {
	fmt.Println("## Key Findings")
	fmt.Println()

	// 1. Eventual vs Strong throughput on read-heavy workload at peak concurrency.
	var strongBest, eventualBest RunResult
	for _, r := range results {
		if r.Workload != "read-heavy" {
			continue
		}
		if r.Mode == "strong" && r.OpsPerSec > strongBest.OpsPerSec {
			strongBest = r
		}
		if r.Mode == "eventual" && r.OpsPerSec > eventualBest.OpsPerSec {
			eventualBest = r
		}
	}
	if strongBest.OpsPerSec > 0 && eventualBest.OpsPerSec > 0 {
		fmt.Printf("1. **Eventual vs Strong throughput (read-heavy, peak concurrency):** %.1fx faster\n",
			eventualBest.OpsPerSec/strongBest.OpsPerSec)
		fmt.Printf("   Strong:   %.0f ops/s  P99=%.2fms  (every read is a Raft proposal via ConfirmLeadership)\n",
			strongBest.OpsPerSec, strongBest.P99Ms)
		fmt.Printf("   Eventual: %.0f ops/s  P99=%.2fms  (local follower read, zero network)\n\n",
			eventualBest.OpsPerSec, eventualBest.P99Ms)
	}

	// 2. Peak stale rate (where eventual consistency actually hurts).
	var peakStale RunResult
	var totalLag, lagCount float64
	for _, r := range results {
		if r.Mode != "eventual" {
			continue
		}
		if r.StaleRate > peakStale.StaleRate {
			peakStale = r
		}
		if r.AvgLagMs > 0 {
			totalLag += r.AvgLagMs
			lagCount++
		}
	}
	if peakStale.StaleRate > 0 {
		fmt.Printf("2. **Peak stale read rate (eventual / %s / conc=%d):** %.1f%%\n",
			peakStale.Workload, peakStale.Concurrency, peakStale.StaleRate*100)
		if lagCount > 0 {
			fmt.Printf("   Avg staleness across all eventual runs: %.2fms\n\n", totalLag/lagCount)
		}
	} else {
		fmt.Println("2. **Stale rate:** 0% across all eventual runs")
		fmt.Println("   In-process cluster is fast enough that followers apply before reads arrive.")
	}

	// 3. Write throughput scaling with concurrency.
	var writeSlow, writeFast RunResult
	for _, r := range results {
		if r.Workload != "write-only" || r.Mode != "strong" {
			continue
		}
		if writeSlow.TotalOps == 0 || r.Concurrency < writeSlow.Concurrency {
			writeSlow = r
		}
		if r.Concurrency > writeFast.Concurrency {
			writeFast = r
		}
	}
	if writeSlow.OpsPerSec > 0 && writeFast.OpsPerSec > 0 {
		fmt.Printf("3. **Write pipeline scaling (write-only, conc %d→%d):** %.0f→%.0f ops/s\n",
			writeSlow.Concurrency, writeFast.Concurrency,
			writeSlow.OpsPerSec, writeFast.OpsPerSec)
		fmt.Println("   Raft serializes all writes through one leader pipeline.")
		fmt.Println("   Concurrency helps pipeline in-flight proposals. Diminishing returns beyond ~16.")
	}
}
