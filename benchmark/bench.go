package benchmark

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/ani03sha/kv-fabric/cluster"
	"github.com/ani03sha/kv-fabric/consistency"
	"github.com/ani03sha/kv-fabric/replication"
	"github.com/ani03sha/kv-fabric/store"
	"go.uber.org/zap"
)

const (
	runDuration   = 3 * time.Second // duration per run — 80 runs × 3s ≈ 4 minutes total
	numKeys       = 500             // pre-populated key space; hot enough for cache warmth
	valueSize     = 128             // bytes per value — representative KV payload
	scanRangeSize = 10              // keys returned per Scan op
)

var (
	concurrencies = []int{1, 4, 16, 64}
	modeNames     = []string{"strong", "eventual", "read-your-writes", "monotonic"}
)

// RunBenchmark runs the full 80-run matrix and prints a markdown table.
//
// Architecture: one 3-node cluster is started and reused across all runs.
// Starting and stopping a cluster per run would add ~300ms of election overhead each,
// turning a 4-minute benchmark into a 40-minute one.
//
// The key design decision in each worker goroutine is per-goroutine session state:
//
//	lastWriteVersion — updated after each write; passed as session token on RYW reads.
//	watermark        — updated after each read; passed as MinVersion on Monotonic reads.
//
// This mirrors real client behavior: session state lives in the client, not the server.
// Servers are stateless; any node can serve any request.
func RunBenchmark(logger *zap.Logger) {
	fmt.Println()
	fmt.Println("╔═══════════════════════════════════════════════════════════╗")
	fmt.Println("║                  BENCHMARK HARNESS                        ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════╝")
	fmt.Println()

	totalRuns := len(AllWorkloads) * len(modeNames) * len(concurrencies)
	fmt.Printf("Workloads: %d  |  Modes: %d  |  Concurrencies: %d  |  Total runs: %d\n", len(AllWorkloads), len(modeNames), len(concurrencies), totalRuns)
	fmt.Printf("Run duration: %v  |  Key space: %d  |  Value: %d bytes\n\n", runDuration, numKeys, valueSize)

	fmt.Println("Starting 3-node in-process Raft cluster...")
	c, err := cluster.Start(logger)
	if err != nil {
		fmt.Printf("[ERROR] cluster start: %v\n", err)
		return
	}
	defer c.Stop()
	fmt.Printf("Leader: %s\n\n", c.Leader().ID)

	if err := populate(c); err != nil {
		fmt.Printf("[ERROR] populate: %v\n", err)
		return
	}

	var results []RunResult

	for _, workload := range AllWorkloads {
		for _, modeName := range modeNames {
			for _, conc := range concurrencies {
				fmt.Printf("  %-12s | %-18s | conc=%-3d ... ", workload.Name, modeName, conc)

				result, err := runOnce(c, workload, modeName, conc, logger)
				if err != nil {
					fmt.Printf("ERROR: %v\n", err)
					continue
				}

				results = append(results, result)
				staleStr := fmt.Sprintf("%.1f%%", result.StaleRate*100)
				fmt.Printf("%.0f ops/s  p99=%.2fms  stale=%s\n", result.OpsPerSec, result.P99Ms, staleStr)
			}
		}
	}

	fmt.Println()
	PrintMarkdown(results)
	PrintSummary(results)
}

// This function populate pre-loads numKeys into the cluster so reads have real data to return.
// All keys are loaded before any timed run starts — we measure steady-state throughput, not cold-cache performance.
func populate(c *cluster.Cluster) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	value := make([]byte, valueSize)
	for i := range value {
		value[i] = 'x'
	}

	for i := 0; i < numKeys; i++ {
		_, err := c.Propose(ctx, replication.KVOperation{
			Type:  replication.OpPut,
			Key:   fmt.Sprintf("bench:key:%04d", i),
			Value: value,
		})
		if err != nil {
			return fmt.Errorf("key %d: %w", i, err)
		}
	}

	// Allow followers to apply the baseline writes before measuring.
	time.Sleep(200 * time.Millisecond)
	return nil
}

// This function executes one (workload, mode, concurrency) combination for runDuration.
//
// Reader routing:
//   - Strong reads:           leaderRouter  — ConfirmLeadership + waitForApplied + local read
//   - Eventual reads:         followerRouter — local follower read, tags IsStale/LagMs
//   - ReadYourWrites reads:   followerRouter — checks token.WriteVersion against appliedIndex fallback to leaderRouter if follower hasn't caught up yet
//   - Monotonic reads:        followerRouter — checks MinVersion (watermark) against appliedIndex fallback to leaderRouter if follower is behind watermark
//
// All writes go through c.Propose() which finds the current leader via cluster.Leader().
func runOnce(
	c *cluster.Cluster,
	workload WorkloadSpec,
	modeName string,
	concurrency int,
	logger *zap.Logger,
) (RunResult, error) {
	leader := c.Leader()
	if leader == nil {
		return RunResult{}, fmt.Errorf("no leader")
	}
	followers := c.Followers()
	if len(followers) == 0 {
		return RunResult{}, fmt.Errorf("no followers")
	}
	follower := followers[0]

	// Build routers. leaderRouter serves Strong reads. followerRouter serves the other three modes.
	// We build these once per run; if leadership changes mid-run, strong reads will return
	// "not leader" errors and be excluded from latency stats (correct behavior).
	leaderRouter := consistency.NewRouter(leader.ID, leader.Engine, leader.Adapter, c.Tracker, logger)

	followerRouter := consistency.NewRouter(follower.ID, follower.Engine, follower.Adapter, c.Tracker, logger)
	followerRouter.RegisterRYW(consistency.NewRYWReader(follower.ID, follower.Engine, follower.Adapter, logger))
	followerRouter.RegisterMonotonic(consistency.NewMonotonicReader(follower.ID, follower.Engine, follower.Adapter, logger))

	// Shared result collectors. latenciesNs is protected by mu (goroutines append locally, then flush to shared slice on exit).
	// StaleTracker uses atomics (no lock needed).
	var (
		mu          sync.Mutex
		latenciesNs []int64
		totalOps    int64
	)
	staleTracker := &StaleTracker{}

	stop := make(chan struct{})

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(uint64(workerID)*31337, uint64(time.Now().UnixNano())))

			// Per-goroutine session state — mirrors real client behavior.
			var lastWriteVersion uint64 // for RYW session token
			var watermark uint64        // for Monotonic MinVersion

			// Per-goroutine value buffer — distinct per worker so writes are identifiable.
			value := make([]byte, valueSize)
			for j := range value {
				value[j] = byte('a' + workerID%26)
			}

			// Accumulate locally; flush to shared state on exit to avoid lock contention
			// in the hot path.
			var localSamples []int64
			var localOps int64

			for {
				select {
				case <-stop:
					mu.Lock()
					latenciesNs = append(latenciesNs, localSamples...)
					totalOps += localOps
					mu.Unlock()
					return
				default:
				}

				keyIdx := rng.IntN(numKeys)
				key := fmt.Sprintf("bench:key:%04d", keyIdx)
				opType := NextOp(workload, rng.IntN(100))

				start := time.Now()
				var opErr error

				switch opType {
				case OpWrite:
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					res, err := c.Propose(ctx, replication.KVOperation{
						Type:  replication.OpPut,
						Key:   key,
						Value: value,
					})
					cancel()
					opErr = err
					if err == nil {
						lastWriteVersion = res.Version
						if res.Version > watermark {
							watermark = res.Version
						}
					}

				case OpRead:
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					var result *store.GetResult

					switch modeName {
					case "strong":
						// ConfirmLeadership sends a heartbeat to quorum before serving the local read.
						// Every strong read is a real Raft proposal (nil data, filtered by the bridge).
						// This is the cost of linearizability — visible in P99 latency.
						result, opErr = leaderRouter.Get(ctx, key, store.GetOptions{
							Consistency: store.ConsistencyStrong,
						})

					case "eventual":
						// Local read from follower's engine. Zero network overhead.
						// IsStale=true if the follower's applied index is behind the leader's commit.
						result, opErr = followerRouter.Get(ctx, key, store.GetOptions{
							Consistency: store.ConsistencyEventual,
						})

					case "read-your-writes":
						// Pass the session token from our last write. The follower checks:
						// does my appliedIndex >= token.WriteVersion? If not → 503 → fall back to leader.
						// The fallback is infrequent but shows up in the tail latency.
						opts := store.GetOptions{Consistency: store.ConsistencyReadYourWrites}
						if lastWriteVersion > 0 {
							if token, err := consistency.IssueToken(leader.ID, lastWriteVersion); err == nil {
								opts.SessionToken = token
							}
						}
						result, opErr = followerRouter.Get(ctx, key, opts)
						var notCaughtUp *consistency.ErrNotCaughtUp
						if errors.As(opErr, &notCaughtUp) {
							// Follower is behind our last write. Fall back to the leader.
							result, opErr = leaderRouter.Get(ctx, key, store.GetOptions{
								Consistency: store.ConsistencyStrong,
							})
						}

					case "monotonic":
						// Pass our current watermark. The follower checks: appliedIndex >= watermark?
						// If not → 503 → fall back to leader (adds one leader-RTT to the tail).
						result, opErr = followerRouter.Get(ctx, key, store.GetOptions{
							Consistency: store.ConsistencyMonotonic,
							MinVersion:  watermark,
						})
						var notCaughtUp *consistency.ErrNotCaughtUp
						if errors.As(opErr, &notCaughtUp) {
							result, opErr = leaderRouter.Get(ctx, key, store.GetOptions{
								Consistency: store.ConsistencyStrong,
							})
						}
					}

					cancel()

					if result != nil {
						staleTracker.Record(result.IsStale, result.LagMs)
						if result.Version > watermark {
							watermark = result.Version
						}
					}

				case OpScan:
					// Scans go directly to the follower's engine — always eventual semantics.
					// There is no ReadIndex protocol for multi-key ranges in this implementation.
					startKey := fmt.Sprintf("bench:key:%04d", keyIdx)
					endKey := fmt.Sprintf("bench:key:%04d", keyIdx+scanRangeSize)
					_, opErr = follower.Engine.Scan(startKey, endKey, scanRangeSize)
				}

				if opErr == nil {
					localSamples = append(localSamples, time.Since(start).Nanoseconds())
					localOps++
				}
				// Failed ops (timeouts, redirects) are excluded from latency stats.
				// They represent real overhead but shouldn't distort the distribution.
			}

		}(i)
	}

	time.Sleep(runDuration)
	close(stop)
	wg.Wait()

	if totalOps == 0 {
		return RunResult{}, fmt.Errorf("zero ops completed — cluster may be overloaded")
	}

	p50, p99, p999 := ComputePercentiles(latenciesNs)

	return RunResult{
		Workload:    workload.Name,
		Mode:        modeName,
		Concurrency: concurrency,
		Duration:    runDuration,
		TotalOps:    totalOps,
		OpsPerSec:   float64(totalOps) / runDuration.Seconds(),
		P50Ms:       p50,
		P99Ms:       p99,
		P999Ms:      p999,
		StaleRate:   staleTracker.StaleRate(),
		AvgLagMs:    staleTracker.AvgLagMs(),
	}, nil
}
