package main

import (
	"fmt"
	"os"

	"github.com/ani03sha/kv-fabric/benchmark"
	"go.uber.org/zap"
)

func main() {
	// Silence the raftly/zap noise during the benchmark — we care about throughput numbers,
	// not log lines. Switch to zap.NewDevelopment() if you need to debug a run.
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logger init: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	benchmark.RunBenchmark(logger)
}
