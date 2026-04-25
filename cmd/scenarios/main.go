package main

import (
	"fmt"
	"os"

	"github.com/ani03sha/kv-fabric/scenarios"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync() //nolint:errcheck

	target := "all"
	if len(os.Args) > 1 {
		target = os.Args[1]
	}

	switch target {
	case "phantom":
		scenarios.RunPhantomDurability(logger)
	case "booking":
		scenarios.RunBookingCom(logger)
	case "mvcc-bloat":
		scenarios.RunMVCCBloat(logger)
	case "dirty-read":
		scenarios.RunDirtyRead(logger)
	case "all":
		scenarios.RunPhantomDurability(logger)
		scenarios.RunBookingCom(logger)
		scenarios.RunMVCCBloat(logger)
		scenarios.RunDirtyRead(logger)
	default:
		fmt.Fprintf(os.Stderr, "unknown scenario %q\n", target)
		fmt.Fprintln(os.Stderr, "usage: scenarios [phantom|booking|mvcc-bloat|dirty-read|all]")
		os.Exit(1)
	}
}
