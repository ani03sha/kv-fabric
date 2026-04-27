package client

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/ani03sha/kv-fabric/store"
	"github.com/spf13/cobra"
)

// --- Global flags ---
var (
	flagNodes       string // comma-separated node addresses
	flagConsistency string // default consistency mode
)

// Top-level kvctl command.
var rootCmd = &cobra.Command{
	Use:   "kvctl",
	Short: "kvctl — command-line client for kv-fabric",
	Long: `kvctl is the command-line interface for kv-fabric, a distributed
  key-value store with tunable consistency. Use --consistency to choose
  between strong, eventual, read-your-writes, and monotonic modes.`,
}

// Execute runs the CLI. Called from cmd/kvctl/main.go.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// Builds a Client from the global --nodes flag.
func newClient() *Client {
	addrs := strings.Split(flagNodes, ",")
	for i, a := range addrs {
		addrs[i] = strings.TrimSpace(a)
	}

	mode := store.ConsistencyStrong
	switch flagConsistency {
	case "eventual":
		mode = store.ConsistencyEventual
	case "ryw", "read-your-writes":
		mode = store.ConsistencyReadYourWrites
	case "monotonic":
		mode = store.ConsistencyMonotonic
	}

	return NewClient(Config{
		Addrs:              addrs,
		DefaultConsistency: mode,
		Timeout:            30 * time.Second,
	})
}

func init() {
	// Persistent flags are inherited by all subcommands.
	rootCmd.PersistentFlags().StringVar(&flagNodes,
		"nodes",
		"localhost:9001,localhost:9002,localhost:9003",
		"Comma-separated list of node gRPC addresses",
	)
	rootCmd.PersistentFlags().StringVar(&flagConsistency,
		"consistency", "strong",
		"Default consistency mode: strong|eventual|ryw|monotonic",
	)
}

// --- put ---

var (
	putConsistency string
	putIfVersion   uint64
)

var putCmd = &cobra.Command{
	Use:   "put <key> <value>",
	Short: "Write a key-value pair",
	Example: `  kvctl put user:1 alice
    kvctl put user:1 alice --consistency=strong
    kvctl put counter 100 --if-version=7`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		key, value := args[0], args[1]

		c := newClient()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		opts := PutOptions{
			Consistency: parseConsistency(putConsistency),
			IfVersion:   putIfVersion,
		}

		result, err := c.Put(ctx, key, []byte(value), opts)
		if err != nil {
			return err
		}

		fmt.Printf("version:       %d\n", result.Version)
		if result.SessionToken != "" {
			fmt.Printf("session_token: %s\n", result.SessionToken)
		}
		return nil
	},
}

func init() {
	putCmd.Flags().StringVar(&putConsistency, "consistency", "",
		"Override consistency mode for this write")
	putCmd.Flags().Uint64Var(&putIfVersion, "if-version", 0,
		"Conditional write: only apply if current version matches")
	rootCmd.AddCommand(putCmd)
}

// --- get ---

var (
	getConsistency  string
	getSessionToken string
	getMinVersion   uint64
)

var getCmd = &cobra.Command{
	Use:   "get <key>",
	Short: "Read a key's value",
	Example: `  kvctl get user:1
    kvctl get user:1 --consistency=eventual
    kvctl get user:1 --consistency=ryw --session-token=eyJ...
    kvctl get user:1 --consistency=monotonic --min-version=42`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		key := args[0]

		c := newClient()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		opts := GetOptions{
			Consistency:  parseConsistency(getConsistency),
			SessionToken: getSessionToken,
			MinVersion:   getMinVersion,
			MaxRetries:   3,
		}

		result, err := c.Get(ctx, key, opts)
		if err != nil {
			return err
		}

		if result.Value == nil {
			fmt.Println("(key not found)")
			return nil
		}

		fmt.Printf("value:    %s\n", string(result.Value))
		fmt.Printf("version:  %d\n", result.Version)
		fmt.Printf("from:     %s\n", result.FromNode)
		fmt.Printf("stale:    %v\n", result.IsStale)
		if result.LagMs > 0 {
			fmt.Printf("lag_ms:   %d\n", result.LagMs)
		}
		return nil
	},
}

func init() {
	getCmd.Flags().StringVar(&getConsistency, "consistency", "",
		"Override consistency mode for this read")
	getCmd.Flags().StringVar(&getSessionToken, "session-token", "",
		"Session token from a previous put (for read-your-writes)")
	getCmd.Flags().Uint64Var(&getMinVersion, "min-version", 0,
		"Minimum version watermark (for monotonic reads)")
	rootCmd.AddCommand(getCmd)
}

// --- delete ---

var deleteCmd = &cobra.Command{
	Use:     "delete <key>",
	Aliases: []string{"del", "rm"},
	Short:   "Delete a key",
	Args:    cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		if err := c.Delete(ctx, args[0]); err != nil {
			return err
		}
		fmt.Printf("deleted: %s\n", args[0])
		return nil
	},
}

func init() {
	rootCmd.AddCommand(deleteCmd)
}

// --- scan ---

var scanLimit int

var scanCmd = &cobra.Command{
	Use:   "scan <start> <end>",
	Short: "Scan a range of keys",
	Example: `  kvctl scan user: user;   # all keys starting with "user:"
    kvctl scan "" "" --limit=50`,
	Args: cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		pairs, err := c.Scan(ctx, args[0], args[1], scanLimit)
		if err != nil {
			return err
		}

		if len(pairs) == 0 {
			fmt.Println("(no keys in range)")
			return nil
		}

		for _, p := range pairs {
			fmt.Printf("%-40s  v%-6d  %s\n", p.Key, p.Version, string(p.Value))
		}
		return nil
	},
}

func init() {
	scanCmd.Flags().IntVar(&scanLimit, "limit", 100, "Maximum results to return")
	rootCmd.AddCommand(scanCmd)
}

// --- status ---

var statusNode string

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	RunE: func(cmd *cobra.Command, args []string) error {
		c := newClient()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		addrs := strings.Split(flagNodes, ",")
		if statusNode != "" {
			addrs = []string{strings.TrimSpace(statusNode)}
		}

		for _, addr := range addrs {
			addr = strings.TrimSpace(addr)
			s, err := c.Status(ctx, addr)
			if err != nil {
				fmt.Printf("%-30s  error: %v\n", addr, err)
				continue
			}
			role := "follower"
			if s.IsLeader {
				role = "leader"
			}
			fmt.Printf("node:          %s (%s)\n", s.NodeID, role)
			fmt.Printf("leader:        %s\n", s.LeaderID)
			fmt.Printf("commit_index:  %d\n", s.CommitIndex)
			fmt.Printf("applied_index: %d\n", s.AppliedIndex)
			if len(s.Followers) > 0 {
				fmt.Println("followers:")
				for _, f := range s.Followers {
					fmt.Printf("  %-10s  match=%-6d  lag=%dms\n",
						f.NodeID, f.MatchIndex, f.LagMs)
				}
			}
			fmt.Println()
		}
		return nil
	},
}

func init() {
	statusCmd.Flags().StringVar(&statusNode, "node", "",
		"Query a specific node address (default: all nodes)")
	rootCmd.AddCommand(statusCmd)
}

// --- bench ---

var benchCmd = &cobra.Command{
	Use:   "bench",
	Short: "Run the benchmark harness (implemented in Phase 11)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("bench: not yet implemented — available after Phase 11")
	},
}

func init() {
	rootCmd.AddCommand(benchCmd)
}

// --- scenario ---

var scenarioCmd = &cobra.Command{
	Use:   "scenario <name>",
	Short: "Run a failure scenario (implemented in Phase 10)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		return fmt.Errorf("scenario %q: not yet implemented — available after Phase 10", args[0])
	},
}

func init() {
	rootCmd.AddCommand(scenarioCmd)
}

// --- helpers ---

func parseConsistency(s string) store.ConsistencyMode {
	switch strings.ToLower(s) {
	case "eventual":
		return store.ConsistencyEventual
	case "ryw", "read-your-writes":
		return store.ConsistencyReadYourWrites
	case "monotonic":
		return store.ConsistencyMonotonic
	case "strong", "":
		return store.ConsistencyStrong
	default:
		return store.ConsistencyStrong
	}
}
