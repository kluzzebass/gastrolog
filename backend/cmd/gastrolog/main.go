// Command gastrolog runs the log aggregation service.
//
// Logging:
//   - Base logger writes to stderr so stdout stays free for CLI subcommands
//     that pipe machine-readable output (config -o json, query, export, …).
//   - Logger is passed into the server via app.Run (not used by thin CLI cmds).
//   - No global slog configuration (no slog.SetDefault)
//   - Components scope loggers with their own attributes
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // G108: pprof is intentionally available when --pprof flag is set
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"gastrolog/cmd/gastrolog/cli"
	"gastrolog/internal/app"
	"gastrolog/internal/logging"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/profiling"

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	// Create base logger with ComponentFilterHandler for dynamic log level control.
	baseHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Allow all levels; filtering done by ComponentFilterHandler
	})
	filterHandler := logging.NewComponentFilterHandler(baseHandler, slog.LevelInfo)

	// Parse and install --log-level spec at boot, before the cluster's
	// config store is available. The watcher in app.Run will replace
	// this rule set once the FSM has loaded its persisted config; the
	// boot-time spec is what runs from process start until that point
	// (and persists if no cluster-level rules are ever set).
	//
	// Done before the signal-handling defer so a parse error exits
	// cleanly without skipping deferred cleanup.
	if spec := getFlagFromArgs(os.Args[1:], "log-level"); spec != "" {
		rs, err := logging.ParseRuleSetSpec(spec)
		if err != nil {
			fmt.Fprintf(os.Stderr, "gastrolog: --log-level: %v\n", err)
			os.Exit(2)
		}
		filterHandler.SetRuleSet(rs)
	}

	// Register signal handler early, before any framework code.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Install capture handler for the "self" ingester. Records from
	// pipeline-internal components are skipped to prevent feedback loops.
	slogCaptureCh := make(chan logging.CapturedRecord, 4096)
	captureHandler := logging.NewCaptureHandler(filterHandler, slogCaptureCh, []string{
		"ingester", "orchestrator", "digest", "chunk", "index", "scheduler",
		"record-forwarder", "broadcast", "dispatch",
	})
	logger := slog.New(captureHandler)

	app.Version = version

	rootCmd := &cobra.Command{
		Use:   "gastrolog",
		Short: "Log aggregation service",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			pprofAddr, _ := cmd.Flags().GetString("pprof")
			pprofDebug, _ := cmd.Flags().GetBool("pprof-debug")
			mutexFraction, _ := cmd.Flags().GetInt("pprof-mutex-fraction")
			blockRate, _ := cmd.Flags().GetInt("pprof-block-rate")

			if pprofDebug {
				debug := profiling.DebugConfig()
				if !cmd.Flags().Changed("pprof-mutex-fraction") {
					mutexFraction = debug.MutexFraction
				}
				if !cmd.Flags().Changed("pprof-block-rate") {
					blockRate = debug.BlockRate
				}
			}
			profiling.Setup(logger, profiling.Config{
				MutexFraction: mutexFraction,
				BlockRate:     blockRate,
			})
			if pprofDebug {
				multiraft.EnableLeaseTrace(logger, true)
			}

			if pprofAddr != "" {
				go func() {
					logger.Info("pprof server listening", "addr", pprofAddr)
					pprofSrv := &http.Server{Addr: pprofAddr, Handler: nil, ReadHeaderTimeout: 10 * time.Second}
					if err := pprofSrv.ListenAndServe(); err != nil {
						logger.Error("pprof server error", "error", err)
					}
				}()
			}
			return nil
		},
	}

	rootCmd.PersistentFlags().String("home", "", "home directory (default: platform config dir)")
	rootCmd.PersistentFlags().String("config-type", "raft", "config store type: raft or memory")
	rootCmd.PersistentFlags().String("pprof", "", "pprof HTTP server address (e.g. localhost:6060); exposes /debug/pprof/{profile,trace,heap,goroutine,mutex,block}")
	rootCmd.PersistentFlags().Bool("pprof-debug", false, "enable mutex (1/5) and block (10ms) sampling for pprof; dev/incident use only")
	rootCmd.PersistentFlags().Int("pprof-mutex-fraction", 0, "mutex contention sample rate for /debug/pprof/mutex (0=off, 1=all, 5=one in five)")
	rootCmd.PersistentFlags().Int("pprof-block-rate", 0, "block profile sample period in nanoseconds for /debug/pprof/block (0=off, 1=all events, 10000000≈one per 10ms blocked)")
	rootCmd.PersistentFlags().String("log-level", "", "boot-time log levels, comma-separated (e.g. \"default=info,orchestrator.**=debug\"). Once the cluster config store has rules, those take precedence. Patterns follow gitignore-style globs (* = one segment, ** = any depth).")
	cli.AddClientFlags(rootCmd)

	serverCmd := &cobra.Command{
		Use:   "server",
		Short: "Start the gastrolog service",
		RunE: func(cmd *cobra.Command, args []string) error {
			raftHeartbeat, err := resolveDurationFlag(cmd, "raft-heartbeat-timeout", "GLOG_RAFT_HEARTBEAT_TIMEOUT")
			if err != nil {
				return err
			}
			raftLease, err := resolveDurationFlag(cmd, "raft-leader-lease", "GLOG_RAFT_LEADER_LEASE")
			if err != nil {
				return err
			}
			cfg := app.RunConfig{
				HomeFlag:         mustString(cmd, "home"),
				VaultsFlag:       mustString(cmd, "vaults"),
				ConfigType:       mustString(cmd, "config-type"),
				ServerAddr:       mustString(cmd, "listen"),
				NoAuth:           mustBool(cmd, "no-auth"),
				ClusterAddr:      mustString(cmd, "cluster-addr"),
				ClusterAdvertise: mustString(cmd, "cluster-advertise"),
				ServicePoolMaxPerPeer: func() int {
					v, _ := cmd.Flags().GetInt("service-pool-max-per-peer")
					return v
				}(),
				JoinAddr:  mustString(cmd, "join-addr"),
				JoinToken: mustString(cmd, "join-token"),
				NodeName:  mustString(cmd, "name"),
				PprofAddr: mustString(cmd, "pprof"),

				WriteBootstrapToken:       mustString(cmd, "write-bootstrap-token"),
				BootstrapTokenFile:        mustString(cmd, "bootstrap-token-file"),
				BootstrapTokenServeSecret: mustString(cmd, "bootstrap-token-serve-secret"),
				BootstrapTokenURL:         mustString(cmd, "bootstrap-token-url"),
				BootstrapTokenSecret:      mustString(cmd, "bootstrap-token-secret"),

				InitialAdminFile:     mustString(cmd, "initial-admin-file"),
				InitialAdminUser:     mustString(cmd, "initial-admin-user"),
				InitialAdminPassword: mustString(cmd, "initial-admin-password"),

				EnvironmentLabel:    mustString(cmd, "environment-label"),
				EnvironmentColor:    mustString(cmd, "environment-color"),
				SegmentHotPathFsync: resolveSegmentHotPathFsync(cmd),

				RaftHeartbeatTimeout: raftHeartbeat,
				RaftLeaderLease:      raftLease,

				SlogCapture:        slogCaptureCh,
				SlogCaptureHandler: captureHandler,
				LogFilter:          filterHandler,
			}

			err = app.Run(cmd.Context(), logger, cfg)
			if cmd.Context().Err() != nil {
				return nil //nolint:nilerr // signal-triggered shutdown is not an error
			}
			return err
		},
	}

	serverCmd.Flags().String("listen", ":4564", "listen address (host:port)")
	serverCmd.Flags().String("vaults", "", "vault storage directory (default: <home>/vaults)")
	serverCmd.Flags().Bool("no-auth", false, "disable authentication (all requests treated as admin)")
	serverCmd.Flags().String("cluster-addr", ":4566", "cluster gRPC listen address")
	serverCmd.Flags().String("cluster-advertise", "", "address peers store and dial to reach this node (empty = use bind address); set to a stable DNS name in environments with rotating pod IPs (e.g. Kubernetes)")
	serverCmd.Flags().Int("service-pool-max-per-peer", 0, "max parallel outbound service-lane gRPC connections per peer (0 = default 4)")
	serverCmd.Flags().String("join-addr", "", "leader's cluster address to join an existing cluster")
	serverCmd.Flags().String("join-token", "", "join token for cluster enrollment (from cluster-init node)")
	serverCmd.Flags().String("name", "", "node name (default: random petname)")

	// Non-interactive cluster bootstrap.
	serverCmd.Flags().String("write-bootstrap-token", "", "bootstrap node only: atomically write the join token to this path (mode 0600) for joiners to read via --bootstrap-token-file")
	serverCmd.Flags().String("bootstrap-token-file", "", "joiner only: read the join token from this path, polling with backoff until present (alternative to --join-token)")
	serverCmd.Flags().String("bootstrap-token-serve-secret", "", "bootstrap node only: serve the join token at GET /cluster/bootstrap-token, gated on this secret (empty disables endpoint)")
	serverCmd.Flags().String("bootstrap-token-url", "", "joiner only: fetch the join token from this URL, polling with backoff (alternative to --join-token); pair with --bootstrap-token-secret")
	serverCmd.Flags().String("bootstrap-token-secret", "", "joiner only: secret sent in the X-Bootstrap-Token-Secret header when fetching from --bootstrap-token-url")

	// Initial admin provisioning. Bootstrap node only; no-op once any user
	// exists.
	serverCmd.Flags().String("initial-admin-file", "", "bootstrap node only: read initial admin credentials from this file (JSON {\"username\":..., \"password\":...} or \"username:password\" line)")
	serverCmd.Flags().String("initial-admin-user", "", "bootstrap node only: initial admin username (paired with --initial-admin-password); ignored if --initial-admin-file is set")
	serverCmd.Flags().String("initial-admin-password", "", "bootstrap node only: initial admin password (paired with --initial-admin-user); ignored if --initial-admin-file is set")

	// Environment banner. Displayed in the UI header so operators can tell
	// at a glance which deployment they are looking at. Both are
	// display-only metadata; empty label hides the banner entirely.
	serverCmd.Flags().String("environment-label", "", "label rendered in the UI header banner (e.g. \"Kubernetes\", \"Development\"); empty hides the banner")
	serverCmd.Flags().String("environment-color", "", "CSS color for the UI header banner (e.g. \"red\", \"#c4302b\"); empty uses the palette default")
	serverCmd.Flags().Bool("segment-hot-path-fsync", true, "fsync segmentation group-commit flushes (default true); set false for load testing — also GLOG_SEGMENT_HOT_PATH_FSYNC")

	// Raft failure-detector timing. The operator lever for substrates whose
	// scheduler-stall tail exceeds the shipped window (e.g. a loaded macOS
	// dev host): widen both to tolerate longer pauses at the cost of slower
	// real-crash failover. Election timeout tracks the heartbeat timeout;
	// vault-ctl groups add 1s slack; transport RPC deadlines derive so they
	// can never undercut the detector.
	serverCmd.Flags().Duration("raft-heartbeat-timeout", 0, "base Raft heartbeat/election timeout for all groups (default 2s, vault-ctl +1s); 0 keeps the default — also GLOG_RAFT_HEARTBEAT_TIMEOUT")
	serverCmd.Flags().Duration("raft-leader-lease", 0, "Raft leader lease for all groups (default 1.5s, must not exceed the heartbeat timeout); 0 keeps the default — also GLOG_RAFT_LEADER_LEASE")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version)
		},
	}

	rootCmd.AddCommand(
		serverCmd,
		versionCmd,
		cli.NewConfigCommand(),
		cli.NewPrimeCommand(),
		cli.NewClusterCommand(),
		cli.NewAlertsCommand(),
		cli.NewJobCommand(),
		cli.NewUserCommand(),
		cli.NewLoginCommand(),
		cli.NewRegisterCommand(),
		cli.NewQueryCommand(),
		cli.NewInspectCommand(),
		cli.NewArchiveCommand(),
		cli.NewRestoreCommand(),
		cli.NewRepatriateCommand(),
		cli.NewSealCommand(),
		cli.NewReindexCommand(),
		cli.NewValidateCommand(),
		cli.NewPauseCommand(),
		cli.NewResumeCommand(),
	)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		if ctx.Err() != nil {
			stop()
			return // signal-triggered shutdown is not an error
		}
		stop()
		os.Exit(1) //nolint:gocritic // stop() called above; defer is a safety net
	}
}

func mustString(cmd *cobra.Command, name string) string {
	v, _ := cmd.Flags().GetString(name)
	return v
}

func mustBool(cmd *cobra.Command, name string) bool {
	v, _ := cmd.Flags().GetBool(name)
	return v
}

// resolveSegmentHotPathFsync returns whether segmentation group-commit flushes
// should fsync. The CLI flag wins when explicitly set; otherwise
// GLOG_SEGMENT_HOT_PATH_FSYNC is read. Default true (durable).
func resolveSegmentHotPathFsync(cmd *cobra.Command) bool {
	if cmd.Flags().Changed("segment-hot-path-fsync") {
		return mustBool(cmd, "segment-hot-path-fsync")
	}
	return envBoolDefaultTrue("GLOG_SEGMENT_HOT_PATH_FSYNC")
}

// resolveDurationFlag resolves a duration setting: the CLI flag wins when
// explicitly set; otherwise the environment variable is parsed. A malformed
// environment value fails the command instead of being silently ignored —
// these knobs exist for operators mid-incident, who need "GLOG_RAFT_...=5"
// (missing unit) to error loudly, not boot with the default.
func resolveDurationFlag(cmd *cobra.Command, flagName, envKey string) (time.Duration, error) {
	if cmd.Flags().Changed(flagName) {
		v, _ := cmd.Flags().GetDuration(flagName)
		return v, nil
	}
	raw := strings.TrimSpace(os.Getenv(envKey))
	if raw == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", envKey, err)
	}
	return d, nil
}

func envBoolDefaultTrue(key string) bool {
	v := os.Getenv(key)
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "true", "1", "yes", "y", "on":
		return true
	case "false", "0", "no", "n", "off":
		return false
	default:
		return true
	}
}

// getFlagFromArgs scans os.Args for "--<name>=<value>" or "--<name>
// <value>" and returns the value if present. Used at main() entry to
// read --log-level before cobra has parsed flags, so the boot-time
// rule set is installed onto the filter handler before any logging
// happens.
//
// Returns "" if the flag is not present or has no value.
func getFlagFromArgs(args []string, name string) string {
	prefix := "--" + name
	for i, a := range args {
		switch {
		case a == prefix && i+1 < len(args):
			return args[i+1]
		case strings.HasPrefix(a, prefix+"="):
			return a[len(prefix)+1:]
		}
	}
	return ""
}
