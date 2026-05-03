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

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	// Register signal handler early, before any framework code.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Create base logger with ComponentFilterHandler for dynamic log level control.
	baseHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Allow all levels; filtering done by ComponentFilterHandler
	})
	filterHandler := logging.NewComponentFilterHandler(baseHandler, slog.LevelInfo)

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
	rootCmd.PersistentFlags().String("pprof", "", "pprof HTTP server address (e.g. localhost:6060)")
	rootCmd.PersistentFlags().String("log-level", "", "per-component log levels (e.g. \"default=info,chunk=debug,replication=warn\"). Can be changed at runtime via the SetLogLevel RPC.")
	cli.AddClientFlags(rootCmd)

	serverCmd := &cobra.Command{
		Use:   "server",
		Short: "Start the gastrolog service",
		RunE: func(cmd *cobra.Command, args []string) error {
			if logLevels := mustString(cmd, "log-level"); logLevels != "" {
				if err := applyLogLevelSpec(filterHandler, logLevels); err != nil {
					return fmt.Errorf("--log-level: %w", err)
				}
			}
			cfg := app.RunConfig{
				HomeFlag:    mustString(cmd, "home"),
				VaultsFlag:  mustString(cmd, "vaults"),
				ConfigType:  mustString(cmd, "config-type"),
				ServerAddr:  mustString(cmd, "listen"),
				NoAuth:      mustBool(cmd, "no-auth"),
				ClusterAddr: mustString(cmd, "cluster-addr"),
				JoinAddr:    mustString(cmd, "join-addr"),
				JoinToken:   mustString(cmd, "join-token"),
				NodeName:    mustString(cmd, "name"),
				PprofAddr:   mustString(cmd, "pprof"),
				SlogCapture:        slogCaptureCh,
				SlogCaptureHandler: captureHandler,
				LogLevels:          filterHandler,
			}

			err := app.Run(cmd.Context(), logger, cfg)
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
	serverCmd.Flags().String("join-addr", "", "leader's cluster address to join an existing cluster")
	serverCmd.Flags().String("join-token", "", "join token for cluster enrollment (from cluster-init node)")
	serverCmd.Flags().String("name", "", "node name (default: random petname)")

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
		cli.NewJobCommand(),
		cli.NewUserCommand(),
		cli.NewLoginCommand(),
		cli.NewRegisterCommand(),
		cli.NewQueryCommand(),
		cli.NewInspectCommand(),
		cli.NewArchiveCommand(),
		cli.NewRestoreCommand(),
		cli.NewSealCommand(),
		cli.NewReindexCommand(),
		cli.NewPauseCommand(),
		cli.NewResumeCommand(),
	)

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		if ctx.Err() != nil {
			stop()
			return // signal-triggered shutdown is not an error
		}
		os.Exit(1) //nolint:gocritic // stop() is just signal cleanup; process is exiting
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

// applyLogLevelSpec parses comma-separated key=value pairs of the form
// "component=level" and applies them to the filter handler. The pseudo-key
// "default" sets the handler's fallback level for components without an
// explicit override. Unknown levels are rejected. See gastrolog-3flfp.
func applyLogLevelSpec(h *logging.ComponentFilterHandler, spec string) error {
	for pair := range strings.SplitSeq(spec, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		left, right, ok := strings.Cut(pair, "=")
		if !ok {
			return fmt.Errorf("expected component=level, got %q", pair)
		}
		component := strings.TrimSpace(left)
		level, err := parseLevelName(strings.TrimSpace(right))
		if err != nil {
			return fmt.Errorf("level for %q: %w", component, err)
		}
		if component == "default" {
			h.SetDefaultLevel(level)
			continue
		}
		h.SetLevel(component, level)
	}
	return nil
}

// parseLevelName accepts the standard slog level names (case-insensitive).
func parseLevelName(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown level %q (want debug, info, warn, or error)", s)
	}
}
