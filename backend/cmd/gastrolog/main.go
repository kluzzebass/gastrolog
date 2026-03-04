// Command gastrolog runs the log aggregation service.
//
// Logging:
//   - Base logger is created here with output format and level
//   - Logger is passed to all components via dependency injection
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

	serverCmd := &cobra.Command{
		Use:   "server",
		Short: "Start the gastrolog service",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := app.RunConfig{
				HomeFlag:    mustString(cmd, "home"),
				ConfigType:  mustString(cmd, "config-type"),
				ServerAddr:  mustString(cmd, "addr"),
				Bootstrap:   mustBool(cmd, "bootstrap"),
				NoAuth:      mustBool(cmd, "no-auth"),
				ClusterAddr: mustString(cmd, "cluster-addr"),
				ClusterInit: mustBool(cmd, "cluster-init"),
				JoinAddr:    mustString(cmd, "join-addr"),
				JoinToken:   mustString(cmd, "join-token"),
				Voteless:    mustBool(cmd, "voteless"),
				SlogCapture: slogCaptureCh,
			}

			err := app.Run(cmd.Context(), logger, cfg)
			if cmd.Context().Err() != nil {
				return nil //nolint:nilerr // signal-triggered shutdown is not an error
			}
			return err
		},
	}

	serverCmd.Flags().String("addr", ":4564", "listen address (host:port)")
	serverCmd.Flags().Bool("bootstrap", false, "bootstrap with default config (memory store + chatterbox)")
	serverCmd.Flags().Bool("no-auth", false, "disable authentication (all requests treated as admin)")
	serverCmd.Flags().String("cluster-addr", ":4566", "cluster gRPC listen address")
	serverCmd.Flags().Bool("cluster-init", false, "deprecated: raft servers auto-bootstrap on first start")
	serverCmd.Flags().String("join-addr", "", "leader's cluster address to join an existing cluster")
	serverCmd.Flags().String("join-token", "", "join token for cluster enrollment (from cluster-init node)")
	serverCmd.Flags().Bool("voteless", false, "join cluster as a nonvoter")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version)
		},
	}

	rootCmd.AddCommand(serverCmd, versionCmd, cli.NewConfigCommand())

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
