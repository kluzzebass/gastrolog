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
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	configfile "gastrolog/internal/config/file"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/ingester/chatterbox"
	ingesthttp "gastrolog/internal/ingester/http"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/repl"
	"gastrolog/internal/server"
)

func main() {
	configPath := flag.String("config", "config.json", "path to configuration file")
	pprofAddr := flag.String("pprof", "", "pprof HTTP server address (e.g. localhost:6060)")
	serverAddr := flag.String("server", ":8080", "Connect RPC server address (empty to disable)")
	replMode := flag.Bool("repl", false, "start interactive REPL after system is running")
	flag.Parse()

	// Create base logger. In REPL mode, use discard logger to avoid interfering
	// with readline. Otherwise use ComponentFilterHandler for dynamic log level control.
	var logger *slog.Logger
	if *replMode {
		logger = logging.Discard()
	} else {
		baseHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: slog.LevelDebug, // Allow all levels; filtering done by ComponentFilterHandler
		})
		filterHandler := logging.NewComponentFilterHandler(baseHandler, slog.LevelInfo)
		logger = slog.New(filterHandler)
	}

	if *pprofAddr != "" {
		go func() {
			logger.Info("pprof server listening", "addr", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
				logger.Error("pprof server error", "error", err)
			}
		}()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx, logger, *configPath, *serverAddr, *replMode); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, configPath, serverAddr string, replMode bool) error {
	// Load configuration.
	logger.Info("loading config", "path", configPath)
	cfgStore := configfile.NewStore(configPath)
	cfg, err := cfgStore.Load(ctx)
	if err != nil {
		return err
	}
	if cfg == nil {
		logger.Info("no config found, running with empty configuration")
	} else {
		logger.Info("loaded config",
			"ingesters", len(cfg.Ingesters),
			"stores", len(cfg.Stores))
	}

	// Create orchestrator.
	orch := orchestrator.New(orchestrator.Config{
		Logger: logger,
	})

	// Apply configuration with factories.
	if err := orch.ApplyConfig(cfg, buildFactories(logger)); err != nil {
		return err
	}

	// Rebuild any missing indexes from interrupted builds.
	logger.Info("checking for missing indexes")
	if err := orch.RebuildMissingIndexes(ctx); err != nil {
		return err
	}

	// Start the orchestrator.
	logger.Info("starting orchestrator")
	if err := orch.Start(ctx); err != nil {
		return err
	}
	logger.Info("orchestrator started")

	// Start Connect RPC server if address is provided.
	var srv *server.Server
	var serverWg sync.WaitGroup
	if serverAddr != "" {
		srv = server.New(orch, server.Config{Logger: logger})
		serverWg.Add(1)
		go func() {
			defer serverWg.Done()
			if err := srv.ServeTCP(serverAddr); err != nil {
				logger.Error("server error", "error", err)
			}
		}()
	}

	if replMode {
		// Run REPL in foreground using embedded gRPC client.
		// This uses an in-memory transport so the REPL talks gRPC
		// just like a remote client, but without network overhead.
		r := repl.New(repl.NewEmbeddedClient(orch))
		if err := r.Run(); err != nil && err != context.Canceled {
			logger.Error("repl error", "error", err)
		}
	} else {
		// Wait for shutdown signal.
		<-ctx.Done()
	}

	// Stop the server first.
	if srv != nil {
		logger.Info("stopping server")
		if err := srv.Stop(ctx); err != nil {
			logger.Error("server stop error", "error", err)
		}
		serverWg.Wait()
	}

	// Stop the orchestrator.
	logger.Info("shutting down orchestrator")
	if err := orch.Stop(); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

// buildFactories creates the factory maps for all supported component types.
// The logger is passed to component factories for structured logging.
func buildFactories(logger *slog.Logger) orchestrator.Factories {
	return orchestrator.Factories{
		Ingesters: map[string]orchestrator.IngesterFactory{
			"chatterbox": chatterbox.NewIngester,
			"http":       ingesthttp.NewFactory(),
			"syslog":     ingestsyslog.NewFactory(),
		},
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file":   chunkfile.NewFactory(),
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file":   indexfile.NewFactory(),
			"memory": indexmem.NewFactory(),
		},
		Logger: logger,
	}
}
