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

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	configfile "gastrolog/internal/config/file"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/receiver/chatterbox"
	"gastrolog/internal/source"
	sourcefile "gastrolog/internal/source/file"
)

func main() {
	configPath := flag.String("config", "config.json", "path to configuration file")
	sourcesPath := flag.String("sources", "sources.db", "path to sources registry file")
	pprofAddr := flag.String("pprof", "", "pprof HTTP server address (e.g. localhost:6060)")
	flag.Parse()

	// Create base logger with ComponentFilterHandler for dynamic log level control.
	baseHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Allow all levels; filtering done by ComponentFilterHandler
	})
	filterHandler := logging.NewComponentFilterHandler(baseHandler, slog.LevelInfo)
	logger := slog.New(filterHandler)

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

	if err := run(ctx, logger, *configPath, *sourcesPath); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, configPath, sourcesPath string) error {
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
			"receivers", len(cfg.Receivers),
			"stores", len(cfg.Stores),
			"routes", len(cfg.Routes))
	}

	// Create source registry.
	sourceStore := sourcefile.NewStore(sourcesPath)
	sources, err := source.NewRegistry(source.Config{
		Store:  sourceStore,
		Logger: logger,
	})
	if err != nil {
		return err
	}
	defer sources.Close()

	// Create orchestrator.
	orch := orchestrator.New(orchestrator.Config{
		Sources: sources,
		Logger:  logger,
	})

	// Apply configuration with factories.
	if err := orch.ApplyConfig(cfg, buildFactories(logger)); err != nil {
		return err
	}

	// Start the orchestrator.
	logger.Info("starting orchestrator")
	if err := orch.Start(ctx); err != nil {
		return err
	}
	logger.Info("orchestrator started, waiting for shutdown signal")

	// Wait for shutdown signal.
	<-ctx.Done()

	// Stop the orchestrator.
	logger.Info("shutting down")
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
		Receivers: map[string]orchestrator.ReceiverFactory{
			"chatterbox": chatterbox.NewReceiver,
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
