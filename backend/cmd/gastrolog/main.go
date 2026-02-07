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
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	configfile "gastrolog/internal/config/file"
	configmem "gastrolog/internal/config/memory"
	configsqlite "gastrolog/internal/config/sqlite"
	digestlevel "gastrolog/internal/digester/level"
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
	configFlag := flag.String("config", "", "config store (memory, json:path, or sqlite:path)")
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

	if err := run(ctx, logger, *configFlag, *serverAddr, *replMode); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, configFlagValue, serverAddr string, replMode bool) error {
	// Open config store.
	cfgStore, err := openConfigStore(configFlagValue)
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}
	if c, ok := cfgStore.(io.Closer); ok {
		defer c.Close()
	}

	// Load configuration.
	logger.Info("loading config", "store", configFlagValue)
	cfg, err := cfgStore.Load(ctx)
	if err != nil {
		return err
	}

	if cfg == nil {
		logger.Info("no config found, bootstrapping default configuration")
		if err := config.Bootstrap(ctx, cfgStore); err != nil {
			return fmt.Errorf("bootstrap config: %w", err)
		}
		cfg, err = cfgStore.Load(ctx)
		if err != nil {
			return fmt.Errorf("load bootstrapped config: %w", err)
		}
	}

	logger.Info("loaded config",
		"ingesters", len(cfg.Ingesters),
		"stores", len(cfg.Stores))

	// Create orchestrator.
	orch := orchestrator.New(orchestrator.Config{
		Logger: logger,
	})

	// Register digesters (message enrichment pipeline).
	orch.RegisterDigester(digestlevel.New())

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

// parseConfigFlag parses a config flag value into store type and path.
// Formats: "json:path", "sqlite:path", or bare path (inferred by extension).
func parseConfigFlag(value string) (storeType, path string, err error) {
	if i := strings.IndexByte(value, ':'); i > 0 {
		return value[:i], value[i+1:], nil
	}
	// Bare path: infer by extension.
	switch filepath.Ext(value) {
	case ".db", ".sqlite", ".sqlite3":
		return "sqlite", value, nil
	default:
		return "json", value, nil
	}
}

// openConfigStore creates a config.Store from a flag value.
// Empty string returns an in-memory store (no persistence).
func openConfigStore(flagValue string) (config.Store, error) {
	if flagValue == "" || flagValue == "memory" {
		return configmem.NewStore(), nil
	}
	storeType, path, err := parseConfigFlag(flagValue)
	if err != nil {
		return nil, err
	}
	switch storeType {
	case "json":
		return configfile.NewStore(path), nil
	case "sqlite":
		return configsqlite.NewStore(path)
	default:
		return nil, fmt.Errorf("unknown config store type: %q", storeType)
	}
}
