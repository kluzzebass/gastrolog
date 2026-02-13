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
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"time"

	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	configfile "gastrolog/internal/config/file"
	configmem "gastrolog/internal/config/memory"
	configsqlite "gastrolog/internal/config/sqlite"
	"gastrolog/internal/datadir"
	digestlevel "gastrolog/internal/digester/level"
	digesttimestamp "gastrolog/internal/digester/timestamp"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/ingester/chatterbox"
	ingesthttp "gastrolog/internal/ingester/http"
	ingestrelp "gastrolog/internal/ingester/relp"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	ingesttail "gastrolog/internal/ingester/tail"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/repl"
	"gastrolog/internal/server"
)

func main() {
	datadirFlag := flag.String("datadir", "", "data directory (default: platform config dir)")
	configType := flag.String("config-type", "sqlite", "config store type: sqlite, json, or memory")
	pprofAddr := flag.String("pprof", "", "pprof HTTP server address (e.g. localhost:6060)")
	serverAddr := flag.String("server", ":4564", "Connect RPC server address (empty to disable)")
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

	if err := run(ctx, logger, *datadirFlag, *configType, *serverAddr, *replMode); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, datadirFlag, configType, serverAddr string, replMode bool) error {
	// Resolve data directory.
	dd, err := resolveDataDir(datadirFlag)
	if err != nil {
		return fmt.Errorf("resolve data directory: %w", err)
	}

	// For non-memory config types, ensure the data directory exists.
	if configType != "memory" {
		if err := dd.EnsureExists(); err != nil {
			return err
		}
		logger.Info("data directory", "path", dd.Root())
	}

	// Open config store.
	cfgStore, err := openConfigStore(dd, configType)
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}
	if c, ok := cfgStore.(io.Closer); ok {
		defer c.Close()
	}

	// Load configuration.
	logger.Info("loading config", "type", configType)
	cfg, err := cfgStore.Load(ctx)
	if err != nil {
		return err
	}

	// Determine dataDir for bootstrap: persistent stores get the real dir,
	// memory gets empty string (in-memory default store).
	bootstrapDataDir := ""
	if configType != "memory" {
		bootstrapDataDir = dd.Root()
	}

	if cfg == nil {
		logger.Info("no config found, bootstrapping default configuration")
		if err := config.Bootstrap(ctx, cfgStore, bootstrapDataDir); err != nil {
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

	// Load persisted server config for scheduler settings.
	var maxConcurrentJobs int
	if raw, err := cfgStore.GetSetting(ctx, "server"); err == nil && raw != nil {
		var sc config.ServerConfig
		if err := json.Unmarshal([]byte(*raw), &sc); err == nil {
			maxConcurrentJobs = sc.Scheduler.MaxConcurrentJobs
		}
	}

	// Create orchestrator.
	orch := orchestrator.New(orchestrator.Config{
		Logger:            logger,
		MaxConcurrentJobs: maxConcurrentJobs,
	})

	// Register digesters (message enrichment pipeline).
	orch.RegisterDigester(digestlevel.New())
	orch.RegisterDigester(digesttimestamp.New())

	// Apply configuration with factories.
	factories := buildFactories(logger, bootstrapDataDir)
	if err := orch.ApplyConfig(cfg, factories); err != nil {
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

	// Create TokenService from server config for auth RPCs.
	tokens, err := buildTokenService(ctx, cfgStore)
	if err != nil {
		return fmt.Errorf("build token service: %w", err)
	}

	// Certificate manager: load certs from config store.
	certMgr := cert.New(cert.Config{Logger: logger})
	certNames, err := cfgStore.ListCertificates(ctx)
	if err != nil {
		return fmt.Errorf("list certificates: %w", err)
	}
	certs := make(map[string]cert.CertSource, len(certNames))
	for _, name := range certNames {
		pem, err := cfgStore.GetCertificate(ctx, name)
		if err != nil {
			return fmt.Errorf("get certificate %q: %w", name, err)
		}
		if pem != nil {
			certs[name] = cert.CertSource{CertPEM: pem.CertPEM, KeyPEM: pem.KeyPEM, CertFile: pem.CertFile, KeyFile: pem.KeyFile}
		}
	}
	sc, err := config.LoadServerConfig(ctx, cfgStore)
	if err != nil {
		return fmt.Errorf("load server config for TLS: %w", err)
	}
	if err := certMgr.LoadFromConfig(sc.TLS.DefaultCert, certs); err != nil {
		return fmt.Errorf("load certs: %w", err)
	}

	// Start Connect RPC server if address is provided.
	var srv *server.Server
	var serverWg sync.WaitGroup
	if serverAddr != "" {
		srv = server.New(orch, cfgStore, factories, tokens, server.Config{Logger: logger, CertManager: certMgr})
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
func buildFactories(logger *slog.Logger, dataDir string) orchestrator.Factories {
	return orchestrator.Factories{
		Ingesters: map[string]orchestrator.IngesterFactory{
			"chatterbox": chatterbox.NewIngester,
			"http":       ingesthttp.NewFactory(),
			"relp":       ingestrelp.NewFactory(),
			"syslog":     ingestsyslog.NewFactory(),
			"tail":       ingesttail.NewFactory(),
		},
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file":   chunkfile.NewFactory(),
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file":   indexfile.NewFactory(),
			"memory": indexmem.NewFactory(),
		},
		Logger:  logger,
		DataDir: dataDir,
	}
}

// resolveDataDir returns a Dir from the flag value, or the platform default.
func resolveDataDir(flagValue string) (datadir.Dir, error) {
	if flagValue != "" {
		return datadir.New(flagValue), nil
	}
	return datadir.Default()
}

// buildTokenService reads the server config from the store and creates a TokenService.
func buildTokenService(ctx context.Context, cfgStore config.Store) (*auth.TokenService, error) {
	val, err := cfgStore.GetSetting(ctx, "server")
	if err != nil {
		return nil, fmt.Errorf("get server setting: %w", err)
	}
	if val == nil {
		return nil, fmt.Errorf("server config not found (bootstrap may have failed)")
	}

	var serverCfg config.ServerConfig
	if err := json.Unmarshal([]byte(*val), &serverCfg); err != nil {
		return nil, fmt.Errorf("parse server config: %w", err)
	}

	secret, err := base64.StdEncoding.DecodeString(serverCfg.Auth.JWTSecret)
	if err != nil {
		return nil, fmt.Errorf("decode JWT secret: %w", err)
	}

	duration := 168 * time.Hour // default 7 days
	if serverCfg.Auth.TokenDuration != "" {
		duration, err = time.ParseDuration(serverCfg.Auth.TokenDuration)
		if err != nil {
			return nil, fmt.Errorf("parse token duration: %w", err)
		}
	}

	return auth.NewTokenService(secret, duration), nil
}

// openConfigStore creates a config.Store based on config type and data directory.
func openConfigStore(dd datadir.Dir, configType string) (config.Store, error) {
	switch configType {
	case "memory":
		return configmem.NewStore(), nil
	case "json":
		return configfile.NewStore(dd.ConfigPath("json"), dd.UsersPath()), nil
	case "sqlite":
		return configsqlite.NewStore(dd.ConfigPath("sqlite"))
	default:
		return nil, fmt.Errorf("unknown config store type: %q", configType)
	}
}
