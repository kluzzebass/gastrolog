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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // G108: pprof is intentionally available when --pprof flag is set
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
	configmem "gastrolog/internal/config/memory"
	configsqlite "gastrolog/internal/config/sqlite"
	"gastrolog/internal/home"
	digestlevel "gastrolog/internal/digester/level"
	digesttimestamp "gastrolog/internal/digester/timestamp"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/ingester/chatterbox"
	ingestdocker "gastrolog/internal/ingester/docker"
	ingestfluentfwd "gastrolog/internal/ingester/fluentfwd"
	ingesthttp "gastrolog/internal/ingester/http"
	ingestkafka "gastrolog/internal/ingester/kafka"
	ingestotlp "gastrolog/internal/ingester/otlp"
	ingestrelp "gastrolog/internal/ingester/relp"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	ingestmetrics "gastrolog/internal/ingester/metrics"
	ingesttail "gastrolog/internal/ingester/tail"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	// Create base logger with ComponentFilterHandler for dynamic log level control.
	baseHandler := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug, // Allow all levels; filtering done by ComponentFilterHandler
	})
	filterHandler := logging.NewComponentFilterHandler(baseHandler, slog.LevelInfo)
	logger := slog.New(filterHandler)

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
	rootCmd.PersistentFlags().String("config-type", "sqlite", "config store type: sqlite or memory")
	rootCmd.PersistentFlags().String("pprof", "", "pprof HTTP server address (e.g. localhost:6060). WARNING: exposes CPU/memory profiles and goroutine dumps — bind to loopback only, never expose publicly")

	serverCmd := &cobra.Command{
		Use:   "server",
		Short: "Start the gastrolog service",
		RunE: func(cmd *cobra.Command, args []string) error {
			homeFlag, _ := cmd.Flags().GetString("home")
			configType, _ := cmd.Flags().GetString("config-type")
			serverAddr, _ := cmd.Flags().GetString("addr")
			bootstrap, _ := cmd.Flags().GetBool("bootstrap")
			noAuth, _ := cmd.Flags().GetBool("no-auth")

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
			defer cancel()

			return run(ctx, logger, homeFlag, configType, serverAddr, bootstrap, noAuth)
		},
	}

	serverCmd.Flags().String("addr", ":4564", "listen address (host:port)")
	serverCmd.Flags().Bool("bootstrap", false, "bootstrap with default config (memory store + chatterbox)")
	serverCmd.Flags().Bool("no-auth", false, "disable authentication (all requests treated as admin)")

	versionCmd := &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(version)
		},
	}

	rootCmd.AddCommand(serverCmd, versionCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger, homeFlag, configType, serverAddr string, bootstrap, noAuth bool) error {
	hd, err := resolveHome(homeFlag)
	if err != nil {
		return fmt.Errorf("resolve home directory: %w", err)
	}

	if configType != "memory" {
		if err := hd.EnsureExists(); err != nil {
			return err
		}
		logger.Info("home directory", "path", hd.Root())
	}

	cfgStore, err := openConfigStore(hd, configType)
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}
	if c, ok := cfgStore.(io.Closer); ok {
		defer func() { _ = c.Close() }()
	}

	logger.Info("loading config", "type", configType)
	cfg, err := ensureConfig(ctx, logger, cfgStore, bootstrap)
	if err != nil {
		return err
	}

	homeDir := ""
	if configType != "memory" {
		homeDir = hd.Root()
	}

	logger.Info("loaded config",
		"ingesters", len(cfg.Ingesters),
		"stores", len(cfg.Stores))

	orch := orchestrator.New(orchestrator.Config{
		Logger:            logger,
		MaxConcurrentJobs: loadMaxConcurrentJobs(ctx, cfgStore),
		ConfigLoader:      cfgStore,
	})
	orch.RegisterDigester(digestlevel.New())
	orch.RegisterDigester(digesttimestamp.New())

	factories := buildFactories(logger, homeDir, cfgStore, orch)
	if err := orch.ApplyConfig(cfg, factories); err != nil {
		return err
	}

	logger.Info("checking for missing indexes")
	if err := orch.RebuildMissingIndexes(ctx); err != nil {
		return err
	}

	logger.Info("starting orchestrator")
	if err := orch.Start(ctx); err != nil {
		return err
	}
	logger.Info("orchestrator started")

	tokens, err := buildAuthTokens(ctx, logger, cfgStore, noAuth)
	if err != nil {
		return err
	}

	certMgr, err := loadCertManager(ctx, logger, cfgStore)
	if err != nil {
		return err
	}

	return serveAndAwaitShutdown(ctx, logger, serverAddr, homeDir, orch, cfgStore, factories, tokens, certMgr, noAuth)
}

func ensureConfig(ctx context.Context, logger *slog.Logger, cfgStore config.Store, bootstrap bool) (*config.Config, error) {
	cfg, err := cfgStore.Load(ctx)
	if err != nil {
		return nil, err
	}
	if cfg != nil {
		return cfg, nil
	}

	if bootstrap {
		logger.Info("no config found, bootstrapping default configuration")
		if err := config.Bootstrap(ctx, cfgStore); err != nil {
			return nil, fmt.Errorf("bootstrap config: %w", err)
		}
	} else {
		logger.Info("no config found, bootstrapping minimal configuration (auth only)")
		if err := config.BootstrapMinimal(ctx, cfgStore); err != nil {
			return nil, fmt.Errorf("bootstrap minimal config: %w", err)
		}
	}

	cfg, err = cfgStore.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load bootstrapped config: %w", err)
	}
	return cfg, nil
}

func loadMaxConcurrentJobs(ctx context.Context, cfgStore config.Store) int {
	raw, err := cfgStore.GetSetting(ctx, "server")
	if err != nil || raw == nil {
		return 0
	}
	var sc config.ServerConfig
	if err := json.Unmarshal([]byte(*raw), &sc); err != nil {
		return 0
	}
	return sc.Scheduler.MaxConcurrentJobs
}

func buildAuthTokens(ctx context.Context, logger *slog.Logger, cfgStore config.Store, noAuth bool) (*auth.TokenService, error) {
	if noAuth {
		logger.Info("authentication disabled (--no-auth)")
		return nil, nil
	}
	tokens, err := buildTokenService(ctx, cfgStore)
	if err != nil {
		return nil, fmt.Errorf("build token service: %w", err)
	}
	return tokens, nil
}

func loadCertManager(ctx context.Context, logger *slog.Logger, cfgStore config.Store) (*cert.Manager, error) {
	certMgr := cert.New(cert.Config{Logger: logger})
	certList, err := cfgStore.ListCertificates(ctx)
	if err != nil {
		return nil, fmt.Errorf("list certificates: %w", err)
	}
	certs := make(map[string]cert.CertSource, len(certList))
	for _, c := range certList {
		certs[c.ID.String()] = cert.CertSource{CertPEM: c.CertPEM, KeyPEM: c.KeyPEM, CertFile: c.CertFile, KeyFile: c.KeyFile}
	}
	sc, err := config.LoadServerConfig(ctx, cfgStore)
	if err != nil {
		return nil, fmt.Errorf("load server config for TLS: %w", err)
	}
	if err := certMgr.LoadFromConfig(sc.TLS.DefaultCert, certs); err != nil {
		return nil, fmt.Errorf("load certs: %w", err)
	}
	return certMgr, nil
}

func serveAndAwaitShutdown(ctx context.Context, logger *slog.Logger, serverAddr, homeDir string, orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, tokens *auth.TokenService, certMgr *cert.Manager, noAuth bool) error {
	var srv *server.Server
	var serverWg sync.WaitGroup
	if serverAddr != "" {
		srv = server.New(orch, cfgStore, factories, tokens, server.Config{Logger: logger, CertManager: certMgr, NoAuth: noAuth, HomeDir: homeDir})
		serverWg.Go(func() {
			if err := srv.ServeTCP(serverAddr); err != nil {
				logger.Error("server error", "error", err)
			}
		})
	}

	<-ctx.Done()

	if srv != nil {
		logger.Info("stopping server")
		if err := srv.Stop(ctx); err != nil {
			logger.Error("server stop error", "error", err)
		}
		serverWg.Wait()
	}

	logger.Info("shutting down orchestrator")
	if err := orch.Stop(); err != nil {
		return err
	}
	logger.Info("shutdown complete")
	return nil
}

// buildFactories creates the factory maps for all supported component types.
// The logger is passed to component factories for structured logging.
func buildFactories(logger *slog.Logger, homeDir string, cfgStore config.Store, orch *orchestrator.Orchestrator) orchestrator.Factories {
	return orchestrator.Factories{
		Ingesters: map[string]orchestrator.IngesterFactory{
			"chatterbox": chatterbox.NewIngester,
			"docker":     ingestdocker.NewFactory(cfgStore),
			"fluentfwd":  ingestfluentfwd.NewFactory(),
			"http":       ingesthttp.NewFactory(),
			"kafka":      ingestkafka.NewFactory(),
			"metrics":    ingestmetrics.NewFactory(orch),
			"otlp":       ingestotlp.NewFactory(),
			"relp":       ingestrelp.NewFactory(),
			"syslog":     ingestsyslog.NewFactory(),
			"tail":       ingesttail.NewFactory(),
		},
		IngesterDefaults: map[string]func() map[string]string{
			"chatterbox": chatterbox.ParamDefaults,
			"docker":     ingestdocker.ParamDefaults,
			"fluentfwd":  ingestfluentfwd.ParamDefaults,
			"http":       ingesthttp.ParamDefaults,
			"kafka":      ingestkafka.ParamDefaults,
			"metrics":    ingestmetrics.ParamDefaults,
			"otlp":       ingestotlp.ParamDefaults,
			"relp":       ingestrelp.ParamDefaults,
			"syslog":     ingestsyslog.ParamDefaults,
			"tail":       ingesttail.ParamDefaults,
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
		HomeDir: homeDir,
	}
}

// resolveHome returns a Dir from the flag value, or the platform default.
func resolveHome(flagValue string) (home.Dir, error) {
	if flagValue != "" {
		return home.New(flagValue), nil
	}
	return home.Default()
}

// buildTokenService reads the server config from the store and creates a TokenService.
func buildTokenService(ctx context.Context, cfgStore config.Store) (*auth.TokenService, error) {
	val, err := cfgStore.GetSetting(ctx, "server")
	if err != nil {
		return nil, fmt.Errorf("get server setting: %w", err)
	}
	if val == nil {
		return nil, errors.New("server config not found (bootstrap may have failed)")
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

// openConfigStore creates a config.Store based on config type and home directory.
func openConfigStore(hd home.Dir, configType string) (config.Store, error) {
	switch configType {
	case "memory":
		return configmem.NewStore(), nil
	case "sqlite":
		return configsqlite.NewStore(hd.ConfigPath("sqlite"))
	default:
		return nil, fmt.Errorf("unknown config store type: %q", configType)
	}
}
