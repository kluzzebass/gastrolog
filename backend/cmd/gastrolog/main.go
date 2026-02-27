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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	_ "net/http/pprof" //nolint:gosec // G108: pprof is intentionally available when --pprof flag is set
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"time"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"

	"gastrolog/cmd/gastrolog/cli"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	configmem "gastrolog/internal/config/memory"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/config/raftstore"
	"gastrolog/internal/home"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
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
	rootCmd.PersistentFlags().String("config-type", "raft", "config store type: raft or memory")
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

	rootCmd.AddCommand(serverCmd, versionCmd, cli.NewConfigCommand())

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

	// Resolve persistent node identity. For memory config (no home dir),
	// generate an ephemeral ID each run.
	var nodeID string
	if configType == "memory" {
		nodeID = uuid.Must(uuid.NewV7()).String()
	} else {
		nodeID, err = hd.NodeID()
		if err != nil {
			return fmt.Errorf("resolve node ID: %w", err)
		}
	}
	logger.Info("node identity", "node_id", nodeID)

	disp := &configDispatcher{localNodeID: nodeID, logger: logger.With("component", "dispatch")}
	cfgStore, err := openConfigStore(hd, configType, nodeID, logger, raftfsm.WithOnApply(disp.Handle))
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}
	if c, ok := cfgStore.(io.Closer); ok {
		defer func() { _ = c.Close() }()
	}

	// Ensure this node has a NodeConfig in the config store.
	// If not, generate a petname and persist it.
	if err := ensureNodeConfig(ctx, cfgStore, nodeID); err != nil {
		return fmt.Errorf("ensure node config: %w", err)
	}

	logger.Info("loading config", "type", configType)
	cfg, err := ensureConfig(ctx, logger, cfgStore, bootstrap)
	if err != nil {
		return err
	}

	homeDir := ""
	socketPath := ""
	if configType != "memory" {
		homeDir = hd.Root()
		socketPath = hd.SocketPath()
	}

	logger.Info("loaded config",
		"ingesters", len(cfg.Ingesters),
		"vaults", len(cfg.Vaults))

	orch := orchestrator.New(orchestrator.Config{
		Logger:            logger,
		MaxConcurrentJobs: loadMaxConcurrentJobs(ctx, cfgStore),
		ConfigLoader:      cfgStore,
		LocalNodeID:       nodeID,
	})
	orch.RegisterDigester(digestlevel.New())
	orch.RegisterDigester(digesttimestamp.New())

	factories := buildFactories(logger, homeDir, cfgStore, orch)

	// Wire the dispatcher now that orchestrator and factories are available.
	// From this point, FSM notifications will trigger orchestrator side effects.
	disp.orch = orch
	disp.cfgStore = cfgStore
	disp.factories = factories

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

	// For non-raft stores, the server must fire notifications itself since
	// there is no FSM callback. For raft stores the FSM handles it.
	var afterConfigApply func(raftfsm.Notification)
	if configType != "raft" {
		afterConfigApply = disp.Handle
	}

	return serveAndAwaitShutdown(ctx, logger, serverAddr, homeDir, nodeID, socketPath, orch, cfgStore, factories, tokens, certMgr, noAuth, afterConfigApply)
}

// ensureNodeConfig checks whether a NodeConfig exists for the local node ID.
// If not, it generates a petname and persists it to the config store.
func ensureNodeConfig(ctx context.Context, cfgStore config.Store, nodeID string) error {
	nodeUUID, err := uuid.Parse(nodeID)
	if err != nil {
		return fmt.Errorf("parse node ID %q: %w", nodeID, err)
	}
	existing, err := cfgStore.GetNode(ctx, nodeUUID)
	if err != nil {
		return fmt.Errorf("get node: %w", err)
	}
	if existing != nil {
		return nil
	}
	name := petname.Generate(2, "-")
	return cfgStore.PutNode(ctx, config.NodeConfig{ID: nodeUUID, Name: name})
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
	_, _, sched, _, _, _, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return 0
	}
	return sched.MaxConcurrentJobs
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
	_, _, _, tlsCfg, _, _, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("load server settings for TLS: %w", err)
	}
	if err := certMgr.LoadFromConfig(tlsCfg.DefaultCert, certs); err != nil {
		return nil, fmt.Errorf("load certs: %w", err)
	}
	return certMgr, nil
}

func serveAndAwaitShutdown(ctx context.Context, logger *slog.Logger, serverAddr, homeDir, nodeID, socketPath string, orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, tokens *auth.TokenService, certMgr *cert.Manager, noAuth bool, afterConfigApply func(raftfsm.Notification)) error {
	var srv *server.Server
	var serverWg sync.WaitGroup
	if serverAddr != "" {
		srv = server.New(orch, cfgStore, factories, tokens, server.Config{Logger: logger, CertManager: certMgr, NoAuth: noAuth, HomeDir: homeDir, NodeID: nodeID, UnixSocket: socketPath, AfterConfigApply: afterConfigApply})
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

// buildTokenService reads the server config from the config store and creates a TokenService.
func buildTokenService(ctx context.Context, cfgStore config.Store) (*auth.TokenService, error) {
	authCfg, _, _, _, _, _, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("load server settings: %w", err)
	}
	if authCfg.JWTSecret == "" {
		return nil, errors.New("server config not found (bootstrap may have failed)")
	}

	secret, err := base64.StdEncoding.DecodeString(authCfg.JWTSecret)
	if err != nil {
		return nil, fmt.Errorf("decode JWT secret: %w", err)
	}

	duration := 168 * time.Hour // default 7 days
	if authCfg.TokenDuration != "" {
		duration, err = time.ParseDuration(authCfg.TokenDuration)
		if err != nil {
			return nil, fmt.Errorf("parse token duration: %w", err)
		}
	}

	return auth.NewTokenService(secret, duration), nil
}

// openConfigStore creates a config.Store based on config type and home directory.
func openConfigStore(hd home.Dir, configType, nodeID string, logger *slog.Logger, fsmOpts ...raftfsm.Option) (config.Store, error) {
	switch configType {
	case "memory":
		return configmem.NewStore(), nil
	case "raft":
		return openRaftConfigStore(hd, nodeID, logger, fsmOpts...)
	default:
		return nil, fmt.Errorf("unknown config store type: %q", configType)
	}
}

// raftConfigStore wraps a raftstore.Store with cleanup logic for the
// underlying raft instance and boltdb store.
type raftConfigStore struct {
	config.Store
	raft    *hraft.Raft
	boltDB io.Closer
}

func (s *raftConfigStore) Close() error {
	future := s.raft.Shutdown()
	err := future.Error()
	if cerr := s.boltDB.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// openRaftConfigStore creates a single-node raft-backed config store with
// BoltDB persistence. On first run, the cluster is bootstrapped so this node
// becomes leader immediately.
func openRaftConfigStore(hd home.Dir, nodeID string, logger *slog.Logger, fsmOpts ...raftfsm.Option) (config.Store, error) {
	raftDir := hd.RaftDir()
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, fmt.Errorf("create raft directory: %w", err)
	}

	// BoltDB for both log store and stable store.
	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(raftDir, "raft.db"),
	})
	if err != nil {
		return nil, fmt.Errorf("open raft boltdb: %w", err)
	}

	snapStore, err := hraft.NewFileSnapshotStore(raftDir, 2, io.Discard)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	_, transport := hraft.NewInmemTransport("")

	fsm := raftfsm.New(fsmOpts...)

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.LogOutput = io.Discard
	// Single-node: tight timeouts for near-instant election.
	conf.HeartbeatTimeout = 100 * time.Millisecond
	conf.ElectionTimeout = 100 * time.Millisecond
	conf.LeaderLeaseTimeout = 100 * time.Millisecond

	r, err := hraft.NewRaft(conf, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	// Bootstrap on first run (no existing configuration).
	existing := r.GetConfiguration()
	if err := existing.Error(); err != nil {
		_ = r.Shutdown().Error()
		_ = boltStore.Close()
		return nil, fmt.Errorf("get raft configuration: %w", err)
	}
	if len(existing.Configuration().Servers) == 0 {
		boot := hraft.Configuration{
			Servers: []hraft.Server{
				{ID: hraft.ServerID(nodeID), Address: transport.LocalAddr()},
			},
		}
		if err := r.BootstrapCluster(boot).Error(); err != nil {
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return nil, fmt.Errorf("bootstrap raft: %w", err)
		}
	}

	// Wait for leadership.
	select {
	case <-r.LeaderCh():
	case <-time.After(5 * time.Second):
		_ = r.Shutdown().Error()
		_ = boltStore.Close()
		return nil, errors.New("timed out waiting for raft leadership")
	}

	logger.Info("raft config store ready", "dir", raftDir)

	return &raftConfigStore{
		Store:  raftstore.New(r, fsm, 10*time.Second),
		raft:   r,
		boltDB: boltStore,
	}, nil
}
