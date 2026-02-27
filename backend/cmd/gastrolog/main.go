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

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
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
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
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
			clusterAddr, _ := cmd.Flags().GetString("cluster-addr")
			clusterInit, _ := cmd.Flags().GetBool("cluster-init")
			joinAddr, _ := cmd.Flags().GetString("join-addr")
			joinToken, _ := cmd.Flags().GetString("join-token")

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
			defer cancel()

			return run(ctx, logger, homeFlag, configType, serverAddr, bootstrap, noAuth, clusterAddr, clusterInit, joinAddr, joinToken)
		},
	}

	serverCmd.Flags().String("addr", ":4564", "listen address (host:port)")
	serverCmd.Flags().Bool("bootstrap", false, "bootstrap with default config (memory store + chatterbox)")
	serverCmd.Flags().Bool("no-auth", false, "disable authentication (all requests treated as admin)")
	serverCmd.Flags().String("cluster-addr", "", "cluster gRPC listen address (e.g., :4565) for multi-node mode")
	serverCmd.Flags().Bool("cluster-init", false, "initialize a new cluster (generates CA, certs, and join token)")
	serverCmd.Flags().String("join-addr", "", "leader's cluster address to join an existing cluster")
	serverCmd.Flags().String("join-token", "", "join token for cluster enrollment (from cluster-init node)")

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

func run(ctx context.Context, logger *slog.Logger, homeFlag, configType, serverAddr string, bootstrap, noAuth bool, clusterAddr string, clusterInit bool, joinAddr, joinToken string) error { //nolint:gocognit,gocyclo // startup orchestration is inherently complex
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

	// Create cluster TLS holder. Populated during cluster-init or enrollment.
	var clusterTLS *cluster.ClusterTLS

	// Joining flow: enroll with the leader before creating the cluster server.
	// The enrollment returns TLS material that we load into the ClusterTLS holder
	// so the Raft transport and gRPC server use mTLS from the start.
	if joinAddr != "" && joinToken != "" && clusterAddr != "" && configType == "raft" {
		tokenSecret, caHash, err := tlsutil.ParseJoinToken(joinToken)
		if err != nil {
			return fmt.Errorf("parse join token: %w", err)
		}

		logger.Info("enrolling with cluster leader", "leader_addr", joinAddr)
		enrollCtx, enrollCancel := context.WithTimeout(ctx, 30*time.Second)
		result, err := cluster.Enroll(enrollCtx, joinAddr, tokenSecret, caHash, nodeID, clusterAddr)
		enrollCancel()
		if err != nil {
			return fmt.Errorf("cluster enrollment: %w", err)
		}

		clusterTLS = cluster.NewClusterTLS()
		if err := clusterTLS.Load(result.ClusterCertPEM, result.ClusterKeyPEM, result.CACertPEM); err != nil {
			return fmt.Errorf("load enrolled TLS material: %w", err)
		}
		if err := cluster.SaveFile(hd.ClusterTLSPath(), result.ClusterCertPEM, result.ClusterKeyPEM, result.CACertPEM); err != nil {
			return fmt.Errorf("save cluster TLS file: %w", err)
		}
		logger.Info("cluster enrollment successful, TLS loaded and saved")
	}

	// Create cluster server if --cluster-addr is set (multi-node mode).
	var clusterSrv *cluster.Server
	if clusterAddr != "" && configType == "raft" { //nolint:nestif // cluster setup has unavoidable branching
		// Always create the ClusterTLS holder for multi-node mode.
		// - cluster-init: populated after Raft is ready (bootstrapClusterTLS)
		// - join: already populated from enrollment above
		// - restart: loaded from local file (cluster-tls.json)
		// The dynamic transport credentials fall back to insecure until loaded.
		if clusterTLS == nil {
			clusterTLS = cluster.NewClusterTLS()
			// Restart path: load TLS from local file persisted during
			// previous cluster-init or enrollment. This must happen before
			// Raft starts so the transport uses mTLS from the beginning.
			if found, err := clusterTLS.LoadFile(hd.ClusterTLSPath()); err != nil {
				return fmt.Errorf("load cluster TLS file: %w", err)
			} else if found {
				logger.Info("cluster TLS loaded from local file")
			}
		}

		clusterSrv, err = cluster.New(cluster.Config{
			ClusterAddr: clusterAddr,
			TLS:         clusterTLS,
			Logger:      logger.With("component", "cluster"),
		})
		if err != nil {
			return fmt.Errorf("create cluster server: %w", err)
		}
	}

	disp := &configDispatcher{localNodeID: nodeID, logger: logger.With("component", "dispatch"), clusterTLS: clusterTLS, tlsFilePath: hd.ClusterTLSPath()}
	cfgStore, err := openConfigStore(hd, configType, nodeID, clusterInit, clusterSrv, clusterTLS, logger, raftfsm.WithOnApply(disp.Handle))
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}
	if c, ok := cfgStore.(io.Closer); ok {
		defer func() { _ = c.Close() }()
	}

	// Verify that cluster TLS is loaded for the restart case.
	// cluster-init populates TLS later (bootstrapClusterTLS); join loaded it
	// from enrollment; restart loaded it from the local file above.
	if clusterTLS != nil && clusterTLS.State() == nil && !clusterInit && joinAddr == "" {
		return errors.New("cluster TLS not found (expected cluster-tls.json from previous cluster membership)")
	}

	// Bootstrap TLS: generate CA + cluster cert + join token and store via Raft.
	// This happens after Raft is ready but before starting the gRPC server.
	if clusterSrv != nil && clusterInit && clusterTLS != nil {
		if err := bootstrapClusterTLS(ctx, cfgStore, clusterTLS, hd.ClusterTLSPath(), logger); err != nil {
			return fmt.Errorf("bootstrap cluster TLS: %w", err)
		}
	}

	// Start the cluster gRPC server after Raft is created. Other nodes
	// need to reach this node's cluster port when they join.
	if clusterSrv != nil {
		// Wire the Enroll handler for accepting new nodes.
		clusterSrv.SetEnrollHandler(makeEnrollHandler(cfgStore, logger))

		if err := clusterSrv.Start(); err != nil {
			return fmt.Errorf("start cluster server: %w", err)
		}
		defer clusterSrv.Stop()
	}

	// For restarting multi-node members: wait for a leader now that our
	// gRPC server is running and can receive Raft RPCs from peers.
	if clusterSrv != nil && !clusterInit && joinAddr == "" {
		if rcs, ok := cfgStore.(*raftConfigStore); ok {
			logger.Info("waiting for cluster quorum (start 2+ nodes)")
			if err := rcs.WaitForLeader(ctx, logger); err != nil {
				return err
			}
			logger.Info("cluster leader found")
		}
	}

	// For joining nodes: now that our cluster server and Raft are running,
	// ask the leader to add us as a voter. This must happen after Start()
	// so we're reachable when the leader tries to replicate to us.
	if joinAddr != "" && clusterTLS != nil && clusterAddr != "" {
		logger.Info("requesting voter membership from leader", "leader_addr", joinAddr)
		joinCtx, joinCancel := context.WithTimeout(ctx, 30*time.Second)
		if err := cluster.JoinCluster(joinCtx, joinAddr, nodeID, clusterAddr, clusterTLS); err != nil {
			joinCancel()
			return fmt.Errorf("join cluster: %w", err)
		}
		joinCancel()
		logger.Info("voter membership granted by leader")
	}

	// For joining or restarting cluster nodes, wait for config replication
	// from the leader. Hashicorp Raft does not replay log entries to the FSM
	// on startup — entries are only applied once a leader commits them. So
	// after finding the leader, we must wait for the FSM to catch up.
	//
	// For single-node, memory, or cluster-init leader: config is local.
	var cfg *config.Config
	clusterRestart := clusterSrv != nil && !clusterInit && joinAddr == ""
	if (joinAddr != "" || clusterRestart) && configType == "raft" { //nolint:nestif // join vs restart vs local config paths
		if joinAddr != "" {
			logger.Info("joining cluster, waiting for config replication", "join_addr", joinAddr)
		} else {
			logger.Info("waiting for config replication from leader")
		}
		cfg, err = waitForConfig(ctx, cfgStore, 60*time.Second, logger)
		if err != nil {
			return fmt.Errorf("wait for config replication: %w", err)
		}
	} else {
		// Ensure this node has a NodeConfig in the config store.
		// If not, generate a petname and persist it.
		if err := ensureNodeConfig(ctx, cfgStore, nodeID); err != nil {
			return fmt.Errorf("ensure node config: %w", err)
		}

		logger.Info("loading config", "type", configType)
		cfg, err = ensureConfig(ctx, logger, cfgStore, bootstrap)
		if err != nil {
			return err
		}
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

	return serveAndAwaitShutdown(ctx, logger, serverAddr, homeDir, nodeID, socketPath, orch, cfgStore, factories, tokens, certMgr, noAuth, afterConfigApply, clusterSrv)
}

// waitForConfig polls the config store until a non-nil config is available.
// Used by joining nodes that wait for the leader to replicate config via Raft.
func waitForConfig(ctx context.Context, store config.Store, timeout time.Duration, logger *slog.Logger) (*config.Config, error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline:
			return nil, errors.New("timed out waiting for config replication from leader")
		case <-ticker.C:
			cfg, err := store.Load(ctx)
			if err != nil {
				logger.Debug("waiting for config", "error", err)
				continue
			}
			if cfg != nil {
				return cfg, nil
			}
		}
	}
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

func serveAndAwaitShutdown(ctx context.Context, logger *slog.Logger, serverAddr, homeDir, nodeID, socketPath string, orch *orchestrator.Orchestrator, cfgStore config.Store, factories orchestrator.Factories, tokens *auth.TokenService, certMgr *cert.Manager, noAuth bool, afterConfigApply func(raftfsm.Notification), clusterSrv *cluster.Server) error {
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

	// Shutdown order: HTTP server → orchestrator → cluster server.
	// The cluster server stops after orchestrator so that in-flight Raft
	// RPCs (e.g. final log replication) can complete.
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

	if clusterSrv != nil {
		logger.Info("stopping cluster server")
		clusterSrv.Stop()
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
// For raft mode with a cluster server, the Raft transport is wired through gRPC.
func openConfigStore(hd home.Dir, configType, nodeID string, clusterInit bool, clusterSrv *cluster.Server, clusterTLS *cluster.ClusterTLS, logger *slog.Logger, fsmOpts ...raftfsm.Option) (config.Store, error) {
	switch configType {
	case "memory":
		return configmem.NewStore(), nil
	case "raft":
		return openRaftConfigStore(hd, nodeID, clusterInit, clusterSrv, clusterTLS, logger, fsmOpts...)
	default:
		return nil, fmt.Errorf("unknown config store type: %q", configType)
	}
}

// raftConfigStore wraps a raftstore.Store with cleanup logic for the
// underlying raft instance, forwarder, and boltdb store.
type raftConfigStore struct {
	config.Store
	raft      *hraft.Raft
	boltDB    io.Closer
	forwarder io.Closer // *cluster.Forwarder; nil for single-node
}

// WaitForLeader polls until any node in the cluster becomes leader or the
// context is cancelled. Logs a reminder every 10 seconds so the operator
// knows the node is alive and waiting for peers.
func (s *raftConfigStore) WaitForLeader(ctx context.Context, logger *slog.Logger) error {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	remind := time.NewTicker(10 * time.Second)
	defer remind.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-remind.C:
			logger.Info("still waiting for cluster quorum (start 2+ nodes)")
		case <-ticker.C:
			if addr, _ := s.raft.LeaderWithID(); addr != "" {
				return nil
			}
		}
	}
}

func (s *raftConfigStore) Close() error {
	if s.forwarder != nil {
		_ = s.forwarder.Close()
	}
	future := s.raft.Shutdown()
	err := future.Error()
	if cerr := s.boltDB.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// openRaftConfigStore creates a raft-backed config store with BoltDB persistence.
//
// In single-node mode (clusterSrv == nil): uses in-memory transport, tight
// timeouts, and auto-bootstraps on first run. This preserves backward compat.
//
// In multi-node mode (clusterSrv != nil): uses gRPC transport, production
// timeouts, conditional bootstrap, and leader forwarding.
func openRaftConfigStore(hd home.Dir, nodeID string, clusterInit bool, clusterSrv *cluster.Server, clusterTLS *cluster.ClusterTLS, logger *slog.Logger, fsmOpts ...raftfsm.Option) (config.Store, error) {
	multiNode := clusterSrv != nil

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

	// Transport: gRPC in multi-node mode, in-memory for single-node.
	var transport hraft.Transport
	if multiNode {
		transport = clusterSrv.Transport()
	} else {
		_, transport = hraft.NewInmemTransport("")
	}

	fsm := raftfsm.New(fsmOpts...)

	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.LogOutput = io.Discard

	if multiNode {
		// Production timeouts for multi-node clusters.
		conf.HeartbeatTimeout = 1000 * time.Millisecond
		conf.ElectionTimeout = 1000 * time.Millisecond
		conf.LeaderLeaseTimeout = 500 * time.Millisecond
	} else {
		// Single-node: tight timeouts for near-instant election.
		conf.HeartbeatTimeout = 100 * time.Millisecond
		conf.ElectionTimeout = 100 * time.Millisecond
		conf.LeaderLeaseTimeout = 100 * time.Millisecond
	}

	r, err := hraft.NewRaft(conf, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	// Bootstrap: conditional on existing state and mode.
	existing := r.GetConfiguration()
	if err := existing.Error(); err != nil {
		_ = r.Shutdown().Error()
		_ = boltStore.Close()
		return nil, fmt.Errorf("get raft configuration: %w", err)
	}

	needsBootstrap := len(existing.Configuration().Servers) == 0
	if needsBootstrap {
		// In single-node mode, always auto-bootstrap (backward compat).
		// In multi-node mode, only bootstrap if --cluster-init is set.
		if !multiNode || clusterInit {
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
			logger.Info("raft cluster bootstrapped", "node_id", nodeID)
		} else {
			logger.Info("raft: no existing configuration; waiting to be added to a cluster")
		}
	}

	// Wait for leadership or a known leader before proceeding.
	// - Single-node / cluster-init: this node must become leader.
	// - Multi-node restart: wait for any leader (requires quorum — 2+ nodes).
	if !multiNode || clusterInit {
		select {
		case <-r.LeaderCh():
		case <-time.After(5 * time.Second):
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return nil, errors.New("timed out waiting for raft leadership")
		}
	}

	logger.Info("raft config store ready", "dir", raftDir, "multi_node", multiNode)

	store := raftstore.New(r, fsm, 10*time.Second)

	var fwd *cluster.Forwarder
	if multiNode {
		// Wire the cluster server with the Raft instance.
		clusterSrv.SetRaft(r)

		// Provide the apply function for ForwardApply handler on the leader.
		clusterSrv.SetApplyFn(func(ctx context.Context, data []byte) error {
			return store.ApplyRaw(data)
		})

		// Enable leader forwarding on followers.
		fwd = cluster.NewForwarder(r, clusterTLS)
		store.SetForwarder(fwd)
	}

	return &raftConfigStore{
		Store:     store,
		raft:      r,
		boltDB:    boltStore,
		forwarder: fwd,
	}, nil
}

// bootstrapClusterTLS generates CA, cluster cert, and join token, then stores
// them via Raft and loads the material into the ClusterTLS holder.
func bootstrapClusterTLS(ctx context.Context, cfgStore config.Store, ctls *cluster.ClusterTLS, tlsFilePath string, logger *slog.Logger) error {
	// Check if TLS material already exists (re-bootstrap scenario).
	existingCfg, err := cfgStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("check existing cluster TLS: %w", err)
	}
	if existingCfg != nil && existingCfg.ClusterTLS != nil {
		existing := existingCfg.ClusterTLS
		// Load existing material.
		if err := ctls.Load([]byte(existing.ClusterCertPEM), []byte(existing.ClusterKeyPEM), []byte(existing.CACertPEM)); err != nil {
			return fmt.Errorf("load existing cluster TLS: %w", err)
		}
		if err := cluster.SaveFile(tlsFilePath, []byte(existing.ClusterCertPEM), []byte(existing.ClusterKeyPEM), []byte(existing.CACertPEM)); err != nil {
			return fmt.Errorf("save cluster TLS file: %w", err)
		}
		logger.Info("cluster TLS loaded from existing config")
		_, caHash, _ := tlsutil.ParseJoinToken(existing.JoinToken)
		logger.Info("cluster join token", "token", existing.JoinToken, "ca_hash", caHash)
		return nil
	}

	// Generate fresh TLS material.
	ca, err := tlsutil.GenerateCA()
	if err != nil {
		return fmt.Errorf("generate CA: %w", err)
	}
	cert, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, nil)
	if err != nil {
		return fmt.Errorf("generate cluster cert: %w", err)
	}
	token, err := tlsutil.GenerateJoinToken(ca.CertPEM)
	if err != nil {
		return fmt.Errorf("generate join token: %w", err)
	}

	// Store atomically via Raft.
	if err := cfgStore.PutClusterTLS(ctx, config.ClusterTLS{
		CACertPEM:      string(ca.CertPEM),
		CAKeyPEM:       string(ca.KeyPEM),
		ClusterCertPEM: string(cert.CertPEM),
		ClusterKeyPEM:  string(cert.KeyPEM),
		JoinToken:      token,
	}); err != nil {
		return fmt.Errorf("store cluster TLS: %w", err)
	}

	// Load into the atomic pointer — new connections use mTLS from here.
	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		return fmt.Errorf("load cluster TLS: %w", err)
	}

	// Persist to local file for restart without Raft quorum.
	if err := cluster.SaveFile(tlsFilePath, cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		return fmt.Errorf("save cluster TLS file: %w", err)
	}

	logger.Info("cluster TLS bootstrapped")
	logger.Info("cluster join token (use --join-token to join)", "token", token)

	return nil
}

// makeEnrollHandler creates the Enroll RPC handler for the cluster server.
// It verifies the join token, adds the new node as a Raft voter, and returns
// the TLS material needed for the node to participate in the cluster.
func makeEnrollHandler(cfgStore config.Store, logger *slog.Logger) cluster.EnrollHandler {
	return func(ctx context.Context, req *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error) {
		// Read stored TLS material from Config.
		cfg, err := cfgStore.Load(ctx)
		if err != nil || cfg == nil || cfg.ClusterTLS == nil {
			logger.Error("enroll: read cluster TLS", "error", err)
			return nil, errors.New("cluster TLS not available")
		}
		tls := cfg.ClusterTLS

		// Verify the token secret.
		storedSecret, _, err := tlsutil.ParseJoinToken(tls.JoinToken)
		if err != nil {
			return nil, fmt.Errorf("parse stored join token: %w", err)
		}
		if req.GetTokenSecret() != storedSecret {
			logger.Warn("enroll: invalid token secret", "node_id", req.GetNodeId())
			return nil, errors.New("invalid join token")
		}

		logger.Info("enroll: token verified, returning TLS material",
			"node_id", req.GetNodeId(),
			"node_addr", req.GetNodeAddr())

		return &gastrologv1.EnrollResponse{
			CaCertPem:      []byte(tls.CACertPEM),
			ClusterCertPem: []byte(tls.ClusterCertPEM),
			ClusterKeyPem:  []byte(tls.ClusterKeyPEM),
		}, nil
	}
}
