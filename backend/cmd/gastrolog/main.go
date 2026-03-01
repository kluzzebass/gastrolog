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
	"syscall"
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
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/config"
	configmem "gastrolog/internal/config/memory"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/config/raftstore"
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
	ingestmetrics "gastrolog/internal/ingester/metrics"
	ingestotlp "gastrolog/internal/ingester/otlp"
	ingestrelp "gastrolog/internal/ingester/relp"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	ingesttail "gastrolog/internal/ingester/tail"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"github.com/spf13/cobra"
)

var version = "dev"

const errFmtSaveClusterTLS = "save cluster TLS file: %w"

func main() {
	// Register signal handler early, before any framework code.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

			cfg := runConfig{
				HomeFlag:    homeFlag,
				ConfigType:  configType,
				ServerAddr:  mustString(cmd, "addr"),
				Bootstrap:   mustBool(cmd, "bootstrap"),
				NoAuth:      mustBool(cmd, "no-auth"),
				ClusterAddr: mustString(cmd, "cluster-addr"),
				ClusterInit: mustBool(cmd, "cluster-init"),
				JoinAddr:    mustString(cmd, "join-addr"),
				JoinToken:   mustString(cmd, "join-token"),
			}

			err := run(cmd.Context(), logger, cfg)
			if cmd.Context().Err() != nil {
				return nil //nolint:nilerr // signal-triggered shutdown is not an error
			}
			return err
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
func mustBool(cmd *cobra.Command, name string) bool { v, _ := cmd.Flags().GetBool(name); return v }

// runConfig groups all CLI flags for the server command.
type runConfig struct {
	HomeFlag    string
	ConfigType  string
	ServerAddr  string
	Bootstrap   bool
	NoAuth      bool
	ClusterAddr string
	ClusterInit bool
	JoinAddr    string
	JoinToken   string
}

func run(ctx context.Context, logger *slog.Logger, cfg runConfig) error {
	hd, err := resolveHome(cfg.HomeFlag)
	if err != nil {
		return fmt.Errorf("resolve home directory: %w", err)
	}

	nodeID, err := resolveIdentity(logger, cfg, hd)
	if err != nil {
		return err
	}

	clusterSrv, clusterTLS, err := setupCluster(ctx, logger, cfg, hd, nodeID)
	if err != nil {
		return err
	}

	disp := &configDispatcher{localNodeID: nodeID, logger: logger.With("component", "dispatch"), clusterTLS: clusterTLS, tlsFilePath: hd.ClusterTLSPath()}
	cfgStore, err := openConfigStore(cfg.ConfigType, raftStoreOpts{
		Home: hd, NodeID: nodeID, Init: cfg.ClusterInit,
		ClusterSrv: clusterSrv, ClusterTLS: clusterTLS,
		Logger: logger, FSMOpts: []raftfsm.Option{raftfsm.WithOnApply(disp.Handle)},
	})
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}
	if c, ok := cfgStore.(io.Closer); ok {
		defer func() { _ = c.Close() }()
	}

	if err := startClusterServices(ctx, cfg, clusterSrv, clusterTLS, cfgStore, hd, logger); err != nil {
		return err
	}
	if clusterSrv != nil {
		defer clusterSrv.Stop()
	}

	// Non-blocking: try local FSM, bootstrap, or return nil for replication cases.
	appCfg, fromLocalFSM, err := loadLocalConfig(ctx, logger, cfg, cfgStore, clusterSrv, clusterTLS, nodeID)
	if err != nil {
		return err
	}

	asyncNodeConfig := fromLocalFSM || appCfg == nil
	homeDir, socketPath, err := finalizeNodeSetup(ctx, logger, cfgStore, nodeID, cfg.ConfigType, asyncNodeConfig, hd)
	if err != nil {
		return err
	}

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

	if err := startOrchestrator(ctx, logger, orch, appCfg, factories); err != nil {
		return err
	}

	broadcaster, peerState, localStatsFn := setupClusterStats(ctx, logger, cfgStore, clusterSrv, clusterTLS, orch, nodeID)

	// For replication cases (cluster restart without local config, joining
	// nodes): block until server settings replicate from the leader before
	// starting the HTTP server. The orchestrator is already running and
	// receives vaults/ingesters via FSM dispatch as they replicate.
	if err := awaitReplication(ctx, appCfg, cfg.ConfigType, cfgStore, logger); err != nil {
		return err
	}

	tokens, err := buildAuthTokens(ctx, logger, cfgStore, cfg.NoAuth)
	if err != nil {
		return err
	}

	certMgr, err := loadCertManager(ctx, logger, cfgStore)
	if err != nil {
		return err
	}

	return serveAndAwaitShutdown(ctx, serverDeps{
		Logger:           logger,
		ServerAddr:       cfg.ServerAddr,
		HomeDir:          homeDir,
		NodeID:           nodeID,
		SocketPath:       socketPath,
		Orch:             orch,
		CfgStore:         cfgStore,
		Factories:        factories,
		Tokens:           tokens,
		CertMgr:          certMgr,
		NoAuth:           cfg.NoAuth,
		AfterConfigApply: nonRaftApplyHook(cfg.ConfigType, disp.Handle),
		ClusterSrv:       clusterSrv,
		Broadcaster:      broadcaster,
		PeerState:        peerState,
		LocalStats:       localStatsFn,
	})
}

// nonRaftApplyHook returns the dispatcher callback for non-raft config stores.
// Raft stores fire FSM notifications directly; other stores need the server to
// dispatch config changes manually.
func nonRaftApplyHook(configType string, handle func(raftfsm.Notification)) func(raftfsm.Notification) {
	if configType != "raft" {
		return handle
	}
	return nil
}

// startOrchestrator applies config, rebuilds missing indexes, and starts the
// orchestrator. ApplyConfig is nil-safe: returns nil immediately when appCfg is nil.
func startOrchestrator(ctx context.Context, logger *slog.Logger, orch *orchestrator.Orchestrator, appCfg *config.Config, factories orchestrator.Factories) error {
	if appCfg != nil {
		logger.Info("loaded config",
			"ingesters", len(appCfg.Ingesters),
			"vaults", len(appCfg.Vaults))
	}
	if err := orch.ApplyConfig(appCfg, factories); err != nil {
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
	return nil
}

// setupClusterStats creates the broadcaster, peer state tracker, and stats
// collector. Returns nils for single-node mode. Launches the collector
// goroutine when in cluster mode.
func setupClusterStats(ctx context.Context, logger *slog.Logger, cfgStore config.Store, clusterSrv *cluster.Server, clusterTLS *cluster.ClusterTLS, orch *orchestrator.Orchestrator, nodeID string) (*cluster.Broadcaster, *cluster.PeerState, func() *gastrologv1.NodeStats) {
	// Create the cluster broadcaster for peer-to-peer message fan-out.
	var broadcaster *cluster.Broadcaster
	if rcs, ok := cfgStore.(*raftConfigStore); ok && clusterSrv != nil {
		broadcaster = cluster.NewBroadcaster(rcs.raft, clusterTLS, nodeID, logger.With("component", "broadcast"))
	}
	if broadcaster == nil || clusterSrv == nil {
		return nil, nil, nil
	}

	// Read broadcast interval from cluster-wide settings.
	var broadcastInterval time.Duration
	if ss, err := cfgStore.LoadServerSettings(ctx); err == nil && ss.Cluster.BroadcastInterval != "" {
		if d, err := time.ParseDuration(ss.Cluster.BroadcastInterval); err == nil {
			broadcastInterval = d
		}
	}

	peerState := cluster.NewPeerState(15 * time.Second)
	clusterSrv.Subscribe(peerState.HandleBroadcast)

	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{
		Broadcaster: broadcaster,
		RaftStats:   clusterSrv,
		Stats:       &orchStatsAdapter{orch: orch},
		NodeID:      nodeID,
		NodeNameFn: func() string {
			nid, err := uuid.Parse(nodeID)
			if err != nil {
				return ""
			}
			n, err := cfgStore.GetNode(ctx, nid)
			if err != nil || n == nil {
				return ""
			}
			return n.Name
		},
		Version:   version,
		StartTime: time.Now(),
		Interval:  broadcastInterval,
		Logger:    logger.With("component", "stats-collector"),
	})
	go collector.Run(ctx)

	return broadcaster, peerState, collector.CollectLocal
}

// resolveIdentity ensures the home directory exists and resolves the node ID.
func resolveIdentity(logger *slog.Logger, cfg runConfig, hd home.Dir) (string, error) {
	if cfg.ConfigType != "memory" {
		if err := hd.EnsureExists(); err != nil {
			return "", err
		}
		logger.Info("home directory", "path", hd.Root())
	}

	if cfg.ConfigType == "memory" {
		return uuid.Must(uuid.NewV7()).String(), nil
	}
	nodeID, err := hd.NodeID()
	if err != nil {
		return "", fmt.Errorf("resolve node ID: %w", err)
	}
	return nodeID, nil
}

// startClusterServices verifies cluster TLS, bootstraps TLS if needed, and starts
// the cluster gRPC server. No-op for single-node mode.
func startClusterServices(ctx context.Context, cfg runConfig, clusterSrv *cluster.Server, clusterTLS *cluster.ClusterTLS, cfgStore config.Store, hd home.Dir, logger *slog.Logger) error {
	// Verify that cluster TLS is loaded for the restart case.
	if clusterTLS != nil && clusterTLS.State() == nil && !cfg.ClusterInit && cfg.JoinAddr == "" {
		return errors.New("cluster TLS not found (expected cluster-tls.json from previous cluster membership)")
	}

	// Bootstrap TLS: generate CA + cluster cert + join token and store via Raft.
	if clusterSrv != nil && cfg.ClusterInit && clusterTLS != nil {
		if err := bootstrapClusterTLS(ctx, cfgStore, clusterTLS, hd.ClusterTLSPath(), logger); err != nil {
			return fmt.Errorf("bootstrap cluster TLS: %w", err)
		}
	}

	// Start the cluster gRPC server after Raft is created.
	if clusterSrv != nil {
		clusterSrv.SetEnrollHandler(makeEnrollHandler(cfgStore, logger))
		if err := clusterSrv.Start(); err != nil {
			return fmt.Errorf("start cluster server: %w", err)
		}
	}

	return nil
}

// setupCluster handles cluster enrollment and cluster server creation.
// Returns nil cluster server and nil TLS for single-node mode.
func setupCluster(ctx context.Context, logger *slog.Logger, cfg runConfig, hd home.Dir, nodeID string) (*cluster.Server, *cluster.ClusterTLS, error) {
	var clusterTLS *cluster.ClusterTLS

	// Joining flow: enroll with the leader before creating the cluster server.
	if cfg.JoinAddr != "" && cfg.JoinToken != "" && cfg.ClusterAddr != "" && cfg.ConfigType == "raft" {
		var err error
		clusterTLS, err = enrollInCluster(ctx, logger, cfg, hd, nodeID)
		if err != nil {
			return nil, nil, err
		}
	}

	// Not in cluster mode — nothing more to do.
	if cfg.ClusterAddr == "" || cfg.ConfigType != "raft" {
		return nil, clusterTLS, nil
	}

	// Load TLS from disk if not already obtained via enrollment.
	if clusterTLS == nil {
		clusterTLS = cluster.NewClusterTLS()
		if found, err := clusterTLS.LoadFile(hd.ClusterTLSPath()); err != nil {
			return nil, nil, fmt.Errorf("load cluster TLS file: %w", err)
		} else if found {
			logger.Info("cluster TLS loaded from local file")
		}
	}

	clusterSrv, err := cluster.New(cluster.Config{
		ClusterAddr: cfg.ClusterAddr,
		TLS:         clusterTLS,
		Logger:      logger.With("component", "cluster"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create cluster server: %w", err)
	}

	return clusterSrv, clusterTLS, nil
}

// enrollInCluster performs the Enroll RPC to obtain TLS material from the
// cluster leader, loads it into a ClusterTLS, and saves it to disk.
func enrollInCluster(ctx context.Context, logger *slog.Logger, cfg runConfig, hd home.Dir, nodeID string) (*cluster.ClusterTLS, error) {
	tokenSecret, caHash, err := tlsutil.ParseJoinToken(cfg.JoinToken)
	if err != nil {
		return nil, fmt.Errorf("parse join token: %w", err)
	}

	logger.Info("enrolling with cluster leader", "leader_addr", cfg.JoinAddr)
	enrollCtx, enrollCancel := context.WithTimeout(ctx, 30*time.Second)
	result, err := cluster.Enroll(enrollCtx, cfg.JoinAddr, tokenSecret, caHash, nodeID, cfg.ClusterAddr)
	enrollCancel()
	if err != nil {
		return nil, fmt.Errorf("cluster enrollment: %w", err)
	}

	clusterTLS := cluster.NewClusterTLS()
	if err := clusterTLS.Load(result.ClusterCertPEM, result.ClusterKeyPEM, result.CACertPEM); err != nil {
		return nil, fmt.Errorf("load enrolled TLS material: %w", err)
	}
	if err := cluster.SaveFile(hd.ClusterTLSPath(), result.ClusterCertPEM, result.ClusterKeyPEM, result.CACertPEM); err != nil {
		return nil, fmt.Errorf(errFmtSaveClusterTLS, err)
	}
	logger.Info("cluster enrollment successful, TLS loaded and saved")
	return clusterTLS, nil
}

// loadLocalConfig attempts to load config from the local FSM or bootstrap.
// Returns nil config (without blocking) when config must replicate from the
// cluster leader — the orchestrator starts empty and receives vaults/ingesters
// via FSM dispatch as they arrive.
func loadLocalConfig(ctx context.Context, logger *slog.Logger, cfg runConfig, cfgStore config.Store, clusterSrv *cluster.Server, clusterTLS *cluster.ClusterTLS, nodeID string) (*config.Config, bool, error) {
	// For joining nodes: request voter membership from the leader.
	if err := requestVoterMembership(ctx, logger, cfg, clusterTLS, nodeID); err != nil {
		return nil, false, err
	}

	// For restarting multi-node members: try loading from local FSM.
	// On a proper restart, NewRaft replays the log/snapshot into the FSM
	// synchronously, so Load() should return non-nil.
	clusterRestart := clusterSrv != nil && !cfg.ClusterInit && cfg.JoinAddr == "" && cfg.ConfigType == "raft"
	if clusterRestart {
		localCfg, _ := cfgStore.Load(ctx)
		if localCfg != nil {
			return localCfg, true, nil
		}
		// First start or lost Raft state — orchestrator will start empty,
		// HTTP server will wait for settings to replicate.
		logger.Info("no local config, orchestrator will start empty")
		return nil, false, nil
	}

	// Joining a cluster: config will replicate via Raft after voter
	// membership is granted. Return nil so the orchestrator starts empty.
	if cfg.JoinAddr != "" {
		logger.Info("joining cluster, config will replicate from leader")
		return nil, false, nil
	}

	// Single-node / init: load or bootstrap immediately.
	logger.Info("loading config", "type", cfg.ConfigType)
	appCfg, err := ensureConfig(ctx, logger, cfgStore, cfg.Bootstrap)
	if err != nil {
		return nil, false, err
	}
	return appCfg, false, nil
}

// requestVoterMembership asks the cluster leader to add this node as a Raft voter.
// It is a no-op if the join parameters are not set.
func requestVoterMembership(ctx context.Context, logger *slog.Logger, cfg runConfig, clusterTLS *cluster.ClusterTLS, nodeID string) error {
	if cfg.JoinAddr == "" || clusterTLS == nil || cfg.ClusterAddr == "" {
		return nil
	}
	logger.Info("requesting voter membership from leader", "leader_addr", cfg.JoinAddr)
	joinCtx, joinCancel := context.WithTimeout(ctx, 30*time.Second)
	defer joinCancel()
	if err := cluster.JoinCluster(joinCtx, cfg.JoinAddr, nodeID, cfg.ClusterAddr, clusterTLS); err != nil {
		return fmt.Errorf("join cluster: %w", err)
	}
	logger.Info("voter membership granted by leader")
	return nil
}

// finalizeNodeSetup ensures this node has a NodeConfig with a petname and
// resolves the home directory and socket path for non-memory stores.
// When asyncNodeConfig is true (restart from local FSM, or no config yet),
// the petname write is deferred to a background goroutine because the config
// store may not be writable yet (needs Raft quorum).
func finalizeNodeSetup(ctx context.Context, logger *slog.Logger, cfgStore config.Store, nodeID, configType string, asyncNodeConfig bool, hd home.Dir) (string, string, error) {
	if asyncNodeConfig {
		logger.Info("node identity", "node_id", nodeID)
		go ensureNodeConfigAsync(ctx, cfgStore, nodeID, configType, hd, logger)
	} else {
		nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID)
		if err != nil {
			return "", "", fmt.Errorf("ensure node config: %w", err)
		}
		logger.Info("node identity", "node_id", nodeID, "node_name", nodeName)
		if configType != "memory" {
			_ = hd.WriteNodeName(nodeName)
		}
	}

	homeDir := ""
	socketPath := ""
	if configType != "memory" {
		homeDir = hd.Root()
		socketPath = hd.SocketPath()
	}
	return homeDir, socketPath, nil
}

// awaitReplication is a no-op when config was loaded locally. For replication
// cases (cluster restart without local FSM, joining nodes), it polls until
// server settings replicate from the leader. Server settings require Raft
// quorum, so this implicitly waits for leader election too. A separate
// waitForQuorum call is NOT needed here — ensureNodeConfigAsync already
// logs quorum progress in the background.
func awaitReplication(ctx context.Context, appCfg *config.Config, configType string, cfgStore config.Store, logger *slog.Logger) error {
	if appCfg != nil || configType != "raft" {
		return nil
	}
	return waitForServerSettings(ctx, cfgStore, 60*time.Second, logger)
}

// waitForServerSettings polls until server settings (specifically the JWT
// secret) are available in the config store. This is the minimal prerequisite
// for starting the HTTP server — it needs the JWT secret for auth and TLS
// config for certs. The orchestrator is already running and receiving
// vaults/ingesters via FSM dispatch while this blocks.
func waitForServerSettings(ctx context.Context, cfgStore config.Store, timeout time.Duration, logger *slog.Logger) error {
	logger.Info("waiting for server settings replication")
	deadline := time.After(timeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	remind := time.NewTicker(10 * time.Second)
	defer remind.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline:
			return errors.New("timed out waiting for server settings replication")
		case <-remind.C:
			logger.Info("still waiting for server settings replication")
		case <-ticker.C:
			ss, err := cfgStore.LoadServerSettings(ctx)
			if err != nil {
				continue
			}
			if ss.Auth.JWTSecret != "" {
				logger.Info("server settings received")
				return nil
			}
		}
	}
}

// ensureNodeConfig checks whether a NodeConfig exists for the local node ID.
// If not, it generates a petname and persists it to the config store.
// Returns the node name (existing or newly generated).
func ensureNodeConfig(ctx context.Context, cfgStore config.Store, nodeID string) (string, error) {
	nodeUUID, err := uuid.Parse(nodeID)
	if err != nil {
		return "", fmt.Errorf("parse node ID %q: %w", nodeID, err)
	}
	existing, err := cfgStore.GetNode(ctx, nodeUUID)
	if err != nil {
		return "", fmt.Errorf("get node: %w", err)
	}
	if existing != nil {
		return existing.Name, nil
	}
	name := petname.Generate(2, "-")
	if err := cfgStore.PutNode(ctx, config.NodeConfig{ID: nodeUUID, Name: name}); err != nil {
		return "", err
	}
	return name, nil
}

// waitForQuorum blocks until a Raft leader is elected, if the config store
// is Raft-backed. Returns nil immediately for non-Raft stores.
func waitForQuorum(ctx context.Context, cfgStore config.Store, logger *slog.Logger) error {
	rcs, ok := cfgStore.(*raftConfigStore)
	if !ok {
		return nil
	}
	logger.Info("waiting for cluster quorum (start 2+ nodes)")
	if err := rcs.WaitForLeader(ctx, logger); err != nil {
		return err
	}
	logger.Info("cluster leader found")
	return nil
}

// ensureNodeConfigAsync waits for a cluster leader in the background, then
// writes the node's petname. Used on restart to avoid blocking ingestion.
func ensureNodeConfigAsync(ctx context.Context, cfgStore config.Store, nodeID, configType string, hd home.Dir, logger *slog.Logger) {
	if err := waitForQuorum(ctx, cfgStore, logger); err != nil {
		return
	}
	nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID)
	if err != nil {
		logger.Warn("ensure node config failed (will retry on next start)", "error", err)
		return
	}
	logger.Info("node name", "node_id", nodeID, "node_name", nodeName)
	if configType != "memory" {
		_ = hd.WriteNodeName(nodeName)
	}
}

func ensureConfig(ctx context.Context, logger *slog.Logger, cfgStore config.Store, bootstrap bool) (*config.Config, error) {
	cfg, err := cfgStore.Load(ctx)
	if err != nil {
		return nil, err
	}

	// Check whether server settings (JWT secret) exist. Load() may return
	// non-nil when only cluster TLS has been stored (before server settings
	// are bootstrapped), so a nil check on cfg alone is insufficient.
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, err
	}
	if cfg != nil && ss.Auth.JWTSecret != "" {
		return cfg, nil
	}

	if cfg == nil && bootstrap {
		logger.Info("no config found, bootstrapping default configuration")
		if err := config.Bootstrap(ctx, cfgStore); err != nil {
			return nil, fmt.Errorf("bootstrap config: %w", err)
		}
	} else if ss.Auth.JWTSecret == "" {
		logger.Info("bootstrapping server settings (auth + query defaults)")
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
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return 0
	}
	return ss.Scheduler.MaxConcurrentJobs
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
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("load server settings for TLS: %w", err)
	}
	if err := certMgr.LoadFromConfig(ss.TLS.DefaultCert, certs); err != nil {
		return nil, fmt.Errorf("load certs: %w", err)
	}
	return certMgr, nil
}

// serverDeps bundles the dependencies needed to start the HTTP server and
// await shutdown. This avoids an excessively long parameter list.
type serverDeps struct {
	Logger           *slog.Logger
	ServerAddr       string
	HomeDir          string
	NodeID           string
	SocketPath       string
	Orch             *orchestrator.Orchestrator
	CfgStore         config.Store
	Factories        orchestrator.Factories
	Tokens           *auth.TokenService
	CertMgr          *cert.Manager
	NoAuth           bool
	AfterConfigApply func(raftfsm.Notification)
	ClusterSrv       *cluster.Server
	Broadcaster      *cluster.Broadcaster
	PeerState        *cluster.PeerState
	LocalStats       func() *gastrologv1.NodeStats
}

func serveAndAwaitShutdown(ctx context.Context, deps serverDeps) error {
	var srv *server.Server
	var serverWg sync.WaitGroup
	if deps.ServerAddr != "" {
		srv = server.New(deps.Orch, deps.CfgStore, deps.Factories, deps.Tokens, server.Config{Logger: deps.Logger, CertManager: deps.CertMgr, NoAuth: deps.NoAuth, HomeDir: deps.HomeDir, NodeID: deps.NodeID, UnixSocket: deps.SocketPath, AfterConfigApply: deps.AfterConfigApply, Cluster: deps.ClusterSrv, PeerStats: deps.PeerState, LocalStats: deps.LocalStats})
		serverWg.Go(func() {
			if err := srv.ServeTCP(deps.ServerAddr); err != nil {
				deps.Logger.Error("server error", "error", err)
			}
		})
	}

	<-ctx.Done()

	// Shutdown order: HTTP server → orchestrator → cluster server.
	if srv != nil {
		deps.Logger.Info("stopping server")
		if err := srv.Stop(ctx); err != nil {
			deps.Logger.Error("server stop error", "error", err)
		}
		serverWg.Wait()
	}

	deps.Logger.Info("shutting down orchestrator")
	if err := deps.Orch.Stop(); err != nil {
		return err
	}

	if deps.Broadcaster != nil {
		_ = deps.Broadcaster.Close()
	}

	if deps.ClusterSrv != nil {
		deps.Logger.Info("stopping cluster server")
		deps.ClusterSrv.Stop()
	}

	deps.Logger.Info("shutdown complete")
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
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return nil, fmt.Errorf("load server settings: %w", err)
	}
	if ss.Auth.JWTSecret == "" {
		return nil, errors.New("server config not found (bootstrap may have failed)")
	}

	secret, err := base64.StdEncoding.DecodeString(ss.Auth.JWTSecret)
	if err != nil {
		return nil, fmt.Errorf("decode JWT secret: %w", err)
	}

	duration := 168 * time.Hour // default 7 days
	if ss.Auth.TokenDuration != "" {
		duration, err = time.ParseDuration(ss.Auth.TokenDuration)
		if err != nil {
			return nil, fmt.Errorf("parse token duration: %w", err)
		}
	}

	return auth.NewTokenService(secret, duration), nil
}

// raftStoreOpts groups the parameters needed to open a raft-backed config store.
type raftStoreOpts struct {
	Home       home.Dir
	NodeID     string
	Init       bool
	ClusterSrv *cluster.Server
	ClusterTLS *cluster.ClusterTLS
	Logger     *slog.Logger
	FSMOpts    []raftfsm.Option
}

// openConfigStore creates a config.Store based on config type and home directory.
// For raft mode with a cluster server, the Raft transport is wired through gRPC.
func openConfigStore(configType string, opts raftStoreOpts) (config.Store, error) {
	switch configType {
	case "memory":
		return configmem.NewStore(), nil
	case "raft":
		return openRaftConfigStore(opts)
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
	// Take a snapshot before shutting down so that the next NewRaft can
	// restore FSM state from the snapshot without needing quorum. Without
	// this, NewRaft starts with an empty FSM and must wait for the leader
	// to re-send all log entries after quorum is re-established.
	if f := s.raft.Snapshot(); f.Error() != nil {
		// Best-effort: snapshot may fail if nothing was applied yet.
		_ = f.Error()
	}
	future := s.raft.Shutdown()
	err := future.Error()
	if cerr := s.boltDB.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// openRaftConfigStore creates a raft-backed config store with BoltDB persistence.
func openRaftConfigStore(opts raftStoreOpts) (config.Store, error) {
	multiNode := opts.ClusterSrv != nil

	raftDir := opts.Home.RaftDir()
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, fmt.Errorf("create raft directory: %w", err)
	}

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

	var transport hraft.Transport
	if multiNode {
		transport = opts.ClusterSrv.Transport()
	} else {
		_, transport = hraft.NewInmemTransport("")
	}

	fsm := raftfsm.New(opts.FSMOpts...)
	conf := newRaftConfig(opts.NodeID, multiNode)

	r, err := hraft.NewRaft(conf, fsm, boltStore, boltStore, snapStore, transport)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	if err := bootstrapAndWaitForLeader(r, boltStore, transport, opts, multiNode); err != nil {
		return nil, err
	}

	opts.Logger.Info("raft config store ready", "dir", raftDir, "multi_node", multiNode)

	store := raftstore.New(r, fsm, 10*time.Second)

	var fwd *cluster.Forwarder
	if multiNode {
		opts.ClusterSrv.SetRaft(r)
		opts.ClusterSrv.SetApplyFn(func(ctx context.Context, data []byte) error {
			return store.ApplyRaw(data)
		})
		fwd = cluster.NewForwarder(r, opts.ClusterTLS)
		store.SetForwarder(fwd)
	}

	return &raftConfigStore{
		Store:     store,
		raft:      r,
		boltDB:    boltStore,
		forwarder: fwd,
	}, nil
}

// newRaftConfig creates a hashicorp/raft config with appropriate timeouts.
func newRaftConfig(nodeID string, multiNode bool) *hraft.Config {
	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)
	conf.LogOutput = io.Discard

	// Lower snapshot threshold and interval from defaults (8192 entries,
	// 120s). A config store typically has ~20 entries, so the default
	// threshold would never be reached. Without a snapshot, NewRaft
	// cannot restore FSM state on restart — it only restores from
	// snapshots, not from log entries. This forces every multi-node
	// restart to wait for quorum + leader re-send.
	// The shutdown hook in Close() also takes an explicit snapshot, but
	// the periodic check acts as a safety net for ungraceful exits.
	conf.SnapshotThreshold = 4
	conf.SnapshotInterval = 30 * time.Second
	conf.TrailingLogs = 4

	if multiNode {
		conf.HeartbeatTimeout = 1000 * time.Millisecond
		conf.ElectionTimeout = 1000 * time.Millisecond
		conf.LeaderLeaseTimeout = 500 * time.Millisecond
	} else {
		conf.HeartbeatTimeout = 100 * time.Millisecond
		conf.ElectionTimeout = 100 * time.Millisecond
		conf.LeaderLeaseTimeout = 100 * time.Millisecond
	}
	return conf
}

// bootstrapAndWaitForLeader handles conditional Raft bootstrap and waits for
// leadership when required.
func bootstrapAndWaitForLeader(r *hraft.Raft, boltStore io.Closer, transport hraft.Transport, opts raftStoreOpts, multiNode bool) error {
	existing := r.GetConfiguration()
	if err := existing.Error(); err != nil {
		_ = r.Shutdown().Error()
		_ = boltStore.Close()
		return fmt.Errorf("get raft configuration: %w", err)
	}

	needsBootstrap := len(existing.Configuration().Servers) == 0
	shouldBootstrap := needsBootstrap && (!multiNode || opts.Init)

	if needsBootstrap && !shouldBootstrap {
		opts.Logger.Info("raft: no existing configuration; waiting to be added to a cluster")
	}

	if shouldBootstrap {
		boot := hraft.Configuration{
			Servers: []hraft.Server{
				{ID: hraft.ServerID(opts.NodeID), Address: transport.LocalAddr()},
			},
		}
		if err := r.BootstrapCluster(boot).Error(); err != nil {
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return fmt.Errorf("bootstrap raft: %w", err)
		}
		opts.Logger.Info("raft cluster bootstrapped", "node_id", opts.NodeID)
	}

	if !multiNode || opts.Init {
		select {
		case <-r.LeaderCh():
		case <-time.After(5 * time.Second):
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return errors.New("timed out waiting for raft leadership")
		}
	}

	return nil
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
			return fmt.Errorf(errFmtSaveClusterTLS, err)
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
		return fmt.Errorf(errFmtSaveClusterTLS, err)
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

// orchStatsAdapter bridges orchestrator methods to the cluster.StatsProvider
// interface. Lives at the composition root because it imports both packages.
type orchStatsAdapter struct {
	orch *orchestrator.Orchestrator
}

func (a *orchStatsAdapter) IngestQueueDepth() int    { return a.orch.IngestQueueDepth() }
func (a *orchStatsAdapter) IngestQueueCapacity() int { return a.orch.IngestQueueCapacity() }

func (a *orchStatsAdapter) VaultSnapshots() []cluster.StatsVaultSnapshot {
	snaps := a.orch.VaultSnapshots()
	out := make([]cluster.StatsVaultSnapshot, len(snaps))
	for i, s := range snaps {
		out[i] = cluster.StatsVaultSnapshot{
			ID:           s.ID.String(),
			RecordCount:  s.RecordCount,
			ChunkCount:   s.ChunkCount,
			SealedChunks: s.SealedChunks,
			DataBytes:    s.DataBytes,
			Enabled:      s.Enabled,
		}
	}
	return out
}

func (a *orchStatsAdapter) IngesterIDs() []string {
	ids := a.orch.ListIngesters()
	out := make([]string, len(ids))
	for i, id := range ids {
		out[i] = id.String()
	}
	return out
}

func (a *orchStatsAdapter) IngesterStats(id string) (name string, messages, bytes, errors int64, running bool) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return "", 0, 0, 0, false
	}
	s := a.orch.GetIngesterStats(uid)
	if s == nil {
		return "", 0, 0, 0, false
	}
	return a.orch.IngesterName(uid), s.MessagesIngested.Load(), s.BytesIngested.Load(), s.Errors.Load(), a.orch.IsIngesterRunning(uid)
}
