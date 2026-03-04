// Package app is the composition root for the gastrolog server. It wires
// all internal packages together and runs the service. The cmd/gastrolog
// binary is a thin CLI wrapper that delegates to [Run].
package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/cluster"
	"gastrolog/internal/config"
	configmem "gastrolog/internal/config/memory"
	"gastrolog/internal/config/raftfsm"
	digestlevel "gastrolog/internal/digester/level"
	digesttimestamp "gastrolog/internal/digester/timestamp"
	"gastrolog/internal/home"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/ingester/chatterbox"
	ingestdocker "gastrolog/internal/ingester/docker"
	ingestfluentfwd "gastrolog/internal/ingester/fluentfwd"
	ingesthttp "gastrolog/internal/ingester/http"
	ingestkafka "gastrolog/internal/ingester/kafka"
	ingestmqtt "gastrolog/internal/ingester/mqtt"
	ingestmetrics "gastrolog/internal/ingester/metrics"
	ingestotlp "gastrolog/internal/ingester/otlp"
	ingestrelp "gastrolog/internal/ingester/relp"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	ingesttail "gastrolog/internal/ingester/tail"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

// Version is set by the caller (typically from ldflags).
var Version = "dev"

// RunConfig groups all CLI flags for the server command.
type RunConfig struct {
	HomeFlag    string
	ConfigType  string
	ServerAddr  string
	Bootstrap   bool
	NoAuth      bool
	ClusterAddr string
	ClusterInit bool
	JoinAddr    string
	JoinToken   string
	Voteless    bool
}

// Run starts the gastrolog server. It wires all components, starts the
// orchestrator and HTTP server, and blocks until ctx is cancelled.
func Run(ctx context.Context, logger *slog.Logger, cfg RunConfig) error {
	if cfg.ClusterInit {
		logger.Warn("--cluster-init is deprecated; raft servers auto-bootstrap on first start")
	}

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

	configSignal := notify.NewSignal()
	disp := &configDispatcher{localNodeID: nodeID, logger: logger.With("component", "dispatch"), clusterTLS: clusterTLS, tlsFilePath: hd.ClusterTLSPath(), configSignal: configSignal}
	rawStore, err := openConfigStore(cfg.ConfigType, raftStoreOpts{
		Home: hd, NodeID: nodeID, Init: cfg.ClusterInit, JoinAddr: cfg.JoinAddr,
		ClusterSrv: clusterSrv, ClusterTLS: clusterTLS,
		Logger: logger, FSMOpts: []raftfsm.Option{raftfsm.WithOnApply(disp.Handle)},
	})
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}

	// Wrap in a proxy so runtime cluster join can swap the inner store.
	// All consumers hold a reference to proxy; on join, only the inner changes.
	proxy := config.NewStoreProxy(rawStore)
	defer func() { _ = proxy.Close() }()
	cfgStore := config.Store(proxy)

	if err := startClusterServices(ctx, clusterSrv, clusterTLS, cfgStore, hd, logger); err != nil {
		return err
	}
	if clusterSrv != nil {
		defer clusterSrv.Stop()
	}

	// Non-blocking: try local FSM, bootstrap, or return nil for replication cases.
	appCfg, fromLocalFSM, err := loadLocalConfig(ctx, logger, cfg, cfgStore, clusterTLS, nodeID)
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

	// Wire cross-node record forwarding and search forwarding in cluster mode.
	var searchForwarder *cluster.SearchForwarder
	if _, ok := rawStore.(*raftConfigStore); ok && clusterSrv != nil {
		searchForwarder = wireClusterForwarding(clusterSrv, orch, logger)
	}

	// Wire the dispatcher now that orchestrator and factories are available.
	disp.orch = orch
	disp.cfgStore = cfgStore
	disp.factories = factories

	if err := startOrchestrator(ctx, logger, orch, appCfg, factories); err != nil {
		return err
	}

	broadcaster, peerState, peerJobState, localStatsFn := setupClusterStats(ctx, logger, cfgStore, clusterSrv, orch, nodeID)

	// For replication cases: block until server settings replicate from the leader.
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

	// Build cluster operation callbacks (raft mode only).
	var joinClusterFn func(ctx context.Context, leaderAddr, joinToken string) error
	var removeNodeFn func(ctx context.Context, nodeID string) error
	var setNodeSuffrageFn func(ctx context.Context, nodeID string, voter bool) error
	if cfg.ConfigType == "raft" && clusterSrv != nil {
		joinClusterFn = makeJoinClusterFunc(proxy, clusterSrv, clusterTLS, hd, nodeID, cfg.ClusterAddr, orch, disp, logger)
		removeNodeFn = makeRemoveNodeFunc(clusterSrv, nodeID, logger)
		setNodeSuffrageFn = makeSetNodeSuffrageFunc(clusterSrv, nodeID, orch.Scheduler(), logger)

		// Register eviction handler: reinitialize as a fresh single-node cluster.
		clusterSrv.SetEvictionHandler(makeEvictionHandler(proxy, clusterSrv, clusterTLS, hd, nodeID, orch, disp, logger))
	}

	return serveAndAwaitShutdown(ctx, serverDeps{
		Logger:              logger,
		ServerAddr:          cfg.ServerAddr,
		HomeDir:             homeDir,
		NodeID:              nodeID,
		SocketPath:          socketPath,
		ClusterAddr:         cfg.ClusterAddr,
		Orch:                orch,
		CfgStore:            cfgStore,
		Factories:           factories,
		Tokens:              tokens,
		CertMgr:             certMgr,
		NoAuth:              cfg.NoAuth,
		AfterConfigApply:    nonRaftApplyHook(cfg.ConfigType, disp.Handle),
		ConfigSignal:        configSignal,
		ClusterSrv:          clusterSrv,
		Broadcaster:         broadcaster,
		PeerState:           peerState,
		PeerJobState:        peerJobState,
		LocalStats:          localStatsFn,
		SearchForwarder:     searchForwarder,
		JoinClusterFunc:     joinClusterFn,
		RemoveNodeFunc:      removeNodeFn,
		SetNodeSuffrageFunc: setNodeSuffrageFn,
	})
}

// wireClusterForwarding sets up cross-node record, search, context, and vault
// forwarding on the cluster server. Returns the search forwarder for the HTTP
// server to use.
func wireClusterForwarding(clusterSrv *cluster.Server, orch *orchestrator.Orchestrator, logger *slog.Logger) *cluster.SearchForwarder {
	peerConns := clusterSrv.PeerConns()

	recordForwarder := cluster.NewRecordForwarder(
		peerConns,
		logger.With("component", "record-forwarder"),
	)
	orch.SetRecordForwarder(recordForwarder)
	// NOTE: recordForwarder.Close() is not deferred here because the caller
	// manages shutdown order. The forwarder is closed when the orchestrator stops.

	clusterSrv.SetRecordAppender(func(ctx context.Context, vaultID uuid.UUID, rec chunk.Record) error {
		_, _, err := orch.Append(vaultID, rec)
		return err
	})

	searchForwarder := cluster.NewSearchForwarder(peerConns)
	clusterSrv.SetSearchExecutor(newSearchExecutor(orch))
	clusterSrv.SetContextExecutor(newContextExecutor(orch))
	clusterSrv.SetListChunksExecutor(newListChunksExecutor(orch))
	clusterSrv.SetGetIndexesExecutor(newGetIndexesExecutor(orch))
	clusterSrv.SetValidateVaultExecutor(newValidateVaultExecutor(orch))

	return searchForwarder
}

// nonRaftApplyHook returns the dispatcher callback for non-raft config stores.
func nonRaftApplyHook(configType string, handle func(raftfsm.Notification)) func(raftfsm.Notification) {
	if configType != "raft" {
		return handle
	}
	return nil
}

// startOrchestrator applies config, rebuilds missing indexes, and starts the orchestrator.
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
	if err := orch.Start(ctx); err != nil {
		return err
	}
	logger.Info("orchestrator started")
	return nil
}

// setupClusterStats creates the broadcaster, peer state tracker, and stats
// collector. Returns nils for single-node mode.
func setupClusterStats(ctx context.Context, logger *slog.Logger, cfgStore config.Store, clusterSrv *cluster.Server, orch *orchestrator.Orchestrator, nodeID string) (*cluster.Broadcaster, *cluster.PeerState, *cluster.PeerJobState, func() *gastrologv1.NodeStats) {
	var broadcaster *cluster.Broadcaster
	if clusterSrv != nil && clusterSrv.PeerConns() != nil {
		broadcaster = cluster.NewBroadcaster(clusterSrv.PeerConns(), logger.With("component", "broadcast"))
	}
	if broadcaster == nil || clusterSrv == nil {
		return nil, nil, nil, nil
	}

	var broadcastInterval time.Duration
	if ss, err := cfgStore.LoadServerSettings(ctx); err == nil && ss.Cluster.BroadcastInterval != "" {
		if d, err := time.ParseDuration(ss.Cluster.BroadcastInterval); err == nil {
			broadcastInterval = d
		}
	}

	peerState := cluster.NewPeerState(15 * time.Second)
	clusterSrv.Subscribe(peerState.HandleBroadcast)

	peerJobState := cluster.NewPeerJobState(15 * time.Second)
	clusterSrv.Subscribe(peerJobState.HandleBroadcast)

	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{
		Broadcaster: broadcaster,
		RaftStats:   clusterSrv,
		Stats:       &orchStatsAdapter{orch: orch},
		Jobs:        &jobBroadcastAdapter{scheduler: orch.Scheduler(), nodeID: nodeID},
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
		Version:   Version,
		StartTime: time.Now(),
		Interval:  broadcastInterval,
		Logger:    logger.With("component", "stats-collector"),
	})

	orch.Scheduler().SetOnJobChange(func() {
		go collector.BroadcastJobs(ctx)
	})

	go collector.Run(ctx)

	return broadcaster, peerState, peerJobState, collector.CollectLocal
}

// resolveIdentity ensures the home directory exists and resolves the node ID.
func resolveIdentity(logger *slog.Logger, cfg RunConfig, hd home.Dir) (string, error) {
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

// loadLocalConfig attempts to load config from the local FSM or bootstrap.
func loadLocalConfig(ctx context.Context, logger *slog.Logger, cfg RunConfig, cfgStore config.Store, clusterTLS *cluster.ClusterTLS, nodeID string) (*config.Config, bool, error) {
	if err := requestClusterMembership(ctx, logger, cfg, clusterTLS, nodeID); err != nil {
		return nil, false, err
	}

	if cfg.JoinAddr != "" {
		logger.Info("joining cluster, config will replicate from leader")
		return nil, false, nil
	}

	if cfg.ConfigType == "raft" {
		localCfg, _ := cfgStore.Load(ctx)
		ss, _ := cfgStore.LoadServerSettings(ctx)
		if localCfg != nil && ss.Auth.JWTSecret != "" {
			return localCfg, true, nil
		}
	}

	logger.Info("loading config", "type", cfg.ConfigType)
	appCfg, err := ensureConfig(ctx, logger, cfgStore, cfg.Bootstrap)
	if err != nil {
		return nil, false, err
	}
	return appCfg, false, nil
}

// requestClusterMembership asks the cluster leader to add this node as a Raft
// voter or nonvoter. No-op if join parameters are not set.
func requestClusterMembership(ctx context.Context, logger *slog.Logger, cfg RunConfig, clusterTLS *cluster.ClusterTLS, nodeID string) error {
	if cfg.JoinAddr == "" || clusterTLS == nil || cfg.ClusterAddr == "" {
		return nil
	}
	voter := !cfg.Voteless
	kind := "voter"
	if !voter {
		kind = "nonvoter"
	}
	logger.Info("requesting "+kind+" membership from leader", "leader_addr", cfg.JoinAddr)
	joinCtx, joinCancel := context.WithTimeout(ctx, 30*time.Second)
	defer joinCancel()
	if err := cluster.JoinCluster(joinCtx, cfg.JoinAddr, nodeID, cfg.ClusterAddr, clusterTLS, voter); err != nil {
		return fmt.Errorf("join cluster: %w", err)
	}
	logger.Info(kind + " membership granted by leader")
	return nil
}

// finalizeNodeSetup ensures this node has a NodeConfig with a petname and
// resolves the home directory and socket path.
func finalizeNodeSetup(ctx context.Context, logger *slog.Logger, cfgStore config.Store, nodeID, configType string, asyncNodeConfig bool, hd home.Dir) (string, string, error) {
	if asyncNodeConfig {
		logNodeIdentity(logger, nodeID, hd.ReadNodeName())
		go ensureNodeConfigAsync(ctx, cfgStore, nodeID, configType, hd, logger)
	} else {
		nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID)
		if err != nil {
			return "", "", fmt.Errorf("ensure node config: %w", err)
		}
		logNodeIdentity(logger, nodeID, nodeName)
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

func logNodeIdentity(logger *slog.Logger, nodeID, nodeName string) {
	if nodeName != "" {
		logger.Info("node identity", "node_id", nodeID, "node_name", nodeName)
	} else {
		logger.Info("node identity", "node_id", nodeID)
	}
}

// awaitReplication blocks until server settings replicate from the leader.
// No-op when config was loaded locally.
func awaitReplication(ctx context.Context, appCfg *config.Config, configType string, cfgStore config.Store, logger *slog.Logger) error {
	if appCfg != nil || configType != "raft" {
		return nil
	}
	return waitForServerSettings(ctx, cfgStore, 60*time.Second, logger)
}

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

func waitForQuorum(ctx context.Context, cfgStore config.Store, logger *slog.Logger) error {
	inner := cfgStore
	if p, ok := cfgStore.(*config.StoreProxy); ok {
		inner = p.Inner()
	}
	rcs, ok := inner.(*raftConfigStore)
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

func ensureNodeConfigAsync(ctx context.Context, cfgStore config.Store, nodeID, configType string, hd home.Dir, logger *slog.Logger) {
	if err := waitForQuorum(ctx, cfgStore, logger); err != nil {
		return
	}
	nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID)
	if err != nil {
		logger.Warn("ensure node config failed (will retry on next start)", "error", err)
		return
	}
	if configType != "memory" {
		_ = hd.WriteNodeName(nodeName)
	}
}

func ensureConfig(ctx context.Context, logger *slog.Logger, cfgStore config.Store, bootstrap bool) (*config.Config, error) {
	cfg, err := cfgStore.Load(ctx)
	if err != nil {
		return nil, err
	}

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

// serverDeps bundles the dependencies needed to start the HTTP server.
type serverDeps struct {
	Logger              *slog.Logger
	ServerAddr          string
	HomeDir             string
	NodeID              string
	SocketPath          string
	ClusterAddr         string
	Orch                *orchestrator.Orchestrator
	CfgStore            config.Store
	Factories           orchestrator.Factories
	Tokens              *auth.TokenService
	CertMgr             *cert.Manager
	NoAuth              bool
	AfterConfigApply    func(raftfsm.Notification)
	ConfigSignal        *notify.Signal
	ClusterSrv          *cluster.Server
	Broadcaster         *cluster.Broadcaster
	PeerState           *cluster.PeerState
	PeerJobState        *cluster.PeerJobState
	LocalStats          func() *gastrologv1.NodeStats
	SearchForwarder     *cluster.SearchForwarder
	JoinClusterFunc     func(ctx context.Context, leaderAddr, joinToken string) error
	RemoveNodeFunc      func(ctx context.Context, nodeID string) error
	SetNodeSuffrageFunc func(ctx context.Context, nodeID string, voter bool) error
}

func serveAndAwaitShutdown(ctx context.Context, deps serverDeps) error {
	var srv *server.Server
	var serverWg sync.WaitGroup
	if deps.ServerAddr != "" {
		srv = server.New(deps.Orch, deps.CfgStore, deps.Factories, deps.Tokens, server.Config{
			Logger: deps.Logger, CertManager: deps.CertMgr, NoAuth: deps.NoAuth,
			HomeDir: deps.HomeDir, NodeID: deps.NodeID, UnixSocket: deps.SocketPath,
			AfterConfigApply: deps.AfterConfigApply, ConfigSignal: deps.ConfigSignal,
			Cluster: deps.ClusterSrv, PeerStats: deps.PeerState,
			PeerVaultStats: deps.PeerState, PeerJobs: deps.PeerJobState,
			LocalStats: deps.LocalStats, RemoteSearcher: deps.SearchForwarder,
			RemoteVaultForwarder: deps.SearchForwarder, ClusterAddress: deps.ClusterAddr,
			JoinClusterFunc: deps.JoinClusterFunc, RemoveNodeFunc: deps.RemoveNodeFunc,
			SetNodeSuffrageFunc: deps.SetNodeSuffrageFunc,
		})
		serverWg.Go(func() {
			if err := srv.ServeTCP(deps.ServerAddr); err != nil {
				deps.Logger.Error("server error", "error", err)
			}
		})
	}

	<-ctx.Done()

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

func buildFactories(logger *slog.Logger, homeDir string, cfgStore config.Store, orch *orchestrator.Orchestrator) orchestrator.Factories {
	return orchestrator.Factories{
		Ingesters: map[string]orchestrator.IngesterFactory{
			"chatterbox": chatterbox.NewIngester,
			"docker":     ingestdocker.NewFactory(cfgStore),
			"fluentfwd":  ingestfluentfwd.NewFactory(),
			"http":       ingesthttp.NewFactory(),
			"kafka":      ingestkafka.NewFactory(),
			"mqtt":       ingestmqtt.NewFactory(),
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
			"mqtt":       ingestmqtt.ParamDefaults,
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

func resolveHome(flagValue string) (home.Dir, error) {
	if flagValue != "" {
		return home.New(flagValue), nil
	}
	return home.Default()
}

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

// openConfigStore creates a config.Store based on config type.
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
