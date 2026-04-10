// Package app is the composition root for the gastrolog server. It wires
// all internal packages together and runs the service. The cmd/gastrolog
// binary is a thin CLI wrapper that delegates to [Run].
package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/google/uuid"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"
	"gastrolog/internal/auth"
	"gastrolog/internal/cert"
	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	chunkfile "gastrolog/internal/chunk/file"
	chunkjsonl "gastrolog/internal/chunk/jsonl"
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
	"gastrolog/internal/ingester/scatterbox"
	ingestself "gastrolog/internal/ingester/self"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	ingesttail "gastrolog/internal/ingester/tail"
	"gastrolog/internal/chanwatch"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/logging"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/server/routing"
)

// Version is set by the caller (typically from ldflags).
var Version = "dev"

// RunConfig groups all CLI flags for the server command.
type RunConfig struct {
	HomeFlag    string
	VaultsFlag  string
	ConfigType  string
	ServerAddr  string
	NoAuth      bool
	ClusterAddr string
	JoinAddr    string
	JoinToken   string
	Voteless    bool
	NodeName    string

	// PprofAddr is the pprof HTTP server address (e.g. "localhost:6060").
	// Empty if pprof is disabled. Advertised to cluster peers via broadcast.
	PprofAddr string

	// SlogCapture receives copies of slog records for the "self" ingester.
	// Created by main and shared with the CaptureHandler. Nil disables capture.
	SlogCapture <-chan logging.CapturedRecord

	// SlogCaptureHandler is the CaptureHandler that tees slog records.
	// Passed to the self ingester factory so it can apply the min_level param.
	SlogCaptureHandler *logging.CaptureHandler
}

// Run starts the gastrolog server. It wires all components, starts the
// orchestrator and HTTP server, and blocks until ctx is cancelled.
func Run(ctx context.Context, logger *slog.Logger, cfg RunConfig) error {
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
	statsSignal := notify.NewSignal()
	disp := &configDispatcher{localNodeID: nodeID, logger: logger.With("component", "dispatch"), clusterTLS: clusterTLS, tlsFilePath: hd.ClusterTLSPath(), configSignal: configSignal}
	rawStore, err := openConfigStore(cfg.ConfigType, raftStoreOpts{
		Home: hd, NodeID: nodeID, JoinAddr: cfg.JoinAddr,
		ClusterSrv: clusterSrv, ClusterTLS: clusterTLS,
		Logger: logger, FSMOpts: []raftfsm.Option{raftfsm.WithOnApply(disp.Handle)},
	})
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}

	// Wrap in a proxy so runtime cluster join can swap the inner store.
	// All consumers hold a reference to proxy; on join, only the inner changes.
	proxy := config.NewStoreProxy(rawStore)
	cfgStore := config.Store(proxy)
	var groupMgr *raftgroup.GroupManager // set later if cluster mode

	if err := startClusterServices(ctx, clusterSrv, clusterTLS, cfgStore, hd, logger); err != nil {
		_ = proxy.Close()
		return err
	}
	// Shutdown order matters: config Raft must stop BEFORE the cluster
	// server, because the Raft follower reads from the transport's rpcChan.
	// Closing the transport first causes a nil-channel deadlock in Raft.
	// Defers run LIFO, so cluster Stop is registered first (runs last).
	if clusterSrv != nil {
		defer clusterSrv.Stop()
	}
	defer func() { _ = proxy.Close() }()

	// Non-blocking: try local FSM, bootstrap, or return nil for replication cases.
	appCfg, fromLocalFSM, err := loadLocalConfig(ctx, logger, cfg, cfgStore, clusterTLS, nodeID)
	if err != nil {
		return err
	}

	asyncNodeConfig := fromLocalFSM || appCfg == nil
	homeDir, socketPath, err := finalizeNodeSetup(ctx, logger, cfgStore, nodeID, cfg.ConfigType, cfg.NodeName, asyncNodeConfig, hd)
	if err != nil {
		return err
	}

	alertCollector := alert.New()

	// Shared shutdown phase. Constructed once per process and threaded into
	// every subsystem that needs to short-circuit work during drain — the
	// orchestrator's replication fanout, the cluster server's stream
	// handlers, the tier announcer, etc. See gastrolog-1e5ke.
	shutdownPhase := lifecycle.New()

	orch, err := orchestrator.New(orchestrator.Config{
		Logger:            logger,
		MaxConcurrentJobs: loadMaxConcurrentJobs(ctx, cfgStore),
		ConfigLoader:      cfgStore,
		LocalNodeID:       nodeID,
		Alerts:            alertCollector,
		Phase:             shutdownPhase,
	})
	if err != nil {
		return fmt.Errorf("create orchestrator: %w", err)
	}
	orch.RegisterDigester(digestlevel.New())
	orch.RegisterDigester(digesttimestamp.New())

	vaultsDir := cfg.VaultsFlag
	if vaultsDir == "" {
		vaultsDir = homeDir // default: vaults resolve relative to home
	}

	certMgr, err := loadCertManager(ctx, logger, cfgStore)
	if err != nil {
		return err
	}

	groupMgr, nodeAddrResolver := setupMultiRaft(clusterSrv, rawStore, nodeID, homeDir, logger)

	factories := buildFactories(logger, homeDir, vaultsDir, cfgStore, orch, certMgr, cfg.SlogCapture, cfg.SlogCaptureHandler, groupMgr, nodeAddrResolver)
	if clusterSrv != nil {
		factories.PeerConns = clusterSrv.PeerConns()
	}

	// Wire cross-node record forwarding and search forwarding in cluster mode.
	// orchReady is closed after startOrchestrator completes so that forwarded
	// records block (instead of failing) while vaults are being registered.
	orchReady := make(chan struct{})
	var searchForwarder *cluster.SearchForwarder
	var recordForwarder *cluster.RecordForwarder
	var routingForwarder *routing.Forwarder
	if _, ok := rawStore.(*raftConfigStore); ok && clusterSrv != nil {
		searchForwarder, recordForwarder = wireClusterForwarding(clusterSrv, orch, orchReady, nodeID, logger, alertCollector)
		routingForwarder = routing.NewForwarder(clusterSrv.PeerConns())
	}

	// Wire the dispatcher now that orchestrator and factories are available.
	disp.orch = orch
	disp.cfgStore = cfgStore
	disp.factories = factories
	disp.catchupScheduler = func(tierID uuid.UUID, followerNodeIDs []string) {
		orch.ScheduleCatchupForTier(tierID, followerNodeIDs)
	}

	orch.OnTierDrainComplete = makeTierDrainCompleteHandler(cfgStore, logger, factories)

	if err := startOrchestrator(ctx, logger, orch, appCfg, factories); err != nil {
		return err
	}
	close(orchReady)

	// Wire the ForwardTierApply handler so other nodes can forward tier
	// Raft applies to us when we're the tier Raft leader.
	if clusterSrv != nil && groupMgr != nil {
		clusterSrv.SetTierApplyFn(func(ctx context.Context, groupID string, data []byte) error {
			g := groupMgr.GetGroup(groupID)
			if g == nil {
				return fmt.Errorf("tier raft group %s not found", groupID)
			}
			return g.Raft.Apply(data, cluster.ReplicationTimeout).Error()
		})
	}

	// Tier Raft group membership is reconciled by per-tier leader loops
	// (raftgroup.LeaderLoop) wired by reconfig_vaults.go. On snapshot
	// restore the loops fire as soon as elections complete and reconcile
	// from inside the leader epoch.

	// Monitor slog capture channel pressure.
	if cfg.SlogCapture != nil {
		slogCW := chanwatch.New(logger, 1*time.Second)
		slogCW.SetAlerts(alertCollector)
		slogCW.Watch("slogCaptureCh", func() (int, int) {
			return len(cfg.SlogCapture), cap(cfg.SlogCapture)
		}, 0.9)
		go slogCW.Run(ctx)
	}

	broadcaster, peerState, peerJobState, localStatsFn := setupClusterStats(ctx, logger, cfgStore, clusterSrv, orch, recordForwarder, alertCollector, nodeID, cfg.ServerAddr, cfg.PprofAddr, statsSignal)

	// Start tier placement manager (cluster mode only).
	var placementReconcileFn func(ctx context.Context)
	if clusterSrv != nil && peerState != nil {
		pm := &placementManager{
			cfgStore:    cfgStore,
			clusterSrv:  clusterSrv,
			peerState:   peerState,
			alerts:      alertCollector,
			localNodeID: nodeID,
			logger:      logger.With("component", "placement"),
			triggerCh:   make(chan struct{}, 1),
		}
		disp.placementTrigger = pm.Trigger
		placementReconcileFn = pm.Reconcile
		go pm.Run(ctx)
	}

	// For replication cases: block until server settings replicate from the leader.
	if err := awaitReplication(ctx, appCfg, cfg.ConfigType, cfgStore, logger); err != nil {
		return err
	}

	tokens, err := buildAuthTokens(ctx, logger, cfgStore, cfg.NoAuth)
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
		StatsSignal:         statsSignal,
		ClusterSrv:          clusterSrv,
		Broadcaster:         broadcaster,
		PeerState:           peerState,
		PeerJobState:        peerJobState,
		LocalStats:          localStatsFn,
		SearchForwarder:     searchForwarder,
		RoutingForwarder:    routingForwarder,
		JoinClusterFunc:     joinClusterFn,
		RemoveNodeFunc:      removeNodeFn,
		SetNodeSuffrageFunc: setNodeSuffrageFn,
		Dispatcher:          disp,
		GroupMgr:            groupMgr,
		ConfigStore:         proxy,
		PlacementReconcile:  placementReconcileFn,
	})
}

// makeTierDrainCompleteHandler returns a callback that deletes the drained tier
// config (removing its vault association) and destroys the tier's Raft group.
func makeTierDrainCompleteHandler(cfgStore config.Store, logger *slog.Logger, factories orchestrator.Factories) func(context.Context, uuid.UUID, uuid.UUID) {
	return func(ctx context.Context, _, tierID uuid.UUID) {
		// Tier ownership lives on TierConfig.VaultID — deleting the tier
		// config removes the association. The drain=false flag avoids
		// re-triggering a drain notification.
		if err := cfgStore.DeleteTier(ctx, tierID, false); err != nil {
			logger.Error("tier drain complete: failed to delete tier config",
				"tier", tierID, "error", err)
		}
		// Destroy the tier's Raft group now that the drain is done.
		if factories.GroupManager != nil {
			if err := factories.GroupManager.DestroyGroup(tierID.String()); err != nil {
				logger.Debug("tier drain complete: destroy tier raft group", "tier", tierID, "error", err)
			}
		}
	}
}

// wireClusterForwarding sets up cross-node record, search, context, vault,
// and explain forwarding on the cluster server. Returns the search forwarder
// for the HTTP server to use.
func wireClusterForwarding(clusterSrv *cluster.Server, orch *orchestrator.Orchestrator, orchReady <-chan struct{}, nodeID string, logger *slog.Logger, alerts *alert.Collector) (*cluster.SearchForwarder, *cluster.RecordForwarder) {
	peerConns := clusterSrv.PeerConns()

	recordForwarder := cluster.NewRecordForwarder(
		peerConns,
		logger.With("component", "record-forwarder"),
		alerts,
	)
	orch.SetRecordForwarder(recordForwarder)
	// NOTE: recordForwarder.Close() is not deferred here because the caller
	// manages shutdown order. The forwarder is closed when the orchestrator stops.

	// The record appender waits for the orchestrator to be ready (vaults
	// registered) before writing. Without this gate, forwarded records
	// arriving during startup hit ErrVaultNotFound, causing the sending
	// node's forwarder to enter exponential backoff and silently buffer
	// records for up to 2 minutes.
	var gateLogOnce sync.Once
	waitForOrch := func(ctx context.Context) error {
		select {
		case <-orchReady:
			return nil
		default:
		}
		gateLogOnce.Do(func() {
			logger.Info("forwarded record waiting for orchestrator startup")
		})
		select {
		case <-orchReady:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	clusterSrv.SetRecordAppender(func(ctx context.Context, vaultID uuid.UUID, rec chunk.Record) error {
		if err := waitForOrch(ctx); err != nil {
			return err
		}
		_, _, err := orch.Append(vaultID, rec)
		return err
	})
	clusterSrv.SetRecordTierAppender(func(ctx context.Context, vaultID, tierID uuid.UUID, primaryChunkID chunk.ChunkID, rec chunk.Record) error {
		if err := waitForOrch(ctx); err != nil {
			return err
		}
		return orch.AppendToTier(vaultID, tierID, primaryChunkID, rec)
	})

	// Wire cross-node chunk migration and replication.
	chunkTransferrer := cluster.NewChunkTransferrer(peerConns)
	orch.SetRemoteTransferrer(chunkTransferrer)

	// Tier replication: unified ordered stream per tier per follower.
	tierReplicator := cluster.NewTierReplicator(peerConns, logger.With("component", "tier-replicator"))
	orch.SetTierReplicator(tierReplicator)

	// Same readiness gate for bulk chunk imports.
	clusterSrv.SetRecordImporter(func(ctx context.Context, vaultID uuid.UUID, next chunk.RecordIterator) error {
		if err := waitForOrch(ctx); err != nil {
			return err
		}
		return orch.ImportChunkRecords(ctx, vaultID, next)
	})
	clusterSrv.SetTierRecordImporter(func(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
		if err := waitForOrch(ctx); err != nil {
			return err
		}
		return orch.ImportToTier(ctx, vaultID, tierID, chunkID, next)
	})
	clusterSrv.SetTierStreamAppender(func(ctx context.Context, vaultID, tierID uuid.UUID, next chunk.RecordIterator) error {
		if err := waitForOrch(ctx); err != nil {
			return err
		}
		return orch.StreamAppendToTier(ctx, vaultID, tierID, next)
	})

	searchForwarder := cluster.NewSearchForwarder(peerConns)
	clusterSrv.SetSearchExecutor(newSearchExecutor(orch))
	clusterSrv.SetContextExecutor(newContextExecutor(orch))
	clusterSrv.SetListChunksExecutor(newListChunksExecutor(orch))
	clusterSrv.SetGetIndexesExecutor(newGetIndexesExecutor(orch))
	clusterSrv.SetValidateVaultExecutor(newValidateVaultExecutor(orch))
	clusterSrv.SetGetChunkExecutor(newGetChunkExecutor(orch))
	clusterSrv.SetAnalyzeChunkExecutor(newAnalyzeChunkExecutor(orch))
	clusterSrv.SetSealVaultExecutor(newSealVaultExecutor(orch))
	clusterSrv.SetSealTierExecutor(func(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error {
		return orch.SealActiveTier(vaultID, tierID, chunkID)
	})
	clusterSrv.SetDeleteChunkExecutor(func(ctx context.Context, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error {
		return orch.DeleteChunkFromTier(vaultID, tierID, chunkID)
	})
	clusterSrv.SetReindexVaultExecutor(newReindexVaultExecutor(orch))
	clusterSrv.SetExplainExecutor(newExplainExecutor(orch, nodeID))
	clusterSrv.SetFollowExecutor(newFollowExecutor(orch))

	return searchForwarder, recordForwarder
}

// wireManagedFileTransfer sets up cluster-side handlers for streaming managed
// files between nodes and returns a managedFileManager for the dispatcher.
func wireManagedFileTransfer(clusterSrv *cluster.Server, httpSrv *server.Server, cfgStore config.Store, homeDir string, logger *slog.Logger) *managedFileManager {
	peerConns := clusterSrv.PeerConns()
	clusterSrv.SetManagedFileReader(httpSrv.ManagedFileReader)
	clusterSrv.SetManagedFileIDs(httpSrv.ManagedFileIDs)

	transferrer := cluster.NewManagedFileTransferrer(peerConns)
	return &managedFileManager{
		homeDir:     homeDir,
		cfgStore:    cfgStore,
		transferrer: transferrer,
		peerIDs:     peerConns.PeerIDs,
		fileExists:  httpSrv.ManagedFileExists,
		logger:      logger.With("component", "managed-files"),
	}
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
func setupClusterStats(ctx context.Context, logger *slog.Logger, cfgStore config.Store, clusterSrv *cluster.Server, orch *orchestrator.Orchestrator, recordForwarder *cluster.RecordForwarder, alerts *alert.Collector, nodeID string, apiAddr string, pprofAddr string, statsSignal *notify.Signal) (*cluster.Broadcaster, *cluster.PeerState, *cluster.PeerJobState, func() *gastrologv1.NodeStats) {
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
		Forwarding:  &forwardingStatsAdapter{srv: clusterSrv, fwd: recordForwarder},
		Alerts:      alerts,
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
		Version:      Version,
		StartTime:    time.Now(),
		Interval:     broadcastInterval,
		ApiAddress:   apiAddr,
		PprofAddress: pprofAddr,
		StatsSignal:  statsSignal,
		Logger:       logger.With("component", "stats-collector"),
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
		// Wait for a leader AND for the local FSM to catch up to the cluster's
		// latest committed state before reading anything from it. hraft's
		// NewRaft returns with the FSM at the snapshot level; post-snapshot
		// committed entries (tier placements, NSCs, etc.) only become visible
		// after either a Barrier on the leader or a few AppendEntries rounds
		// on a follower. Without this wait, the orchestrator reads stale
		// state and creates tier Raft groups with incomplete member lists.
		if err := waitForQuorum(ctx, cfgStore, logger); err != nil {
			return nil, false, err
		}
		if err := waitForFSMCatchup(ctx, cfgStore, 10*time.Second, logger); err != nil {
			return nil, false, err
		}
		localCfg, _ := cfgStore.Load(ctx)
		ss, _ := cfgStore.LoadServerSettings(ctx)
		if localCfg != nil && ss.Auth.JWTSecret != "" {
			return localCfg, true, nil
		}
	}

	logger.Info("loading config", "type", cfg.ConfigType)
	appCfg, err := ensureConfig(ctx, logger, cfgStore)
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

// finalizeNodeSetup ensures this node has a NodeConfig with a name and
// resolves the home directory and socket path. If preferredName is set, it
// is used instead of generating a random petname.
func finalizeNodeSetup(ctx context.Context, logger *slog.Logger, cfgStore config.Store, nodeID, configType, preferredName string, asyncNodeConfig bool, hd home.Dir) (string, string, error) {
	if asyncNodeConfig {
		logNodeIdentity(logger, nodeID, hd.ReadNodeName())
		go ensureNodeConfigAsync(ctx, cfgStore, nodeID, configType, preferredName, hd, logger)
	} else {
		nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID, preferredName)
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

func ensureNodeConfig(ctx context.Context, cfgStore config.Store, nodeID, preferredName string) (string, error) {
	nodeUUID, err := uuid.Parse(nodeID)
	if err != nil {
		return "", fmt.Errorf("parse node ID %q: %w", nodeID, err)
	}
	existing, err := cfgStore.GetNode(ctx, nodeUUID)
	if err != nil {
		return "", fmt.Errorf("get node: %w", err)
	}
	if existing != nil {
		// If a preferred name was given and differs from the stored name, update it.
		if preferredName != "" && existing.Name != preferredName {
			existing.Name = preferredName
			if err := cfgStore.PutNode(ctx, *existing); err != nil {
				return "", err
			}
			return preferredName, nil
		}
		return existing.Name, nil
	}
	name := preferredName
	if name == "" {
		name = petname.Generate(2, "-")
	}
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

// waitForFSMCatchup blocks until the local config FSM reflects the cluster's
// committed state. No-op for non-raft stores.
func waitForFSMCatchup(ctx context.Context, cfgStore config.Store, timeout time.Duration, logger *slog.Logger) error {
	inner := cfgStore
	if p, ok := cfgStore.(*config.StoreProxy); ok {
		inner = p.Inner()
	}
	rcs, ok := inner.(*raftConfigStore)
	if !ok {
		return nil
	}
	logger.Info("waiting for config FSM to catch up to committed state")
	if err := rcs.WaitForFSMCatchup(ctx, timeout, logger); err != nil {
		return fmt.Errorf("wait for FSM catchup: %w", err)
	}
	logger.Info("config FSM caught up")
	return nil
}

func ensureNodeConfigAsync(ctx context.Context, cfgStore config.Store, nodeID, configType, preferredName string, hd home.Dir, logger *slog.Logger) {
	if err := waitForQuorum(ctx, cfgStore, logger); err != nil {
		return
	}
	nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID, preferredName)
	if err != nil {
		logger.Warn("ensure node config failed (will retry on next start)", "error", err)
		return
	}
	if configType != "memory" {
		_ = hd.WriteNodeName(nodeName)
	}
}

func ensureConfig(ctx context.Context, logger *slog.Logger, cfgStore config.Store) (*config.Config, error) {
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

	if ss.Auth.JWTSecret == "" {
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
	StatsSignal         *notify.Signal
	ClusterSrv          *cluster.Server
	Broadcaster         *cluster.Broadcaster
	PeerState           *cluster.PeerState
	PeerJobState        *cluster.PeerJobState
	LocalStats          func() *gastrologv1.NodeStats
	SearchForwarder     *cluster.SearchForwarder
	RoutingForwarder    routing.UnaryForwarder
	JoinClusterFunc     func(ctx context.Context, leaderAddr, joinToken string) error
	RemoveNodeFunc      func(ctx context.Context, nodeID string) error
	SetNodeSuffrageFunc func(ctx context.Context, nodeID string, voter bool) error
	Dispatcher          *configDispatcher
	GroupMgr            *raftgroup.GroupManager
	ConfigStore         io.Closer // rawStore — closed before gRPC for clean Raft shutdown
	PlacementReconcile  func(ctx context.Context)
}

func serveAndAwaitShutdown(ctx context.Context, deps serverDeps) error {
	var srv *server.Server
	var serverWg sync.WaitGroup
	if deps.ServerAddr != "" {
		srv = server.New(deps.Orch, deps.CfgStore, deps.Factories, deps.Tokens, server.Config{
			Logger: deps.Logger, CertManager: deps.CertMgr, NoAuth: deps.NoAuth,
			HomeDir: deps.HomeDir, NodeID: deps.NodeID, UnixSocket: deps.SocketPath,
			AfterConfigApply: deps.AfterConfigApply, ConfigSignal: deps.ConfigSignal, StatsSignal: deps.StatsSignal,
			Cluster: deps.ClusterSrv, PeerStats: deps.PeerState,
			PeerVaultStats: deps.PeerState, PeerIngesterStats: deps.PeerState, PeerRouteStats: deps.PeerState,
			PeerJobs: deps.PeerJobState,
			LocalStats: deps.LocalStats, RemoteSearcher: deps.SearchForwarder, RemoteChunkLister: deps.SearchForwarder,
			RoutingForwarder: deps.RoutingForwarder, ClusterAddress: deps.ClusterAddr,
			JoinClusterFunc: deps.JoinClusterFunc, RemoveNodeFunc: deps.RemoveNodeFunc,
			SetNodeSuffrageFunc: deps.SetNodeSuffrageFunc,
			CloudTesters: map[string]server.CloudServiceTester{
				"file": chunkcloud.NewConnectionTester(),
			},
			PlacementReconcile: deps.PlacementReconcile,
		})
		// Provide the cluster's ForwardRPC handler with the internal mux.
		// NoAuthInterceptor + no routing interceptor prevents loops.
		if deps.ClusterSrv != nil {
			deps.ClusterSrv.SetInternalHandler(srv.BuildInternalHandler())
		}

		// Wire managed file transfer handlers on the cluster server. The HTTP
		// server owns the managed files on disk; the cluster server streams them
		// to peers. Must happen after server creation but before serving starts.
		if deps.ClusterSrv != nil && deps.Dispatcher != nil {
			mgr := wireManagedFileTransfer(deps.ClusterSrv, srv, deps.CfgStore, deps.HomeDir, deps.Logger)
			deps.Dispatcher.managedFileHandler = mgr

			// Wire on-demand repair: when the server resolves a manifest
			// entry but the file is missing from disk, it calls this to
			// pull the file from a peer before returning "not found".
			srv.SetManagedFileRepair(mgr.RepairFile)

			// Wire export-to-vault executor so remote nodes can forward
			// export jobs to the node that owns the target vault.
			deps.ClusterSrv.SetExportToVaultExecutor(srv.ExportToVaultFunc())

			// Startup reconciliation with backoff, then periodic drift check.
			go func() {
				reconcileManagedFilesStartup(ctx, mgr)
				mgr.RunPeriodicReconciliation(ctx)
			}()
		}

		serverWg.Go(func() {
			if err := srv.ServeTCP(deps.ServerAddr); err != nil {
				deps.Logger.Error("server error", "error", err)
			}
		})
	}

	<-ctx.Done()

	if srv != nil {
		deps.Logger.Info("stopping server")
		// The root ctx is already cancelled by the time we get here (that
		// is how we woke up). Pass srv.Stop a FRESH context with a bounded
		// drain budget so it can finish in-flight HTTP requests cleanly
		// instead of returning context.Canceled immediately. See
		// gastrolog-1e5ke.
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := srv.Stop(stopCtx)
		stopCancel()
		// context.Canceled / DeadlineExceeded are expected outcomes when a
		// peer holds a long-running request across shutdown — logged at
		// Debug, not Error, so the shutdown trail stays clean.
		if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
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

	// Shutdown order: tier Raft → config Raft → gRPC server.
	// Raft must shut down WHILE the transport is alive, otherwise the
	// leader's replication goroutines block on dead gRPC connections.
	if deps.GroupMgr != nil {
		deps.Logger.Info("shutting down tier raft groups")
		deps.GroupMgr.Shutdown()
	}

	if deps.ConfigStore != nil {
		deps.Logger.Info("shutting down config raft")
		_ = deps.ConfigStore.Close()
	}

	if deps.ClusterSrv != nil {
		deps.Logger.Info("stopping cluster server")
		deps.ClusterSrv.Stop()
	}

	deps.Logger.Info("shutdown complete")
	return nil
}

// setupMultiRaft creates the GroupManager and node address resolver for tier
// Raft groups. Returns (nil, nil) in single-node / non-raft mode.
func setupMultiRaft(clusterSrv *cluster.Server, rawStore config.Store, nodeID, homeDir string, logger *slog.Logger) (*raftgroup.GroupManager, func(string) (string, bool)) {
	if clusterSrv == nil {
		return nil, nil
	}
	mrt := clusterSrv.MultiRaftTransport()
	if mrt == nil {
		return nil, nil
	}

	groupMgr := raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{
		Transport:    mrt,
		NodeID:       nodeID,
		BaseDir:      filepath.Join(homeDir, "raft", "groups"),
		ShutdownLast: "config",
		Logger:       logger,
	})

	var resolver func(string) (string, bool)
	if rcs, ok := rawStore.(*raftConfigStore); ok {
		resolver = func(nodeID string) (string, bool) {
			future := rcs.raft.GetConfiguration()
			if future.Error() != nil {
				return "", false
			}
			for _, srv := range future.Configuration().Servers {
				if string(srv.ID) == nodeID {
					return string(srv.Address), true
				}
			}
			return "", false
		}
	}

	return groupMgr, resolver
}

func buildFactories(logger *slog.Logger, homeDir, vaultsDir string, cfgStore config.Store, orch *orchestrator.Orchestrator, certMgr *cert.Manager, slogCh <-chan logging.CapturedRecord, slogCapture *logging.CaptureHandler, groupMgr *raftgroup.GroupManager, nodeAddrResolver func(string) (string, bool)) orchestrator.Factories {
	reg := func(factory orchestrator.IngesterFactory, defaults func() map[string]string, tester orchestrator.ConnectionTester) orchestrator.IngesterRegistration {
		return orchestrator.IngesterRegistration{Factory: factory, Defaults: defaults, Tester: tester}
	}
	listen := func(factory orchestrator.IngesterFactory, defaults func() map[string]string, addrs func(map[string]string) []orchestrator.ListenAddr) orchestrator.IngesterRegistration {
		return orchestrator.IngesterRegistration{Factory: factory, Defaults: defaults, ListenAddrs: addrs}
	}
	ingesterTypes := map[string]orchestrator.IngesterRegistration{
		"chatterbox": reg(chatterbox.NewIngester, chatterbox.ParamDefaults, nil),
		"scatterbox": reg(scatterbox.NewIngester, scatterbox.ParamDefaults, nil),
		"docker": reg(ingestdocker.NewFactory(cfgStore), ingestdocker.ParamDefaults,
			func(ctx context.Context, params map[string]string) (string, error) {
				return ingestdocker.TestConnection(ctx, params, cfgStore)
			}),
		"fluentfwd": listen(ingestfluentfwd.NewFactory(), ingestfluentfwd.ParamDefaults, ingestfluentfwd.ListenAddrs),
		"http":      listen(ingesthttp.NewFactory(), ingesthttp.ParamDefaults, ingesthttp.ListenAddrs),
		"kafka":     reg(ingestkafka.NewFactory(), ingestkafka.ParamDefaults, ingestkafka.TestConnection),
		"mqtt":      reg(ingestmqtt.NewFactory(), ingestmqtt.ParamDefaults, ingestmqtt.TestConnection),
		"metrics":   reg(ingestmetrics.NewFactory(orch), ingestmetrics.ParamDefaults, nil),
		"otlp":      listen(ingestotlp.NewFactory(), ingestotlp.ParamDefaults, ingestotlp.ListenAddrs),
		"relp":      listen(ingestrelp.NewFactory(certMgr), ingestrelp.ParamDefaults, ingestrelp.ListenAddrs),
		"syslog":    listen(ingestsyslog.NewFactory(), ingestsyslog.ParamDefaults, ingestsyslog.ListenAddrs),
		"tail":      reg(ingesttail.NewFactory(), ingesttail.ParamDefaults, nil),
	}
	if slogCh != nil {
		ingesterTypes["self"] = reg(ingestself.NewFactory(slogCh, slogCapture), ingestself.ParamDefaults, nil)
	}
	return orchestrator.Factories{
		IngesterTypes: ingesterTypes,
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file":   chunkfile.NewFactory(),
			"memory": chunkmem.NewFactory(),
			"jsonl":  chunkjsonl.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file":   indexfile.NewFactory(),
			"memory": indexmem.NewFactory(),
		},
		Logger:              logger,
		HomeDir:             homeDir,
		VaultsDir:           vaultsDir,
		GroupManager:        groupMgr,
		NodeAddressResolver: nodeAddrResolver,
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
