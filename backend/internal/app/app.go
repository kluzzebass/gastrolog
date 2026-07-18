// Package app is the composition root for the gastrolog server. It wires
// all internal packages together and runs the service. The cmd/gastrolog
// binary is a thin CLI wrapper that delegates to [Run].
package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	petname "github.com/dustinkirkland/golang-petname"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"
	"gastrolog/internal/auth"
	"gastrolog/internal/blobstore"
	"gastrolog/internal/cert"
	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkjsonl "gastrolog/internal/chunk/jsonl"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/cluster"
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
	ingestmetrics "gastrolog/internal/ingester/metrics"
	ingestmqtt "gastrolog/internal/ingester/mqtt"
	ingestotlp "gastrolog/internal/ingester/otlp"
	ingestrelp "gastrolog/internal/ingester/relp"
	"gastrolog/internal/ingester/scatterbox"
	ingestself "gastrolog/internal/ingester/self"
	ingestsyslog "gastrolog/internal/ingester/syslog"
	ingesttail "gastrolog/internal/ingester/tail"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/logging"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/schedwatch"
	"gastrolog/internal/server"
	"gastrolog/internal/server/routing"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/system/raftfsm"
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
	// ClusterAdvertise is the address peers store and dial to reach this
	// node (Raft transport target). When empty, the bound listen address
	// is used — but in environments where the pod's bound IP can change
	// across restarts (Kubernetes pod IP rotation, Docker network
	// rescheduling), the advertised address MUST be a stable identifier
	// (DNS name) so peers reconnect after IP changes without manual
	// reconfiguration. See gastrolog-4zy8a.
	ClusterAdvertise string
	// ServicePoolMaxPerPeer caps parallel outbound service-lane gRPC
	// connections per peer (0 = default 4).
	ServicePoolMaxPerPeer int
	JoinAddr              string
	JoinToken             string
	NodeName              string

	// PprofAddr is the pprof HTTP server address (e.g. "localhost:6060").
	// Empty if pprof is disabled. Advertised to cluster peers via broadcast.
	PprofAddr string

	// Non-interactive cluster bootstrap (gastrolog-o9z6o).
	//
	// File-based path (default; no shared HTTP needed):
	//   - WriteBootstrapToken: the bootstrap node atomically writes its
	//     join token to this path with mode 0600 once cluster TLS is
	//     bootstrapped. Joiners on the same shared volume read it via
	//     BootstrapTokenFile.
	//   - BootstrapTokenFile: a joiner reads the join token from this
	//     path, polling with backoff until the file exists or the
	//     timeout fires. Mutually exclusive with JoinToken (the literal
	//     flag wins if both are set).
	//
	// Endpoint-based path (opt-in; for cross-region / immutable infra):
	//   - BootstrapTokenServeSecret: the bootstrap node serves
	//     `GET /cluster/bootstrap-token` on its HTTP listener (port
	//     4564 by default), gated on this shared secret. Empty disables.
	//   - BootstrapTokenURL: a joiner fetches the join token from this
	//     URL, polling with backoff. Authenticates with
	//     BootstrapTokenSecret.
	//   - BootstrapTokenSecret: the secret sent by the joiner.
	WriteBootstrapToken       string
	BootstrapTokenFile        string
	BootstrapTokenServeSecret string
	BootstrapTokenURL         string
	BootstrapTokenSecret      string

	// Initial admin provisioning (gastrolog-3ot7r).
	//
	// When set on a bootstrap node (no --join-addr) and no users exist
	// in the cluster yet, an admin user is created at startup. The
	// existing first-access UI remains the default for unattended /
	// interactive setups and serves as the fallback when no provisioning
	// source is configured.
	//
	// Precedence: file > env. Once any user exists, both sources are
	// no-ops (idempotency — restarts don't re-provision).
	InitialAdminFile     string
	InitialAdminUser     string
	InitialAdminPassword string

	// Environment banner (gastrolog-4vr0l). Display-only metadata surfaced
	// to the UI header so operators can tell which deployment they are
	// looking at without inspecting hostnames or URLs. Both empty by
	// default; empty label hides the banner entirely.
	EnvironmentLabel string
	EnvironmentColor string

	// SegmentHotPathFsync controls segmentation group-commit fsync. When false,
	// the pipeline supervisor sets SegmentDisableFsync (load testing only).
	SegmentHotPathFsync bool

	// RaftHeartbeatTimeout and RaftLeaderLease override the node-wide base
	// Raft failure-detector timing when > 0 (gastrolog-o6plq9). Zero keeps
	// the raftgroup defaults. The operator lever for substrates whose
	// scheduler-stall tail exceeds the shipped detector window; boot fails
	// if lease > heartbeat.
	RaftHeartbeatTimeout time.Duration
	RaftLeaderLease      time.Duration

	// SlogCapture receives copies of slog records for the "self" ingester.
	// Created by main and shared with the CaptureHandler. Nil disables capture.
	SlogCapture <-chan logging.CapturedRecord

	// SlogCaptureHandler is the CaptureHandler that tees slog records.
	// Passed to the self ingester factory so it can apply the min_level param.
	SlogCaptureHandler *logging.CaptureHandler

	// LogFilter is the ComponentFilterHandler whose rule set is driven
	// from the system config store (gastrolog-3flfp). The watcher reads
	// LogLevelConfig and calls SetRuleSet on every configSignal fire.
	LogFilter *logging.ComponentFilterHandler
}

// advertisedClusterAddr returns the address peers should store and dial to
// reach this node. Prefers ClusterAdvertise (stable identifier such as a
// Kubernetes headless-service DNS name) when set, falling back to
// ClusterAddr for single-host / non-clustered scenarios.
func (c RunConfig) advertisedClusterAddr() string {
	if c.ClusterAdvertise != "" {
		return c.ClusterAdvertise
	}
	return c.ClusterAddr
}

// Run starts the gastrolog server. It wires all components, starts the
// orchestrator and HTTP server, and blocks until ctx is cancelled.
//
// boot. Splitting this into helpers per subsystem has been tried in
// past passes and produced worse readability — each subsystem's
// wiring depends on every earlier one and threading 15+ parameters
// into helpers obscures the dataflow. Accept the linear complexity
// here; individual subsystem logic lives in dedicated files.
//
//nolint:gocognit,gocyclo // composition root: wires every subsystem at
func Run(ctx context.Context, logger *slog.Logger, cfg RunConfig) error {
	// Raft failure-detector timing must be installed before ANY group or
	// transport exists — openConfigStore below already starts cluster-ctl.
	// Partial operator input resolves against the shipped defaults so
	// setting only the lease (or only the heartbeat) validates correctly.
	raftHeartbeat := cfg.RaftHeartbeatTimeout
	if raftHeartbeat <= 0 {
		raftHeartbeat = raftgroup.DefaultHeartbeatTimeout
	}
	raftLease := cfg.RaftLeaderLease
	if raftLease <= 0 {
		raftLease = raftgroup.DefaultLeaderLeaseTimeout
	}
	if err := raftgroup.ConfigureTimeouts(raftHeartbeat, raftLease); err != nil {
		return fmt.Errorf("raft timing (--raft-heartbeat-timeout / --raft-leader-lease): %w", err)
	}
	multiraft.ConfigureRPCTimeouts(raftHeartbeat, raftHeartbeat)
	if cfg.RaftHeartbeatTimeout > 0 || cfg.RaftLeaderLease > 0 {
		logger.Info("raft failure-detector timing configured",
			"heartbeat_timeout", raftHeartbeat, "leader_lease", raftLease)
	}

	hd, err := resolveHome(cfg.HomeFlag)
	if err != nil {
		return fmt.Errorf("resolve home directory: %w", err)
	}

	nodeID, err := resolveIdentity(logger, cfg, hd)
	if err != nil {
		return err
	}

	// gastrolog-o9z6o: when running unattended (Docker, K8s), the
	// operator may have configured --bootstrap-token-file or
	// --bootstrap-token-url instead of supplying --join-token directly.
	// Resolve those into cfg.JoinToken before setupCluster reads it.
	// No-op if --join-token is already set or if no source is configured.
	if err := resolveJoinTokenFromSources(ctx, &cfg, logger); err != nil {
		return fmt.Errorf("resolve bootstrap token: %w", err)
	}

	clusterSrv, clusterTLS, err := setupCluster(ctx, logger, cfg, hd, nodeID)
	if err != nil {
		return err
	}

	// gastrolog-2yeie: the boot-time auto-rejoin path (gastrolog-24iv4
	// Step C) used to fire here, asking the cluster "do you still have
	// me?" and renaming raftDir away if not. That mechanism was
	// destructive (wiped node identity + cluster state) and racy (a
	// startup-time GetConfiguration RPC could return stale data and
	// trigger a spurious wipe). With yield-leadership preStop preserving
	// membership across restart, the recovery path that motivated this
	// code is no longer needed — raft loads its WAL, heartbeats with
	// peers, and resumes. If a node IS truly evicted and the operator
	// wants to bring it back fresh, that's an explicit operator action
	// (delete /config/raft/ before starting the pod), not a silent
	// boot-time decision.

	// Alarm state is in-memory only: nothing survives restart — after a
	// restart a re-detected condition is simply a standing alarm again.
	// The configured logger routes transition lines through the same
	// handler chain as every other component (structured format, captured
	// by the self-ingester) — never the bare slog package globals.
	alertCollector := alert.New()
	alertCollector.SetLogger(logger.With("component", "alert"))

	configSignal := notify.NewSignal()
	statsSignal := notify.NewSignal()
	disp := &configDispatcher{localNodeID: nodeID, logger: compDispatch.Apply(logger), clusterTLS: clusterTLS, tlsFilePath: hd.ClusterTLSPath(), configSignal: configSignal}
	rawStore, err := openConfigStore(cfg.ConfigType, raftStoreOpts{
		Home: hd, NodeID: nodeID, JoinAddr: cfg.JoinAddr,
		ClusterSrv: clusterSrv, ClusterTLS: clusterTLS,
		Logger: logger, FSMOpts: []raftfsm.Option{raftfsm.WithOnApply(disp.Handle)},
		Alerts: alertCollector,
	})
	if err != nil {
		return fmt.Errorf("open config store: %w", err)
	}

	// Wrap in a proxy so runtime cluster join can swap the inner store.
	// All consumers hold a reference to proxy; on join, only the inner changes.
	proxy := system.NewStoreProxy(rawStore)
	cfgStore := system.Store(proxy)
	var groupMgr *raftgroup.GroupManager // set later if cluster mode

	if err := startClusterServices(ctx, clusterSrv, clusterTLS, cfgStore, hd, logger, cfg.WriteBootstrapToken); err != nil {
		_ = proxy.Close()
		return err
	}
	// Shutdown order matters: cluster-ctl Raft must stop BEFORE the cluster
	// server, because the Raft follower reads from the transport's rpcChan.
	// Closing the transport first causes a nil-channel deadlock in Raft.
	// Defers run LIFO, so cluster Stop is registered first (runs last).
	if clusterSrv != nil {
		defer clusterSrv.Stop()
	}
	defer func() { _ = proxy.Close() }()

	// Non-blocking: try local FSM, bootstrap, or return nil for replication cases.
	appSys, fromLocalFSM, err := loadLocalConfig(ctx, logger, cfg, cfgStore, clusterTLS, nodeID)
	if err != nil {
		return err
	}

	// gastrolog-3ot7r: provision the initial admin user from --initial-admin-file
	// or --initial-admin-user/--initial-admin-password if configured and no
	// users exist yet. Joiners skip this entirely. The interactive
	// first-access UI remains the fallback for any unconfigured bootstrap.
	if err := provisionInitialAdmin(ctx, cfgStore, cfg, logger); err != nil {
		return fmt.Errorf("provision initial admin: %w", err)
	}

	asyncNodeConfig := fromLocalFSM || appSys == nil
	homeDir, socketPath, err := finalizeNodeSetup(ctx, logger, cfgStore, nodeID, cfg.ConfigType, cfg.NodeName, asyncNodeConfig, hd)
	if err != nil {
		return err
	}

	// Scheduler-stall watchdog (gastrolog-1io54g phase 2): measures runtime
	// starvation — the one resource every Raft group on this node shares.
	// Stalls past the leader lease raise an operator alert; the WARN log
	// timestamps correlate against election events to pin the liveness leak.
	schedWatch := schedwatch.New(logger, raftLease)
	go schedWatch.Run(ctx)

	// Shared shutdown phase. Constructed once per process and threaded into
	// every subsystem that needs to short-circuit work during drain — the
	// orchestrator's replication fanout, the cluster server's stream
	// handlers, the vault announcer, etc. See gastrolog-1e5ke.
	shutdownPhase := lifecycle.New()

	// Async reconciler for SetIngesterAlive Raft applies. Decouples the
	// per-ingester run goroutine (which toggles alive state) from Raft
	// latency and retries transient failures (gastrolog-1ox8z). Without
	// this, an unlucky startup race drops the apply, the error is silently
	// swallowed, and the FSM alive map stays empty for the goroutine's life.
	aliveReconciler := NewAliveReconciler(cfgStore, nodeID, logger)
	go aliveReconciler.Run(ctx)

	orch, err := orchestrator.New(orchestrator.Config{
		Logger:            logger,
		MaxConcurrentJobs: loadMaxConcurrentJobs(ctx, cfgStore),
		SystemLoader:      cfgStore,
		LocalNodeID:       nodeID,
		Alerts:            alertCollector,
		SegmentsDir:       hd.SegmentsDir(),
		DiskGuardPaths:    []string{hd.Root(), hd.SegmentsDir()},
		Phase:             shutdownPhase,
		OnIngesterAlive: func(ingesterID glid.GLID, alive bool) {
			aliveReconciler.Enqueue(ingesterID, alive)
		},
		OnIngesterCheckpoint: func(ingesterID glid.GLID, data []byte) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = cfgStore.SetIngesterCheckpoint(ctx, ingesterID, data)
		},
		// digestion enrichers: extract log level and parse source
		// timestamps from raw bodies when the ingester didn't supply them.
		Digesters:           []digestion.Digester{digestlevel.New(), digesttimestamp.New()},
		SegmentDisableFsync: !cfg.SegmentHotPathFsync,
	})
	if err != nil {
		return fmt.Errorf("create orchestrator: %w", err)
	}

	// Ingest-pipeline channel pressure was demoted from an alarm to a
	// diagnostic (gastrolog-3phtqv, gastrolog-5nvb4y); its transition edges
	// are records of occurrence, and the gate's OnChange hook is the
	// existing choke point — one log line per level change, no per-tick
	// chatter. The log stream is the event record.
	orch.PressureGate().AddOnChange(func(tr chanwatch.PressureTransition) {
		if tr.To == chanwatch.PressureNormal {
			logger.Info("ingest pipeline pressure back to normal",
				"from", tr.From.String(), "channel", tr.Cause, "ratio", tr.Ratio)
		} else {
			logger.Warn("ingest pipeline pressure elevated — ingesters throttling",
				"level", tr.To.String(), "channel", tr.Cause, "ratio", tr.Ratio)
		}
	})

	vaultsDir := cfg.VaultsFlag
	if vaultsDir == "" {
		vaultsDir = homeDir // default: vaults resolve relative to home
	}

	certMgr, err := loadCertManager(ctx, logger, cfgStore)
	if err != nil {
		return err
	}

	groupMgr, vaultWAL, nodeAddrResolver := setupMultiRaft(clusterSrv, rawStore, nodeID, homeDir, logger, alertCollector)

	factories := buildFactories(logger, homeDir, vaultsDir, cfgStore, orch, certMgr, cfg.SlogCapture, cfg.SlogCaptureHandler, groupMgr, nodeAddrResolver, nodeID)
	if clusterSrv != nil {
		factories.PeerConns = clusterSrv.PeerConns()
	}

	// Wire cross-node record forwarding and search forwarding in cluster mode.
	// orchReady is closed after startOrchestrator completes so that forwarded
	// records block (instead of failing) while vaults are being registered.
	orchReady := make(chan struct{})
	var searchForwarder *cluster.SearchForwarder
	var routingForwarder *routing.Forwarder
	if _, ok := rawStore.(*raftClusterCtlStore); ok && clusterSrv != nil {
		searchForwarder = wireClusterForwarding(clusterSrv, orch, orchReady, nodeID, logger, alertCollector)
		routingForwarder = routing.NewForwarder(clusterSrv.PeerConns())
	}

	// Wire the dispatcher now that orchestrator and factories are available.
	disp.orch = orch
	disp.cfgStore = cfgStore
	disp.factories = factories

	// Sync the ComponentFilterHandler's rule set with the system config
	// store. Watcher runs until ctx is cancelled; on every configSignal
	// fire it reads LogLevelConfig and atomically swaps the rule set
	// across every derived handler in the process (gastrolog-3flfp).
	if cfg.LogFilter != nil {
		go WatchLogLevels(ctx, cfg.LogFilter, cfgStore, configSignal, logger)
	}
	disp.catchupScheduler = func(vaultID glid.GLID, followerNodeIDs []string) {
		orch.ScheduleCatchup(vaultID, followerNodeIDs)
	}

	// Wire follower-driven replica catchup (gastrolog-2dgvj). The cluster
	// server's RequestReplicaCatchup handler delegates here; the orchestrator
	// validates leadership, filters chunk eligibility, and fans out pushes
	// asynchronously via the existing replicateToFollower machinery.
	if clusterSrv != nil {
		clusterSrv.SetReplicaCatchupFn(func(ctx context.Context, vaultID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (int, error) {
			n, err := orch.CatchupSelectedChunks(ctx, vaultID, requesterNodeID, chunkIDs)
			return int(n), err
		})
	}

	if err := startOrchestrator(ctx, logger, orch, appSys, factories); err != nil {
		return err
	}
	close(orchReady)

	// Clear any stale "alive" entries in Raft for ingesters this node is
	// configured to know about but isn't running (e.g. last session crashed
	// before setIngesterAlive(false), or config was edited while down).
	// Must happen AFTER orch.Start so ListIngesters() reflects reality.
	clearStaleIngesterAlive(ctx, cfgStore, orch, nodeID, logger)

	wireClusterRaftApplies(clusterSrv, groupMgr)

	// Vault-ctl Raft group membership is reconciled by per-vault leader loops
	// (raftgroup.LeaderLoop) wired by reconfig_vaults.go. On snapshot
	// restore the loops fire as soon as elections complete and reconcile
	// from inside the leader epoch.

	// Monitor slog capture channel pressure.
	if cfg.SlogCapture != nil {
		slogCW := chanwatch.New(logger, 1*time.Second)
		slogCW.Watch("slogCaptureCh", func() (int, int) {
			return len(cfg.SlogCapture), cap(cfg.SlogCapture)
		}, 0.9)
		go slogCW.Run(ctx)
	}

	// Aggregate Raft liveness sources: the vault-group WAL + GroupManager
	// counters, and the cluster-ctl store's own WAL + counters
	// (gastrolog-1io54g). Nil-tolerant: single-node mode may lack any.
	raftLive := &raftLivenessAdapter{}
	if vaultWAL != nil {
		raftLive.wals = append(raftLive.wals, vaultWAL)
	}
	if groupMgr != nil {
		raftLive.counters = append(raftLive.counters, groupMgr.Liveness())
	}
	if src, ok := rawStore.(interface {
		RaftLivenessSources() (*raftgroup.LivenessCounters, *raftwal.WAL)
	}); ok {
		ctlCounters, ctlWAL := src.RaftLivenessSources()
		raftLive.counters = append(raftLive.counters, ctlCounters)
		if ctlWAL != nil {
			raftLive.wals = append(raftLive.wals, ctlWAL)
		}
	}

	broadcaster, peerState, peerJobState, localStatsFn, clusterRouteRatesFn := setupClusterStats(ctx, logger, cfgStore, clusterSrv, orch, alertCollector, cfg.SlogCaptureHandler, nodeID, cfg.ServerAddr, cfg.PprofAddr, statsSignal, raftLive)
	if peerState != nil {
		// Per-vault admission verdicts are cluster-consistent: a starved
		// vault volume or an over-budget vault claim on any node suspends
		// admission for that vault everywhere.
		orch.SetRemoteVaultDiskProtected(peerState.VaultDiskProtected)
		orch.SetRemoteVaultSizeCapped(peerState.VaultSizeCapped)
	}

	// Start vault placement manager (cluster mode only).
	var placementReconcileFn func(ctx context.Context)
	if clusterSrv != nil && peerState != nil {
		pm := &placementManager{
			cfgStore:    cfgStore,
			clusterSrv:  clusterSrv,
			peerState:   peerState,
			factories:   &factories,
			alerts:      alertCollector,
			localNodeID: nodeID,
			logger:      compPlacement.Apply(logger),
			triggerCh:   make(chan struct{}, 1),
			// Local half of the degraded-home check (gastrolog-38bm9t):
			// peers report their vault protect state via NodeStats, but
			// the local node isn't in its own peer table.
			localVaultDiskProtected: func(id glid.GLID) bool {
				return slices.Contains(orch.DiskProtectedVaults(), id)
			},
		}
		disp.placementTrigger = pm.Trigger
		placementReconcileFn = pm.Reconcile
		go pm.Run(ctx)

		// register flattens the standard register-or-warn pattern so
		// the cluster-mode init block stays linear at the top level
		// (nestif lint).
		register := func(jobLabel string, err error) {
			if err != nil {
				logger.Warn("schedule "+jobLabel+" job", "error", err)
			}
		}

		// Periodic placement-reconcile fallback (gastrolog-1ia46). The
		// event-driven reconciles (leadership change, manual Trigger
		// from RPC handlers) stay in pm.Run above; this scheduled
		// job pokes pm.Trigger() as the periodic safety net so the
		// fallback cadence is visible in the inspector.
		register("vault-placement-reconcile", startPlacementReconcile(ctx, orch.Scheduler(), pm))

		// Heartbeat-driven node-state sweep (gastrolog-39m2k). Flips
		// NodeConfig.State between Live and Unreachable based on
		// PeerState freshness so the placement guard sees soft-offline
		// nodes without operator intervention. Registered with the
		// orchestrator's job scheduler so it shows up in the inspector's
		// Scheduled view alongside the rest of the periodic work
		// (gastrolog-28o8p).
		sweep := newUnreachableSweep(cfgStore, clusterSrv, peerState, nodeID, alertCollector, compPlacement.Apply(logger))
		register("unreachable-sweep", startUnreachableSweep(ctx, orch.Scheduler(), sweep))

		// Cluster-ctl learner promoter (gastrolog-2czh9). Watches for
		// Nonvoter / Staging members and promotes them to voters once
		// their broadcast RaftAppliedIndex has matched the leader's
		// for a stability window. Companion to the JoinCluster-as-
		// learner change (gastrolog-41sut) and the per-vault-ctl
		// promoter below. Registered with the orchestrator job
		// scheduler (gastrolog-5npek) so it appears in the inspector.
		learnerPromoter := newClusterCtlLearnerPromoter(clusterSrv, peerState, compCluster.Apply(logger))
		register("cluster-ctl-learner-promoter", startClusterCtlLearnerPromoter(ctx, orch.Scheduler(), learnerPromoter))

		// Per-vault-ctl learner promoter (gastrolog-gcbx7). Same
		// shape as the cluster-ctl promoter but iterates every vault
		// on each tick; only acts on groups this node leads. Catchup
		// signal comes from VaultStats.RaftAppliedIndex in the
		// existing NodeStats broadcast. Registered with the
		// orchestrator job scheduler (gastrolog-4icsr).
		if groupMgr != nil {
			vaultLearnerPromoter := newVaultCtlLearnerPromoter(cfgStore, groupMgr, peerState, nodeID, compCluster.Apply(logger))
			register("vault-ctl-learner-promoter", startVaultCtlLearnerPromoter(ctx, orch.Scheduler(), vaultLearnerPromoter))
		}
	}

	// Ingester convergence sweep (gastrolog-3mnjlo). Event-driven ingester
	// dispatch is one-shot per FSM notification with silent early returns; a
	// node that misses its boot dispatch runs no ingesters until the next
	// config change (a full-cluster restart left one node originating nothing
	// for 40+ minutes). This periodic safety net re-reconciles
	// desired-vs-running (idempotent) and logs any divergence, once per
	// state change. Registered unconditionally — single-node deploys
	// converge the same way.
	if err := startIngesterReconcileSweep(ctx, orch.Scheduler(), disp, logger.With("component", "ingestion")); err != nil {
		logger.Warn("startup: register scheduled job", "job", "ingester-reconcile", "error", err)
	}

	// For replication cases: block until server settings replicate from the leader.
	if err := awaitReplication(ctx, appSys, cfg.ConfigType, cfgStore, logger); err != nil {
		return err
	}

	// Fresh joiner: the FSM has been populated by snapshot replication,
	// but FSM.Restore (unlike FSM.Apply) does not fire onApply
	// notifications. The dispatcher therefore never saw the existing
	// vaults / ingesters / routes / policies during boot, and
	// orch.ApplyConfig was called with appSys=nil (a no-op). Without
	// this replay, every AllNodes:true ingester would never register
	// or run on the joiner — even though the dashboard correctly lists
	// it as a target. See gastrolog-3hcfm. No-op for restart-of-voter
	// and bootstrap paths because appSys is non-nil there.
	if appSys == nil {
		disp.ReplayConfigFromStore(ctx)
	}

	tokens, err := buildAuthTokens(ctx, logger, cfgStore, cfg.NoAuth)
	if err != nil {
		return err
	}

	// Build cluster operation callbacks (raft mode only).
	var joinClusterFn func(ctx context.Context, leaderAddr, joinToken string) error
	var removeNodeFn func(ctx context.Context, nodeID string, force bool) error
	var setNodeSuffrageFn func(ctx context.Context, nodeID string, voter bool) error
	advertisedAddr := cfg.advertisedClusterAddr()
	if cfg.ConfigType == "raft" && clusterSrv != nil {
		joinClusterFn = makeJoinClusterFunc(proxy, clusterSrv, clusterTLS, hd, nodeID, advertisedAddr, orch, disp, logger)
		removeNodeFn = makeRemoveNodeFunc(clusterSrv, cfgStore, nodeID, logger)
		setNodeSuffrageFn = makeSetNodeSuffrageFunc(clusterSrv, nodeID, orch.Scheduler(), logger)

		// Register eviction handler: reinitialize as a fresh single-node cluster.
		clusterSrv.SetEvictionHandler(makeEvictionHandler(proxy, clusterSrv, clusterTLS, hd, nodeID, orch, disp, logger))

		// gastrolog-24iv4: the stale-voter-reaper that previously lived
		// here is gone. Its only role was cleaning up after operators
		// who used `kubectl scale` directly instead of the supported
		// `cluster remove-node` path (gastrolog-6bfwk). The preStop
		// hook + `cluster demote-self` CLI in this same PR makes every
		// K8s-managed termination call cluster.RemoveNode cleanly,
		// closing that operator-discipline gap at the source. Raft
		// handles unreachable voters correctly without intervention;
		// auto-eviction added risk (maintenance-window evictions,
		// no-quorum-gate cascades) without solving any problem the
		// preStop hook doesn't already solve.
	}

	return serveAndAwaitShutdown(ctx, serverDeps{
		Logger:              logger,
		ServerAddr:          cfg.ServerAddr,
		HomeDir:             homeDir,
		NodeID:              nodeID,
		SocketPath:          socketPath,
		ClusterAddr:         advertisedAddr,
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
		ClusterRouteRates:   clusterRouteRatesFn,
		SearchForwarder:     searchForwarder,
		RoutingForwarder:    routingForwarder,
		JoinClusterFunc:     joinClusterFn,
		RemoveNodeFunc:      removeNodeFn,
		SetNodeSuffrageFunc: setNodeSuffrageFn,
		Dispatcher:          disp,
		GroupMgr:            groupMgr,
		WAL:                 vaultWAL,
		ConfigStore:         proxy,
		PlacementReconcile:  placementReconcileFn,

		BootstrapTokenServeSecret: cfg.BootstrapTokenServeSecret,
		BootstrapTokenFn:          makeBootstrapTokenFn(cfgStore),

		EnvironmentLabel: cfg.EnvironmentLabel,
		EnvironmentColor: cfg.EnvironmentColor,

		LogFilter: cfg.LogFilter,
	})
}

// wireClusterForwarding sets up cross-node record, search, context, vault,
// and explain forwarding on the cluster server. Returns the search forwarder
// for the HTTP server to use.
func wireClusterForwarding(clusterSrv *cluster.Server, orch *orchestrator.Orchestrator, orchReady <-chan struct{}, nodeID string, logger *slog.Logger, alerts *alert.Collector) *cluster.SearchForwarder {
	peerConns := clusterSrv.PeerConns()

	// The record importer waits for the orchestrator to be ready (vaults
	// registered) before writing. Without this gate, sealed-chunk imports
	// arriving during startup hit ErrVaultNotFound, causing the sending
	// node to enter exponential backoff.
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

	// Wire cross-node chunk migration and replication.
	chunkTransferrer := cluster.NewChunkTransferrer(peerConns)
	orch.SetRemoteTransferrer(chunkTransferrer)

	// Vault replication: unified ordered stream per vault per follower.
	chunkReplicator := cluster.NewChunkReplicator(peerConns, compVaultReplicator.Apply(logger))
	orch.SetChunkReplicator(chunkReplicator)

	// Same readiness gate for bulk chunk imports.
	clusterSrv.SetRecordImporter(func(ctx context.Context, vaultID glid.GLID, next chunk.RecordIterator) error {
		if err := waitForOrch(ctx); err != nil {
			return err
		}
		return orch.ImportChunkRecords(ctx, vaultID, next)
	})
	clusterSrv.SetVaultRecordImporter(func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error {
		if err := waitForOrch(ctx); err != nil {
			return err
		}
		return orch.ImportToVault(ctx, vaultID, chunkID, next)
	})
	searchForwarder := cluster.NewSearchForwarder(peerConns)
	clusterSrv.SetSearchExecutor(newSearchExecutor(orch))
	clusterSrv.SetContextExecutor(newContextExecutor(orch))
	clusterSrv.SetListChunksExecutor(newListChunksExecutor(orch))
	clusterSrv.SetPipelineBacklogDiskExecutor(newPipelineBacklogDiskExecutor(orch))
	clusterSrv.SetGetIndexesExecutor(newGetIndexesExecutor(orch))
	clusterSrv.SetValidateVaultExecutor(newValidateVaultExecutor(orch))
	clusterSrv.SetGetChunkExecutor(newGetChunkExecutor(orch))
	clusterSrv.SetAnalyzeChunkExecutor(newAnalyzeChunkExecutor(orch))
	clusterSrv.SetChunkEventSubscriber(newChunkEventSubscriber(orch))
	clusterSrv.SetSealVaultExecutor(newSealVaultExecutor(orch))
	clusterSrv.SetDeleteChunkExecutor(func(ctx context.Context, vaultID glid.GLID, chunkID chunk.ChunkID) error {
		return orch.DeleteChunk(vaultID, chunkID)
	})
	clusterSrv.SetReindexVaultExecutor(newReindexVaultExecutor(orch))
	clusterSrv.SetExplainExecutor(newExplainExecutor(orch, nodeID))
	clusterSrv.SetFollowExecutor(newFollowExecutor(orch))
	clusterSrv.SetSegmentPullServer(orch.ServeSegmentPull)
	clusterSrv.SetChunkGLCBPullServer(orch.ServeChunkGLCBPull)

	return searchForwarder
}

// wireManagedFileTransfer sets up cluster-side handlers for streaming managed
// files between nodes and returns a managedFileManager for the dispatcher.
func wireManagedFileTransfer(clusterSrv *cluster.Server, httpSrv *server.Server, cfgStore system.Store, homeDir string, logger *slog.Logger) *managedFileManager {
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
		logger:      compManagedFiles.Apply(logger),
	}
}

// nonRaftApplyHook returns the dispatcher callback for non-raft system stores.
func nonRaftApplyHook(configType string, handle func(raftfsm.Notification)) func(raftfsm.Notification) {
	if configType != "raft" {
		return handle
	}
	return nil
}

// startOrchestrator applies config, rebuilds missing indexes, and starts the orchestrator.
func startOrchestrator(ctx context.Context, logger *slog.Logger, orch *orchestrator.Orchestrator, appSys *system.System, factories orchestrator.Factories) error {
	if appSys != nil {
		logger.Info("loaded config",
			"ingesters", len(appSys.Config.Ingesters),
			"vaults", len(appSys.Config.Vaults))
	}
	if err := orch.ApplyConfig(appSys, factories); err != nil {
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
// raftLivenessAdapter sums WAL append latency and liveness counters across
// every Raft instance on this node for the stats collector
// (gastrolog-1io54g).
type raftLivenessAdapter struct {
	wals     []*raftwal.WAL
	counters []*raftgroup.LivenessCounters
}

func (a *raftLivenessAdapter) WALAppendTotals() (count, totalNanos uint64) {
	for _, w := range a.wals {
		c, n := w.AppendTotals()
		count += c
		totalNanos += n
	}
	return count, totalNanos
}

func (a *raftLivenessAdapter) TakeWALAppendMax() (maxNanos uint64) {
	for _, w := range a.wals {
		if m := w.TakeMaxAppendLatency(); m > maxNanos {
			maxNanos = m
		}
	}
	return maxNanos
}

func (a *raftLivenessAdapter) RaftLiveness() (elections, leaderLosses, failedHeartbeats uint64) {
	for _, c := range a.counters {
		e, l, f := c.Snapshot()
		elections += e
		leaderLosses += l
		failedHeartbeats += f
	}
	return elections, leaderLosses, failedHeartbeats
}

func setupClusterStats(ctx context.Context, logger *slog.Logger, cfgStore system.Store, clusterSrv *cluster.Server, orch *orchestrator.Orchestrator, alerts *alert.Collector, slogCapture *logging.CaptureHandler, nodeID string, apiAddr string, pprofAddr string, statsSignal *notify.Signal, raftLive cluster.RaftLivenessProvider) (*cluster.Broadcaster, *cluster.PeerState, *cluster.PeerJobState, func() *gastrologv1.NodeStats, func() (*gastrologv1.ThroughputRate, *gastrologv1.ThroughputRate)) {
	// Taken as a concrete type and converted explicitly: assigning a typed
	// nil *CaptureHandler straight into the interface field would read as
	// non-nil and panic in DroppedCount on the first tick.
	var logDrops cluster.LogDropsProvider
	if slogCapture != nil {
		logDrops = slogCapture
	}
	var broadcaster *cluster.Broadcaster
	if clusterSrv != nil && clusterSrv.PeerConns() != nil {
		broadcaster = cluster.NewBroadcaster(clusterSrv.PeerConns(), compBroadcast.Apply(logger))
	}
	if broadcaster == nil || clusterSrv == nil {
		return nil, nil, nil, nil, nil
	}

	broadcastInterval, heartbeatInterval := loadClusterIntervals(ctx, cfgStore)

	// PeerState TTL must be a multiple of the **heartbeat** cadence (not
	// the heavy NodeStats cadence) so paused-peer detection is fast.
	// Default 8× heartbeat: tolerate up to 7 missed heartbeats before
	// offline. The previous 4× setting caused user-visible UI flapping
	// (gastrolog-4iacg): a single late broadcast — network blip, GC
	// pause, scheduler hiccup — would lapse the TTL, the inspector
	// would flip the peer offline, the next broadcast would restore
	// it, and the cycle repeated every ~5 seconds. 8× absorbs single
	// missed broadcasts; networks with worse jitter can tune via
	// GLOG_PEER_TTL_MULTIPLIER. Original 4× rationale was the safety
	// factor from gastrolog-2kio8's 5s broadcast / 20s shape — that
	// matched theoretical missed-broadcast math but underestimated
	// real-world jitter on 1s heartbeats.
	peerState := cluster.NewPeerState(peerTTLMultiplier(logger) * heartbeatInterval)
	clusterSrv.Subscribe(peerState.HandleBroadcast)

	peerJobState := cluster.NewPeerJobState(20 * time.Second)
	clusterSrv.Subscribe(peerJobState.HandleBroadcast)

	// Write a placeholder NodeConfig for every newly admitted peer so
	// fresh joiners never display as raw GLIDs in the UI while their
	// own async ensureNodeConfig write is in flight (gastrolog-4dqfs).
	// Leader-only; the joiner's own write later updates Name to its
	// preferred value (e.g. pod hostname).
	observePeerAdditions(ctx, clusterSrv, cfgStore, logger)

	statsAdapter := &orchStatsAdapter{orch: orch}
	collector := cluster.NewStatsCollector(cluster.StatsCollectorConfig{
		Broadcaster:  broadcaster,
		RaftStats:    clusterSrv,
		Stats:        statsAdapter,
		PeerConns:    clusterSrv.PeerConns(),
		RaftLiveness: raftLive,
		// Discarded diagnostic log records; nil when capture is disabled.
		LogDrops: logDrops,
		// Cluster-total route counters: local + live peers' cumulative
		// broadcast totals. Windowed server-side so cluster rate history is
		// system data, not client-side accumulation (gastrolog-4eh5ns).
		ClusterRouteTotals: func() (int64, int64, string) {
			rs := statsAdapter.RouteStats()
			pRouted, pMatched, members := peerState.AggregateRouteTotals()
			// "self" + sorted live peers: the summed window re-anchors on
			// any change so peers entering/leaving the sum never read as
			// traffic (gastrolog-mliwrd).
			return rs.Routed + pRouted, rs.Matched + pMatched,
				"self," + strings.Join(members, ",")
		},
		Alerts: alerts,
		Jobs:   &jobBroadcastAdapter{scheduler: orch.Scheduler(), nodeID: nodeID},
		NodeID: nodeID,
		NodeNameFn: func() string {
			nid, err := glid.ParseAny(nodeID)
			if err != nil {
				return ""
			}
			n, err := cfgStore.GetNode(ctx, nid)
			if err != nil || n == nil {
				return ""
			}
			return n.Name
		},
		Version:           Version,
		StartTime:         time.Now(),
		Interval:          broadcastInterval,
		HeartbeatInterval: heartbeatInterval,
		ApiAddress:        apiAddr,
		PprofAddress:      pprofAddr,
		StatsSignal:       statsSignal,
		Logger:            compStatsCollector.Apply(logger),
	})

	orch.Scheduler().SetOnJobChange(func() {
		go collector.BroadcastJobs(ctx)
	})

	if err := startStatsCollectorJobs(orch.Scheduler(), collector, ctx, broadcastInterval, heartbeatInterval); err != nil {
		logger.Error("register stats collector scheduler jobs", "error", err)
	}

	// Evict per-peer satellite state the moment a node is removed
	// from the Raft configuration. Without this the various caches
	// grow unboundedly on clusters that churn nodes. See gastrolog-9ohip
	// for the inventory; predecessor gastrolog-19bq4 covered the first
	// two (PeerState, PeerJobState) but missed the rest.
	observePeerRemovals(ctx, clusterSrv, logger,
		peerState,
		peerJobState,
		clusterSrv.ByteMetrics(),
		broadcaster,
		collector,
	)
	// Belt-and-suspenders: periodic reconcile against current Raft
	// membership covers the edge case where a follower receives the
	// config change via snapshot install (which doesn't fire
	// PeerObservation) and would otherwise leave the observer-based
	// eviction stranded. Same six caches as the observer above.
	// Registration + cron lives in peer_cache_reconcile.go so the
	// job is discoverable alongside the orchestrator's other
	// scheduled sweeps in the inspector's Scheduled view.
	if err := startPeerCacheReconcile(orch.Scheduler(), clusterSrv, compCluster.Apply(logger),
		peerState,
		peerJobState,
		clusterSrv.ByteMetrics(),
		broadcaster,
		collector,
	); err != nil {
		logger.Warn("schedule peer-cache reconcile job", "error", err)
	}

	return broadcaster, peerState, peerJobState, collector.CollectLocalSnapshot, collector.ClusterRouteRates
}

// resolveIdentity ensures the home directory exists and resolves the node ID.
//
// Canonical source: the system-raft StableStore (see resolveNodeID).
// For memory-only config (tests, ephemeral single-node), a fresh ID is
// generated per process — memory mode has no raft WAL to consult.
func resolveIdentity(logger *slog.Logger, cfg RunConfig, hd home.Dir) (string, error) {
	if cfg.ConfigType == "memory" {
		return glid.New().String(), nil
	}
	if err := hd.EnsureExists(); err != nil {
		return "", err
	}
	logger.Info("home directory", "path", hd.Root())

	id, err := resolveNodeID(hd, logger)
	if err != nil {
		return "", fmt.Errorf("resolve node ID: %w", err)
	}
	return id.String(), nil
}

// loadLocalConfig attempts to load config from the local FSM or bootstrap.
func loadLocalConfig(ctx context.Context, logger *slog.Logger, cfg RunConfig, cfgStore system.Store, clusterTLS *cluster.ClusterTLS, nodeID string) (*system.System, bool, error) {
	if err := requestClusterMembership(ctx, logger, cfg, cfgStore, clusterTLS, nodeID); err != nil {
		return nil, false, err
	}

	if cfg.JoinAddr != "" {
		// JoinAddr is set in two scenarios that look identical at the
		// pod level:
		//   1. Fresh join — new voter, no local FSM state. Config must
		//      replicate from the leader; we return nil and let the
		//      dispatcher's Notify path create vault-ctl Raft groups as
		//      vault configs apply.
		//   2. Restart of an existing voter (K8s rolling upgrade, pod
		//      reschedule, etc.) — local FSM was just restored from a
		//      snapshot at the previous incarnation's commit index, and
		//      already contains the full system config. Snapshot restore
		//      goes through fsm.Restore() (NOT fsm.Apply()), so no
		//      NotifyVaultConfigPut events fire for the existing vaults.
		//      If we return nil here, the orchestrator boots with vaults=0
		//      forever and no vault-ctl Raft groups ever start on this
		//      node. See gastrolog-1gh5s — operator-visible symptom was
		//      vault-ctl groups stuck without a leader after every
		//      rolling upgrade.
		//
		// Distinguish the two by probing the local FSM. If it already has
		// vault configs or a bootstrapped server-settings JWT secret,
		// this is a restart — use the local state directly. Otherwise
		// fall through to the fresh-join return.
		if cfg.ConfigType == "raft" && isRestartOfVoter(ctx, cfg, cfgStore) {
			localCfg, _ := cfgStore.Load(ctx)
			logger.Info("restart of existing voter detected; using local FSM config",
				"vaults", len(localCfg.Config.Vaults),
				"ingesters", len(localCfg.Config.Ingesters))
			return localCfg, true, nil
		}
		logger.Info("joining cluster, config will replicate from leader")
		return nil, false, nil
	}

	if cfg.ConfigType == "raft" {
		// Wait for a leader AND for the local FSM to catch up to the cluster's
		// latest committed state before reading anything from it. hraft's
		// NewRaft returns with the FSM at the snapshot level; post-snapshot
		// committed entries (vault placements, NSCs, etc.) only become visible
		// after either a Barrier on the leader or a few AppendEntries rounds
		// on a follower. Without this wait, the orchestrator reads stale
		// state and creates vault-ctl Raft groups with incomplete member lists.
		//
		// cluster.sh run restarts nodes without --join-addr; a returning voter
		// still has a snapshot-restored FSM and must take the same restart
		// shortcut as the JoinAddr path (gastrolog-1gh5s) instead of blocking
		// on WaitForFSMCatchup — under load the stability window may not
		// complete within 10s while vault-ctl snapshots stream.
		if err := waitForQuorum(ctx, cfgStore, logger); err != nil {
			return nil, false, err
		}
		if isRestartOfVoter(ctx, cfg, cfgStore) {
			localCfg, _ := cfgStore.Load(ctx)
			logger.Info("restart of existing voter detected; using local FSM config",
				"vaults", len(localCfg.Config.Vaults),
				"ingesters", len(localCfg.Config.Ingesters))
			return localCfg, true, nil
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
	appSys, err := ensureConfig(ctx, logger, cfgStore)
	if err != nil {
		return nil, false, err
	}
	return appSys, false, nil
}

// requestClusterMembership asks the cluster leader to add this node to
// the Raft configuration. Fresh joiners enter as nonvoters (learners)
// and get promoted by the cluster-ctl learner promoter (gastrolog-2czh9)
// once caught up; restart-of-existing-voter requests use AddVoter for
// idempotent address refresh. The fresh-vs-restart decision probes the
// local FSM (presence of vault configs or a JWT secret).
//
// No-op if join parameters are not set.
func requestClusterMembership(ctx context.Context, logger *slog.Logger, cfg RunConfig, cfgStore system.Store, clusterTLS *cluster.ClusterTLS, nodeID string) error {
	advertise := cfg.advertisedClusterAddr()
	if cfg.JoinAddr == "" || clusterTLS == nil || advertise == "" {
		return nil
	}
	asVoter := isRestartOfVoter(ctx, cfg, cfgStore)
	kind := "nonvoter (learner)"
	if asVoter {
		kind = "voter (restart of existing voter)"
	}
	logger.Info("requesting cluster membership", "leader_addr", cfg.JoinAddr, "advertise", advertise, "as", kind)
	joinCtx, joinCancel := context.WithTimeout(ctx, 30*time.Second)
	defer joinCancel()
	if err := cluster.JoinCluster(joinCtx, logger, cfg.JoinAddr, nodeID, advertise, clusterTLS, asVoter); err != nil {
		return fmt.Errorf("join cluster: %w", err)
	}
	logger.Info("cluster membership granted by leader", "as", kind)
	return nil
}

// isRestartOfVoter probes the local FSM to determine whether this
// join request is a restart of an existing voter (preserved local
// state) versus a fresh joiner (empty local state). Used by both
// requestClusterMembership and loadLocalConfig — extracted so the two
// agree on the boundary without duplicating the predicate.
//
// True (restart): local FSM has at least one vault or a JWT secret —
// the snapshot replay from the previous incarnation populated state
// the joiner intends to resume from.
//
// False (fresh): empty FSM, never been in the cluster.
func isRestartOfVoter(ctx context.Context, cfg RunConfig, cfgStore system.Store) bool {
	if cfg.ConfigType != "raft" || cfgStore == nil {
		return false
	}
	localCfg, _ := cfgStore.Load(ctx)
	ss, _ := cfgStore.LoadServerSettings(ctx)
	return localCfg != nil && (len(localCfg.Config.Vaults) > 0 || ss.Auth.JWTSecret != "")
}

// finalizeNodeSetup ensures this node has a NodeConfig with a name and
// resolves the home directory and socket path. If preferredName is set, it
// is used instead of generating a random petname.
func finalizeNodeSetup(ctx context.Context, logger *slog.Logger, cfgStore system.Store, nodeID, configType, preferredName string, asyncNodeConfig bool, hd home.Dir) (string, string, error) {
	if asyncNodeConfig {
		logNodeIdentity(logger, nodeID, hd.ReadNodeName())
		go ensureNodeConfigAsync(ctx, cfgStore, nodeID, configType, preferredName, hd, logger)
	} else {
		nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID, preferredName)
		if err != nil {
			return "", "", fmt.Errorf("ensure node config: %w", err)
		}
		logNodeIdentity(logger, nodeID, nodeName)
		persistNodeName(logger, configType, hd, nodeName)
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
func awaitReplication(ctx context.Context, appSys *system.System, configType string, cfgStore system.Store, logger *slog.Logger) error {
	if appSys != nil || configType != "raft" {
		return nil
	}
	return waitForServerSettings(ctx, cfgStore, 60*time.Second, logger)
}

func waitForServerSettings(ctx context.Context, cfgStore system.Store, timeout time.Duration, logger *slog.Logger) error {
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

func ensureNodeConfig(ctx context.Context, cfgStore system.Store, nodeID, preferredName string) (string, error) {
	nodeUUID, err := glid.ParseAny(nodeID)
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
	if err := cfgStore.PutNode(ctx, system.NodeConfig{
		ID:         nodeUUID,
		Name:       name,
		State:      system.NodeStateLive,
		StateSince: time.Now(),
	}); err != nil {
		return "", err
	}
	return name, nil
}

func waitForQuorum(ctx context.Context, cfgStore system.Store, logger *slog.Logger) error {
	inner := cfgStore
	if p, ok := cfgStore.(*system.StoreProxy); ok {
		inner = p.Inner()
	}
	rcs, ok := inner.(*raftClusterCtlStore)
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
func waitForFSMCatchup(ctx context.Context, cfgStore system.Store, timeout time.Duration, logger *slog.Logger) error {
	inner := cfgStore
	if p, ok := cfgStore.(*system.StoreProxy); ok {
		inner = p.Inner()
	}
	rcs, ok := inner.(*raftClusterCtlStore)
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

func ensureNodeConfigAsync(ctx context.Context, cfgStore system.Store, nodeID, configType, preferredName string, hd home.Dir, logger *slog.Logger) {
	if err := waitForQuorum(ctx, cfgStore, logger); err != nil {
		return
	}
	nodeName, err := ensureNodeConfig(ctx, cfgStore, nodeID, preferredName)
	if err != nil {
		logger.Warn("ensure node config failed (will retry on next start)", "error", err)
		return
	}
	persistNodeName(logger, configType, hd, nodeName)
}

func persistNodeName(logger *slog.Logger, configType string, hd home.Dir, nodeName string) {
	if configType == "memory" {
		return
	}
	if err := hd.WriteNodeName(nodeName); err != nil {
		logger.Warn("failed to persist node name", "error", err)
	}
}

func ensureConfig(ctx context.Context, logger *slog.Logger, cfgStore system.Store) (*system.System, error) {
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
		if err := system.BootstrapMinimal(ctx, cfgStore); err != nil {
			return nil, fmt.Errorf("bootstrap minimal config: %w", err)
		}
	}

	cfg, err = cfgStore.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load bootstrapped config: %w", err)
	}
	return cfg, nil
}

func loadMaxConcurrentJobs(ctx context.Context, cfgStore system.Store) int {
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return 0
	}
	return ss.Scheduler.MaxConcurrentJobs
}

// defaultPeerTTLMultiplier is the multiplier applied to the heartbeat
// interval to compute the PeerState TTL. 8× tolerates up to 7 missed
// heartbeats; raised from the original 4× in gastrolog-4iacg because
// the tighter window caused UI flapping on single late broadcasts.
const defaultPeerTTLMultiplier = 8

// peerTTLMultiplier returns the PeerState TTL multiplier, sourced
// from GLOG_PEER_TTL_MULTIPLIER if set to a positive integer,
// otherwise defaultPeerTTLMultiplier. Operators on pathological
// networks can raise this to absorb worse jitter without paying
// proportional detection latency on healthy clusters.
func peerTTLMultiplier(logger *slog.Logger) time.Duration {
	v := os.Getenv("GLOG_PEER_TTL_MULTIPLIER")
	if v == "" {
		return defaultPeerTTLMultiplier
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		logger.Warn("invalid GLOG_PEER_TTL_MULTIPLIER, using default",
			"value", v, "default", defaultPeerTTLMultiplier)
		return defaultPeerTTLMultiplier
	}
	return time.Duration(n)
}

// loadClusterIntervals reads the broadcast and heartbeat intervals from
// configured server settings. Either may be zero on return — the
// StatsCollector applies its own defaults (5s broadcast, 1s heartbeat).
// The heartbeat is forced to a sane minimum here too so the PeerState
// TTL (multiplier × heartbeat) never collapses to 0.
func loadClusterIntervals(ctx context.Context, cfgStore system.Store) (broadcast, heartbeat time.Duration) {
	ss, err := cfgStore.LoadServerSettings(ctx)
	if err != nil {
		return 0, 1 * time.Second
	}
	if d, err := time.ParseDuration(ss.Cluster.BroadcastInterval); err == nil && d > 0 {
		broadcast = d
	}
	if d, err := time.ParseDuration(ss.Cluster.HeartbeatInterval); err == nil && d > 0 {
		heartbeat = d
	}
	if heartbeat <= 0 {
		heartbeat = 1 * time.Second
	}
	return broadcast, heartbeat
}

func buildAuthTokens(ctx context.Context, logger *slog.Logger, cfgStore system.Store, noAuth bool) (*auth.TokenService, error) {
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

func loadCertManager(ctx context.Context, logger *slog.Logger, cfgStore system.Store) (*cert.Manager, error) {
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
	CfgStore            system.Store
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
	ClusterRouteRates   func() (*gastrologv1.ThroughputRate, *gastrologv1.ThroughputRate)
	SearchForwarder     *cluster.SearchForwarder
	RoutingForwarder    routing.UnaryForwarder
	JoinClusterFunc     func(ctx context.Context, leaderAddr, joinToken string) error
	RemoveNodeFunc      func(ctx context.Context, nodeID string, force bool) error
	SetNodeSuffrageFunc func(ctx context.Context, nodeID string, voter bool) error
	Dispatcher          *configDispatcher
	GroupMgr            *raftgroup.GroupManager
	WAL                 *raftwal.WAL // vault-ctl raftwal at raft/groups/wal; closed after cluster-ctl raft
	ConfigStore         io.Closer    // rawStore — closed before gRPC for clean Raft shutdown
	PlacementReconcile  func(ctx context.Context)

	// gastrolog-o9z6o: when non-empty, the server registers
	// /cluster/bootstrap-token gated on this secret. BootstrapTokenFn
	// returns the cluster join token from the live config store.
	BootstrapTokenServeSecret string
	BootstrapTokenFn          func() (string, error)

	// Environment banner (gastrolog-4vr0l). Display-only metadata.
	EnvironmentLabel string
	EnvironmentColor string

	LogFilter *logging.ComponentFilterHandler
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
			PeerPipelineDisk: deps.PeerState,
			PeerJobs:         deps.PeerJobState,
			LocalStats:       deps.LocalStats, ClusterRouteRates: deps.ClusterRouteRates, RemoteSearcher: deps.SearchForwarder, RemoteChunkLister: deps.SearchForwarder,
			RemotePipelineBacklog: deps.SearchForwarder,
			RemoteChunkWatcher:    deps.SearchForwarder,
			RemoteIndexer:         deps.SearchForwarder,
			RoutingForwarder:      deps.RoutingForwarder, ClusterAddress: deps.ClusterAddr,
			JoinClusterFunc: deps.JoinClusterFunc, RemoveNodeFunc: deps.RemoveNodeFunc,
			SetNodeSuffrageFunc: deps.SetNodeSuffrageFunc,
			CloudTesters: map[string]server.CloudServiceTester{
				"file": blobstore.NewConnectionTester(deps.Logger),
			},
			PlacementReconcile:        deps.PlacementReconcile,
			BootstrapTokenServeSecret: deps.BootstrapTokenServeSecret,
			BootstrapTokenFn:          deps.BootstrapTokenFn,
			EnvironmentLabel:          deps.EnvironmentLabel,
			EnvironmentColor:          deps.EnvironmentColor,
			LogFilter:                 deps.LogFilter,
		})
		// Provide the cluster's ForwardRPC handler with the internal mux.
		// NoAuthInterceptor + no routing interceptor prevents loops.
		if deps.ClusterSrv != nil {
			deps.ClusterSrv.SetInternalHandler(srv.BuildInternalHandler())
		}

		// Wire managed file transfer handlers on the cluster server. The HTTP
		// server owns the managed files on disk; the cluster server streams them
		// to peers. Must happen after server creation but before serving starts.
		wireManagedFiles(ctx, deps, srv)

		serverWg.Go(func() {
			if err := srv.ServeTCP(deps.ServerAddr); err != nil {
				deps.Logger.Error("server error", "error", err)
			}
		})
	}

	<-ctx.Done()

	var stopErr error

	deps.Logger.Info("shutting down orchestrator")
	if err := deps.Orch.Stop(); err != nil {
		stopErr = err
	}

	if deps.Broadcaster != nil {
		_ = deps.Broadcaster.Close()
	}

	// Shutdown order: vault multiraft → cluster-ctl Raft → vault WAL → cluster WAL (via ConfigStore) → gRPC.
	// Cluster-ctl and vault groups use separate raftwal directories (gastrolog-3tp89). Raft must
	// shut down WHILE the transport is alive, otherwise the leader's replication goroutines block
	// on dead gRPC connections.
	if deps.GroupMgr != nil {
		deps.Logger.Info("shutting down vault multiraft groups")
		deps.GroupMgr.Shutdown()
	}

	if deps.ConfigStore != nil {
		deps.Logger.Info("shutting down cluster-ctl raft")
		_ = deps.ConfigStore.Close()
	}

	if deps.WAL != nil {
		if err := deps.WAL.Close(); err != nil {
			deps.Logger.Error("vault-ctl raftwal close failed", "error", err)
		}
	}

	if deps.ClusterSrv != nil {
		deps.Logger.Info("stopping cluster server")
		deps.ClusterSrv.Stop()
	}

	if srv != nil {
		deps.Logger.Info("stopping server")
		// The root ctx is already cancelled by the time we get here (that
		// is how we woke up). Pass srv.Stop a FRESH context with a bounded
		// drain budget so it can finish in-flight HTTP requests cleanly
		// instead of returning context.Canceled immediately.
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
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

	deps.Logger.Info("shutdown complete")
	return stopErr
}

// setupMultiRaft creates the GroupManager and node address resolver for vault
// control-plane multiraft. Returns (nil, nil) in single-node / non-raft mode.
func setupMultiRaft(clusterSrv *cluster.Server, rawStore system.Store, nodeID, homeDir string, logger *slog.Logger, alerts *alert.Collector) (*raftgroup.GroupManager, *raftwal.WAL, func(string) (string, bool)) {
	if clusterSrv == nil {
		return nil, nil, nil
	}
	mrt := clusterSrv.MultiRaftTransport()
	if mrt == nil {
		return nil, nil, nil
	}

	hd := home.New(homeDir)
	walDir := hd.VaultCtlWALDir()
	wal, err := raftwal.Open(walDir, raftwal.Config{
		OnReserveState: walReserveAlarm(alerts, logger, "vault-ctl"),
	})
	if err != nil {
		logger.Warn("failed to open vault-ctl raft WAL", "dir", walDir, "error", err)
		return nil, nil, nil
	}
	logger.Info("vault-ctl raft WAL ready", "dir", walDir)

	groupMgr := raftgroup.NewGroupManager(raftgroup.GroupManagerConfig{
		Transport: mrt,
		NodeID:    nodeID,
		BaseDir:   filepath.Join(homeDir, "raft", "groups"),
		// The cluster-ctl raft is not managed by GroupManager; only vault/.../ctl
		// multiraft groups are. Leave ShutdownLast empty so Shutdown does not look for a
		// non-existent group ID.
		ShutdownLast:   "",
		WAL:            wal,
		PeerConns:      clusterSrv.PeerConns(),
		EnsureRaftLane: clusterSrv.EnsureRaftGroupLane,
		RemoveRaftLane: clusterSrv.RemoveRaftGroupLane,
		Logger:         logger,
	})

	var resolver func(string) (string, bool)
	if rcs, ok := rawStore.(*raftClusterCtlStore); ok {
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

	return groupMgr, wal, resolver
}

func buildFactories(logger *slog.Logger, homeDir, vaultsDir string, cfgStore system.Store, orch *orchestrator.Orchestrator, certMgr *cert.Manager, slogCh <-chan logging.CapturedRecord, slogCapture *logging.CaptureHandler, groupMgr *raftgroup.GroupManager, nodeAddrResolver func(string) (string, bool), nodeID string) orchestrator.Factories {
	reg := func(factory orchestrator.IngesterFactory, defaults func() map[string]string, tester orchestrator.ConnectionTester) orchestrator.IngesterRegistration {
		return orchestrator.IngesterRegistration{Factory: factory, Defaults: defaults, Tester: tester}
	}
	regHA := func(factory orchestrator.IngesterFactory, defaults func() map[string]string, tester orchestrator.ConnectionTester) orchestrator.IngesterRegistration {
		return orchestrator.IngesterRegistration{Factory: factory, Defaults: defaults, Tester: tester, SingletonSupported: true}
	}
	listen := func(factory orchestrator.IngesterFactory, defaults func() map[string]string, addrs func(map[string]string) []orchestrator.ListenAddr) orchestrator.IngesterRegistration {
		return orchestrator.IngesterRegistration{Factory: factory, Defaults: defaults, ListenAddrs: addrs}
	}
	// SingletonSupported table (see gastrolog-2kcw4):
	//   chatterbox / scatterbox  — synthetic, both parallel and singleton-with-failover are legitimate
	//   kafka / mqtt             — depends on broker setup (consumer group / shared subscription)
	//   docker / self / tail / metrics — per-node-local source, singleton would hide 3/4 of cluster data
	//   listeners                — OS-level port coordination, concept doesn't apply
	ingesterTypes := map[string]orchestrator.IngesterRegistration{
		"chatterbox": regHA(chatterbox.NewIngester, chatterbox.ParamDefaults, nil),
		"scatterbox": regHA(scatterbox.NewFactory(nodeID), scatterbox.ParamDefaults, nil),
		"docker": reg(ingestdocker.NewFactory(cfgStore), ingestdocker.ParamDefaults,
			func(ctx context.Context, params map[string]string) (string, error) {
				return ingestdocker.TestConnection(ctx, params, cfgStore)
			}),
		"fluentfwd": listen(ingestfluentfwd.NewFactory(), ingestfluentfwd.ParamDefaults, ingestfluentfwd.ListenAddrs),
		"http":      listen(ingesthttp.NewFactory(), ingesthttp.ParamDefaults, ingesthttp.ListenAddrs),
		"kafka":     regHA(ingestkafka.NewFactory(), ingestkafka.ParamDefaults, ingestkafka.TestConnection),
		"mqtt":      regHA(ingestmqtt.NewFactory(), ingestmqtt.ParamDefaults, ingestmqtt.TestConnection),
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

func wireClusterRaftApplies(clusterSrv *cluster.Server, groupMgr *raftgroup.GroupManager) {
	if clusterSrv == nil || groupMgr == nil {
		return
	}
	fn := func(_ context.Context, groupID string, data []byte) error {
		g := groupMgr.GetGroup(groupID)
		if g == nil {
			return fmt.Errorf("raft group %s not found", groupID)
		}
		return g.Raft.Apply(data, cluster.ReplicationTimeout).Error()
	}
	clusterSrv.SetGroupApplyFn(fn)
}

func resolveHome(flagValue string) (home.Dir, error) {
	if flagValue != "" {
		return home.New(flagValue), nil
	}
	return home.Default()
}

func buildTokenService(ctx context.Context, cfgStore system.Store) (*auth.TokenService, error) {
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

// openConfigStore creates a system.Store based on config type.
func openConfigStore(configType string, opts raftStoreOpts) (system.Store, error) {
	switch configType {
	case "memory":
		return sysmem.NewStore(), nil
	case "raft":
		return openRaftClusterCtlStore(opts)
	default:
		return nil, fmt.Errorf("unknown config store type: %q", configType)
	}
}
