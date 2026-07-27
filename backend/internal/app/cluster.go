package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const errFmtSaveClusterTLS = "save cluster TLS file: %w"

// setupCluster handles cluster enrollment and cluster server creation.
// Always creates cluster infra for raft mode. Returns nil for non-raft modes.
func setupCluster(ctx context.Context, logger *slog.Logger, cfg RunConfig, hd home.Dir, nodeID string) (*cluster.Server, *cluster.ClusterTLS, error) {
	if cfg.ConfigType != "raft" {
		return nil, nil, nil
	}

	clusterTLS := cluster.NewClusterTLS()

	// Joining flow: enroll with the leader before creating the cluster server.
	// gastrolog-o9z6o: cfg.JoinToken may have been populated from a
	// file or HTTP source by resolveJoinTokenFromSources before we got
	// here, so the literal-token check still does the right thing.
	if cfg.JoinAddr != "" && cfg.JoinToken != "" {
		enrolled, err := enrollInCluster(ctx, logger, cfg, hd, nodeID)
		if err != nil {
			return nil, nil, err
		}
		clusterTLS = enrolled
	}

	// Restart: load existing TLS from disk.
	if clusterTLS.State() == nil {
		if found, err := clusterTLS.LoadFile(hd.ClusterTLSPath()); err != nil {
			return nil, nil, fmt.Errorf("load cluster TLS file: %w", err)
		} else if found {
			logger.Info("cluster TLS loaded from local file")
		}
	}

	clusterSrv, err := cluster.New(cluster.Config{
		ClusterAddr:           cfg.ClusterAddr,
		LocalAddr:             cfg.ClusterAdvertise,
		NodeID:                nodeID,
		TLS:                   clusterTLS,
		ByteMetrics:           cluster.NewPeerByteMetrics(),
		ServicePoolMaxPerPeer: cfg.ServicePoolMaxPerPeer,
		Logger:                compCluster.Apply(logger),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("create cluster server: %w", err)
	}

	return clusterSrv, clusterTLS, nil
}

// enrollInCluster performs the Enroll RPC to obtain TLS material from the leader.
func enrollInCluster(ctx context.Context, logger *slog.Logger, cfg RunConfig, hd home.Dir, nodeID string) (*cluster.ClusterTLS, error) {
	tokenSecret, caHash, err := tlsutil.ParseJoinToken(cfg.JoinToken)
	if err != nil {
		return nil, fmt.Errorf("parse join token: %w", err)
	}

	logger.Info("enrolling with cluster leader", "leader_addr", cfg.JoinAddr)
	enrollCtx, enrollCancel := context.WithTimeout(ctx, 30*time.Second)
	result, err := cluster.Enroll(enrollCtx, cfg.JoinAddr, tokenSecret, caHash, nodeID, cfg.advertisedClusterAddr())
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

// startClusterServices bootstraps TLS if needed and starts the cluster gRPC server.
func startClusterServices(ctx context.Context, clusterSrv *cluster.Server, clusterTLS *cluster.ClusterTLS, cfgStore system.Store, hd home.Dir, logger *slog.Logger, writeBootstrapTokenPath string) error {
	if clusterSrv == nil {
		return nil
	}

	if clusterTLS.State() == nil {
		if err := bootstrapClusterTLS(ctx, cfgStore, clusterTLS, hd.ClusterTLSPath(), logger, writeBootstrapTokenPath); err != nil {
			return fmt.Errorf("bootstrap cluster TLS: %w", err)
		}
	}

	clusterSrv.SetEnrollHandler(makeEnrollHandler(cfgStore, logger))
	return clusterSrv.Start()
}

// bootstrapClusterTLS generates CA, cluster cert, and join token. When
// writeBootstrapTokenPath is non-empty, the token is also written
// atomically to that path with mode 0600 so an orchestrator-launched
// joiner can pick it up via --bootstrap-token-file (gastrolog-o9z6o).
func bootstrapClusterTLS(ctx context.Context, cfgStore system.Store, ctls *cluster.ClusterTLS, tlsFilePath string, logger *slog.Logger, writeBootstrapTokenPath string) error {
	existingCfg, err := cfgStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("check existing cluster TLS: %w", err)
	}
	if existingCfg != nil && existingCfg.Runtime.ClusterTLS != nil {
		return loadExistingClusterTLS(existingCfg.Runtime.ClusterTLS, ctls, tlsFilePath, logger, writeBootstrapTokenPath)
	}

	ca, err := tlsutil.GenerateCA()
	if err != nil {
		return fmt.Errorf("generate CA: %w", err)
	}
	cert, err := tlsutil.GenerateClusterCert(ca.CertPEM, ca.KeyPEM, cluster.LaneSANs)
	if err != nil {
		return fmt.Errorf("generate cluster cert: %w", err)
	}
	token, err := tlsutil.GenerateJoinToken(ca.CertPEM)
	if err != nil {
		return fmt.Errorf("generate join token: %w", err)
	}

	if err := cfgStore.PutClusterTLS(ctx, system.ClusterTLS{
		CACertPEM:      string(ca.CertPEM),
		CAKeyPEM:       string(ca.KeyPEM),
		ClusterCertPEM: string(cert.CertPEM),
		ClusterKeyPEM:  string(cert.KeyPEM),
		JoinToken:      token,
	}); err != nil {
		return fmt.Errorf("store cluster TLS: %w", err)
	}

	if err := ctls.Load(cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		return fmt.Errorf("load cluster TLS: %w", err)
	}

	if err := cluster.SaveFile(tlsFilePath, cert.CertPEM, cert.KeyPEM, ca.CertPEM); err != nil {
		return fmt.Errorf(errFmtSaveClusterTLS, err)
	}

	logger.Info("cluster TLS bootstrapped")
	logger.Info("cluster join token (use --join-token to join)", "token", token)

	if writeBootstrapTokenPath != "" {
		if err := writeBootstrapTokenAtomic(writeBootstrapTokenPath, token); err != nil {
			return fmt.Errorf("write bootstrap token: %w", err)
		}
		logger.Info("bootstrap token written to file", "path", writeBootstrapTokenPath)
	}

	return nil
}

// loadExistingClusterTLS handles the restart path: cluster TLS material
// already exists in the store from a prior run, so reuse it instead of
// regenerating. Also writes the join token to the configured path if
// requested. Extracted from bootstrapClusterTLS to keep both branches
// flat per the project's nestif lint rule.
func loadExistingClusterTLS(existing *system.ClusterTLS, ctls *cluster.ClusterTLS, tlsFilePath string, logger *slog.Logger, writeBootstrapTokenPath string) error {
	if err := ctls.Load([]byte(existing.ClusterCertPEM), []byte(existing.ClusterKeyPEM), []byte(existing.CACertPEM)); err != nil {
		return fmt.Errorf("load existing cluster TLS: %w", err)
	}
	if err := cluster.SaveFile(tlsFilePath, []byte(existing.ClusterCertPEM), []byte(existing.ClusterKeyPEM), []byte(existing.CACertPEM)); err != nil {
		return fmt.Errorf(errFmtSaveClusterTLS, err)
	}
	logger.Info("cluster TLS loaded from existing config")
	_, caHash, _ := tlsutil.ParseJoinToken(existing.JoinToken)
	logger.Info("cluster join token", "token", existing.JoinToken, "ca_hash", caHash)
	if writeBootstrapTokenPath != "" {
		if err := writeBootstrapTokenAtomic(writeBootstrapTokenPath, existing.JoinToken); err != nil {
			return fmt.Errorf("write bootstrap token: %w", err)
		}
		logger.Info("bootstrap token written to file", "path", writeBootstrapTokenPath)
	}
	return nil
}

// makeEnrollHandler creates the Enroll RPC handler for the cluster server.
func makeEnrollHandler(cfgStore system.Store, logger *slog.Logger) cluster.EnrollHandler {
	return func(ctx context.Context, req *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error) {
		cfg, err := cfgStore.Load(ctx)
		if err != nil || cfg == nil || cfg.Runtime.ClusterTLS == nil {
			logger.Error("enroll: read cluster TLS", "error", err)
			return nil, errors.New("cluster TLS not available")
		}
		tls := cfg.Runtime.ClusterTLS

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

// makeJoinRollback creates a rollback function that restores the old raft
// directory from backup and reopens the old config store.
func makeJoinRollback(
	proxy *system.StoreProxy,
	clusterSrv *cluster.Server,
	clusterTLS *cluster.ClusterTLS,
	hd home.Dir,
	nodeID, raftDir, backupDir string,
	disp *configDispatcher,
	logger *slog.Logger,
) func() {
	return func() {
		logger.Warn("rolling back: restoring raft directory from backup")
		if err := os.Rename(backupDir, raftDir); err != nil {
			logger.Error("rollback: restore raft dir failed", "error", err)
			return
		}
		oldStore, err := openRaftClusterCtlStore(raftStoreOpts{
			Home: hd, NodeID: nodeID,
			ClusterSrv: clusterSrv, ClusterTLS: clusterTLS,
			Logger: logger, FSMOpts: []raftfsm.Option{raftfsm.WithOnApply(disp.Handle)},
		})
		if err != nil {
			logger.Error("rollback: reopen old store failed", "error", err)
			proxy.ClearJoining()
			return
		}
		proxy.Swap(oldStore)
		clusterSrv.SetApplyFn(func(ctx context.Context, data []byte) (uint64, error) {
			return oldStore.raftStore.ApplyRaw(data)
		})
		clusterSrv.SetEnrollHandler(makeEnrollHandler(proxy, logger))
		if err := clusterSrv.Start(); err != nil {
			logger.Error("rollback: restart cluster server failed", "error", err)
		}
	}
}

// cleanOrchestrator removes all vaults and ingesters from the orchestrator.
func cleanOrchestrator(orch *orchestrator.Orchestrator, logger *slog.Logger) {
	for _, vaultID := range orch.ListVaults() {
		if err := orch.ForceRemoveVault(vaultID); err != nil {
			logger.Warn("join cleanup: remove vault failed", "vault_id", vaultID, "error", err)
		}
	}
	// Drop every ingester by reconciling to an empty desired set.
	if err := orch.ReconcileIngesters(nil); err != nil {
		logger.Warn("join cleanup: reconcile ingesters to empty failed", "error", err)
	}
}

// restartClusterWithStore configures the cluster server to use the given config
// store's raft instance and starts the gRPC server.
func restartClusterWithStore(store *raftClusterCtlStore, proxy *system.StoreProxy, clusterSrv *cluster.Server, logger *slog.Logger) error {
	clusterSrv.SetApplyFn(func(ctx context.Context, data []byte) (uint64, error) {
		return store.raftStore.ApplyRaw(data)
	})
	clusterSrv.SetEnrollHandler(makeEnrollHandler(proxy, logger))
	if err := clusterSrv.Start(); err != nil {
		return fmt.Errorf("restart cluster server: %w", err)
	}
	logger.Info("cluster server restarted")
	return nil
}

// validateSingleNodeCluster checks that the proxy wraps a raft store and
// the cluster has exactly one node (self).
func validateSingleNodeCluster(proxy *system.StoreProxy, clusterSrv *cluster.Server, nodeID string) (*raftClusterCtlStore, error) {
	rcs, ok := proxy.Inner().(*raftClusterCtlStore)
	if !ok {
		return nil, errors.New("runtime cluster join requires raft system store")
	}
	servers, err := clusterSrv.Servers()
	if err != nil {
		return nil, fmt.Errorf("get raft servers: %w", err)
	}
	if len(servers) != 1 || servers[0].ID != nodeID {
		return nil, errors.New("runtime cluster join requires a single-node cluster")
	}
	return rcs, nil
}

// makeJoinClusterFunc creates the callback for the JoinCluster RPC.
func makeJoinClusterFunc(
	proxy *system.StoreProxy,
	clusterSrv *cluster.Server,
	clusterTLS *cluster.ClusterTLS,
	hd home.Dir,
	nodeID string,
	clusterAddr string,
	orch *orchestrator.Orchestrator,
	disp *configDispatcher,
	logger *slog.Logger,
) func(ctx context.Context, leaderAddr, joinToken string) error {
	return func(ctx context.Context, leaderAddr, joinToken string) error {
		logger.Info("runtime cluster join starting", "leader_addr", leaderAddr)

		rcs, err := validateSingleNodeCluster(proxy, clusterSrv, nodeID)
		if err != nil {
			return err
		}

		// 1. Parse join token
		tokenSecret, caHash, err := tlsutil.ParseJoinToken(joinToken)
		if err != nil {
			return fmt.Errorf("parse join token: %w", err)
		}

		// 2. Enroll with remote leader
		logger.Info("enrolling with remote leader", "leader_addr", leaderAddr)
		enrollCtx, enrollCancel := context.WithTimeout(ctx, 30*time.Second)
		result, err := cluster.Enroll(enrollCtx, leaderAddr, tokenSecret, caHash, nodeID, clusterAddr)
		enrollCancel()
		if err != nil {
			return fmt.Errorf("cluster enrollment: %w", err)
		}

		// 3. Hot-swap TLS
		if err := clusterTLS.Load(result.ClusterCertPEM, result.ClusterKeyPEM, result.CACertPEM); err != nil {
			return fmt.Errorf("load enrolled TLS material: %w", err)
		}
		if err := cluster.SaveFile(hd.ClusterTLSPath(), result.ClusterCertPEM, result.ClusterKeyPEM, result.CACertPEM); err != nil {
			return fmt.Errorf("save cluster TLS: %w", err)
		}
		logger.Info("TLS material swapped")

		// 4. Mark proxy as joining
		proxy.SetJoining()

		// 5. Close old raft system store
		logger.Info("closing old raft system store")
		if err := rcs.Close(); err != nil {
			proxy.ClearJoining()
			return fmt.Errorf("close old raft store: %w", err)
		}

		// 6. Backup old raft directory
		raftDir := hd.RaftDir()
		backupDir := raftDir + ".bak." + strconv.FormatInt(time.Now().UnixMilli(), 10)
		logger.Info("backing up old raft directory", "from", raftDir, "to", backupDir)
		if err := os.Rename(raftDir, backupDir); err != nil {
			proxy.ClearJoining()
			return fmt.Errorf("rename raft dir: %w", err)
		}

		rollback := makeJoinRollback(proxy, clusterSrv, clusterTLS, hd, nodeID, raftDir, backupDir, disp, logger)

		// 7. PrepareRejoin
		logger.Info("preparing cluster server for rejoin")
		newTransport, err := clusterSrv.PrepareRejoin()
		if err != nil {
			rollback()
			return fmt.Errorf("prepare rejoin: %w", err)
		}

		// 8. Open new raft system store
		logger.Info("opening new raft system store")
		newStore, err := openRaftClusterCtlStore(raftStoreOpts{
			Home: hd, NodeID: nodeID, JoinAddr: leaderAddr,
			ClusterSrv: clusterSrv, ClusterTLS: clusterTLS,
			Logger: logger, FSMOpts: []raftfsm.Option{raftfsm.WithOnApply(disp.Handle)},
			transport: newTransport,
		})
		if err != nil {
			rollback()
			return fmt.Errorf("open new raft store: %w", err)
		}

		// 9. Swap proxy
		proxy.Swap(newStore)
		logger.Info("config store swapped")

		// 10. Clean up orchestrator
		cleanOrchestrator(orch, logger)

		// 11. Restart cluster gRPC
		if err := restartClusterWithStore(newStore, proxy, clusterSrv, logger); err != nil {
			return err
		}

		// 12. Request membership as a nonvoter (learner). This is a
		// runtime single-node-to-cluster join — the joining node is
		// new to the target cluster's Raft membership regardless of
		// its own local state, so the safe shape is to enter as a
		// learner and let the cluster-ctl promoter (gastrolog-2czh9)
		// upgrade to voter once caught up.
		logger.Info("requesting cluster membership (as learner)", "leader_addr", leaderAddr)
		joinCtx, joinCancel := context.WithTimeout(ctx, 30*time.Second)
		err = cluster.JoinCluster(joinCtx, logger, leaderAddr, nodeID, clusterAddr, clusterTLS, false)
		joinCancel()
		if err != nil {
			return fmt.Errorf("join cluster: %w", err)
		}
		logger.Info("cluster membership granted (as learner; promoter will upgrade once caught up)")

		// 13. Wait for config replication
		logger.Info("waiting for config replication from leader")
		if err := waitForServerSettings(ctx, proxy, 60*time.Second, logger); err != nil {
			return fmt.Errorf("wait for config replication: %w", err)
		}

		// 14. Ensure node name
		if _, err := ensureNodeConfig(ctx, proxy, nodeID, ""); err != nil {
			logger.Warn("failed to write node name after join", "error", err)
		}

		logger.Info("runtime cluster join complete")
		return nil
	}
}

// makeEvictionHandler creates the callback invoked when this node is evicted
// from the cluster. Reinitializes as a fresh single-node cluster.
func makeEvictionHandler(
	proxy *system.StoreProxy,
	clusterSrv *cluster.Server,
	clusterTLS *cluster.ClusterTLS,
	hd home.Dir,
	nodeID string,
	orch *orchestrator.Orchestrator,
	disp *configDispatcher,
	logger *slog.Logger,
) func() {
	return func() {
		logger.Warn("evicted from cluster — reinitializing as single-node")

		rcs, ok := proxy.Inner().(*raftClusterCtlStore)
		if !ok {
			logger.Error("eviction reinit: config store is not raft-backed, shutting down instead")
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		}

		proxy.SetJoining()

		logger.Info("eviction reinit: closing old raft system store")
		if err := rcs.Close(); err != nil {
			logger.Error("eviction reinit: close old store failed, shutting down", "error", err)
			proxy.ClearJoining()
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		}

		raftDir := hd.RaftDir()
		backupDir := raftDir + ".bak." + strconv.FormatInt(time.Now().UnixMilli(), 10)
		logger.Info("eviction reinit: backing up old raft directory", "from", raftDir, "to", backupDir)
		if err := os.Rename(raftDir, backupDir); err != nil {
			logger.Error("eviction reinit: rename raft dir failed, shutting down", "error", err)
			proxy.ClearJoining()
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		}

		logger.Info("eviction reinit: preparing cluster server for reinit")
		newTransport, err := clusterSrv.PrepareRejoin()
		if err != nil {
			logger.Error("eviction reinit: prepare rejoin failed, shutting down", "error", err)
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		}

		logger.Info("eviction reinit: opening fresh raft system store")
		newStore, err := openRaftClusterCtlStore(raftStoreOpts{
			Home: hd, NodeID: nodeID,
			ClusterSrv: clusterSrv, ClusterTLS: clusterTLS,
			Logger: logger, FSMOpts: []raftfsm.Option{raftfsm.WithOnApply(disp.Handle)},
			transport: newTransport,
		})
		if err != nil {
			logger.Error("eviction reinit: open new store failed, shutting down", "error", err)
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		}

		proxy.Swap(newStore)
		logger.Info("eviction reinit: config store swapped")

		cleanOrchestrator(orch, logger)

		if err := restartClusterWithStore(newStore, proxy, clusterSrv, logger); err != nil {
			logger.Error("eviction reinit: restart cluster server failed, shutting down", "error", err)
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		}

		logger.Info("eviction reinit complete — running as single-node cluster")
	}
}

// nodeRemover is the leader-side removal serializer: one instance per
// node, shared by every entry point that can start a removal here (the
// local-leader call and the ForwardRemoveNode handler). Removal requests
// arrive in bursts — kubectl scale fires preStop on many pods at once —
// and a gate is only as good as the state it reads, so remove() holds mu
// across BOTH the gate evaluation and the removal itself. Request N+1
// therefore re-reads the FSM after request N has committed its Raft
// membership change and deleted the departed NodeConfig, instead of
// clearing a gate computed against the same pre-removal snapshot.
type nodeRemover struct {
	mu       sync.Mutex
	cfgStore system.Store
	logger   *slog.Logger
	// execute performs the removal proper once the gates pass: Raft
	// membership change, FSM node-config cleanup, eviction notification.
	execute func(ctx context.Context, targetNodeID string, opts cluster.RemoveNodeOptions) error
}

// remove runs the leader-side removal gates and, if they pass, the
// removal — serialized against every other removal on this node.
func (r *nodeRemover) remove(ctx context.Context, targetNodeID string, opts cluster.RemoveNodeOptions) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Gates run on the leader, inside the serializer, so the placement
	// state they read is the cluster's authoritative view AND reflects
	// every removal already committed by this node.
	if err := evaluateRemovalGates(ctx, r.cfgStore, targetNodeID, opts, r.logger); err != nil {
		return err
	}
	return r.execute(ctx, targetNodeID, opts)
}

// makeRemoveNodeFunc creates the callback for the RemoveNode RPC. The
// returned function runs the leader-side removal gates — orphan-refusal
// (gastrolog-2ch9y) and RF-preservation (gastrolog-3vyex) — before the
// Raft membership change, so a removal that would destroy or degrade a
// vault fails with an operator-actionable error listing the affected
// vaults. opts.Force skips both gates, loudly logged. When this node is
// not the leader the request is forwarded, policy and all, to the node
// that owns the gates.
func makeRemoveNodeFunc(
	clusterSrv *cluster.Server,
	cfgStore system.Store,
	nodeID string,
	logger *slog.Logger,
) cluster.RemoveNodeFunc {
	execute := func(ctx context.Context, targetNodeID string, opts cluster.RemoveNodeOptions) error {
		peerConns := clusterSrv.PeerConns()
		var evictHandle cluster.PeerConnHandle
		if peerConns != nil {
			if h, err := peerConns.AcquireService(targetNodeID, cluster.PurposeEviction); err == nil {
				evictHandle = h
			} else {
				logger.Warn("cannot pre-connect to evicted node for notification", "error", err)
			}
		}

		logger.Info("removing node from cluster", "node_id", targetNodeID, "force", opts.Force, "policy", opts.Policy)
		if err := clusterSrv.RemoveServer(targetNodeID, 10*time.Second); err != nil {
			return fmt.Errorf("remove server: %w", err)
		}
		logger.Info("node removed from cluster", "node_id", targetNodeID)

		// Also delete the system-FSM NodeConfig so that downstream consumers
		// (placement manager, RefreshVaultCtlMembers, ListNodes RPC) stop
		// treating the removed node as a cluster member. Without this, a
		// scale-down leaves stale NodeConfig entries that keep vault-ctl Raft
		// groups attempting to talk to defunct pod IPs. See gastrolog-4zy8a.
		if cfgStore != nil {
			targetGLID, err := glid.Parse(targetNodeID)
			if err != nil {
				logger.Warn("delete node config: parse node ID failed", "node_id", targetNodeID, "error", err)
			} else if err := cfgStore.DeleteNode(ctx, targetGLID); err != nil {
				logger.Warn("delete node config failed", "node_id", targetNodeID, "error", err)
			}
		}

		if evictHandle != nil {
			go func() {
				defer evictHandle.Release()
				notifyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				client := cluster.NewNotifyEvictionClient(evictHandle.GRPC())
				if err := client.NotifyEviction(notifyCtx, "removed from cluster by leader"); err != nil {
					logger.Warn("failed to notify evicted node", "node_id", targetNodeID, "error", err)
				} else {
					logger.Info("eviction notification sent", "node_id", targetNodeID)
				}
				peerConns.Invalidate(targetNodeID, status.Error(codes.Unavailable, "node evicted from cluster"))
			}()
		}

		return nil
	}

	remover := &nodeRemover{cfgStore: cfgStore, logger: logger, execute: execute}
	clusterSrv.SetRemoveNodeFn(remover.remove)

	return func(ctx context.Context, targetNodeID string, opts cluster.RemoveNodeOptions) error {
		_, leaderID := clusterSrv.LeaderInfo()

		if leaderID == nodeID {
			return remover.remove(ctx, targetNodeID, opts)
		}

		if leaderID == "" {
			return errors.New("no leader available")
		}
		peerConns := clusterSrv.PeerConns()
		if peerConns == nil {
			return errors.New("peer connections not available")
		}
		logger.Info("forwarding node removal to leader",
			"leader_id", leaderID, "target_node_id", targetNodeID,
			"force", opts.Force, "policy", opts.Policy)
		// The policy rides along: the gates run on the leader, so a
		// preStop self-removal that lands on a follower must still be
		// evaluated optimistically there (gastrolog-3vyex).
		req := &gastrologv1.ForwardRemoveNodeRequest{
			NodeId:      []byte(targetNodeID),
			Force:       opts.Force,
			SelfRemoval: opts.Policy == cluster.RemovalPolicySelf,
		}
		resp := &gastrologv1.ForwardRemoveNodeResponse{}
		if err := peerConns.InvokeService(ctx, leaderID, cluster.PurposeRemoveNode,
			"/gastrolog.v1.ClusterService/ForwardRemoveNode", req, resp); err != nil {
			return fmt.Errorf("forward remove node to leader %s: %w", leaderID, err)
		}
		return nil
	}
}

// evaluateRemovalGates runs every leader-side removal gate against the
// CURRENT FSM state and returns a refusal error, or nil to let the
// removal proceed. Callers must hold the removal serializer: both gates
// read placement state here, at call time, so back-to-back removals are
// evaluated against the state each one actually leaves behind.
//
// Gate order is severity order — orphan (total data loss) before
// RF-preservation (reduced redundancy) — so an operator staring at one
// error sees the worse consequence first.
func evaluateRemovalGates(
	ctx context.Context,
	cfgStore system.Store,
	targetNodeID string,
	opts cluster.RemoveNodeOptions,
	logger *slog.Logger,
) error {
	// Orphan-refusal gate (gastrolog-2ch9y): removal would leave a vault
	// with zero placements. Applies to every removal policy — losing a
	// vault entirely is never an acceptable side effect of a pod
	// terminating.
	if orphans := vaultsOrphanedByRemoval(ctx, cfgStore, targetNodeID); len(orphans) > 0 {
		if !opts.Force {
			return orphanRefusalError(targetNodeID, orphans)
		}
		logger.Warn("FORCE REMOVE: bypassing orphan-refusal gate — data loss",
			"node_id", targetNodeID,
			"orphaned_vaults", orphans)
	}

	// RF-preservation gate (gastrolog-3vyex): removal would leave a
	// vault with fewer surviving placements than its replication factor,
	// and no eligible Live node is available to re-place onto.
	degraded := vaultsBelowRFAfterRemoval(ctx, cfgStore, targetNodeID)
	if len(degraded) == 0 {
		return nil
	}
	switch {
	case opts.Policy == cluster.RemovalPolicySelf:
		// Optimistic: the node is on its way out regardless, so refusing
		// buys nothing and leaves a stranded voter behind. Placement
		// reconcile re-places these vaults as soon as an eligible node
		// exists; until then the under-replication alarm carries the
		// condition.
		logger.Warn("self-removal proceeds below replication factor — placement reconcile will re-place when an eligible node exists",
			"node_id", targetNodeID,
			"degraded_vaults", degraded)
	case !opts.Force:
		return rfRefusalError(targetNodeID, degraded)
	default:
		logger.Warn("FORCE REMOVE: bypassing RF-preservation gate — reduced redundancy",
			"node_id", targetNodeID,
			"degraded_vaults", degraded)
	}
	return nil
}

// orphanedVault describes one vault that would be orphaned by removing a
// node from the cluster. Carried through the error so the operator
// sees exactly which vaults are at risk.
type orphanedVault struct {
	ID   glid.GLID
	Name string
}

// vaultsOrphanedByRemoval returns the vaults whose entire placement set
// lives on targetNodeID — removing the node would leave them with zero
// surviving placements (i.e. data loss). Used by the orphan-refusal
// gate. An empty return means the removal is safe to proceed.
//
// Operates on placement-level granularity: if a vault's placements list
// contains only storages on targetNodeID, the vault is orphaned. This
// is conservative compared to a chunk-level check (which would walk
// every vault-ctl FSM manifest), but it catches the bug class the
// gate is designed for: RF=1 vaults whose sole holder is being
// decommissioned. Higher-RF vaults with all placements alive elsewhere
// are correctly allowed through.
func vaultsOrphanedByRemoval(ctx context.Context, cfgStore system.Store, targetNodeID string) []orphanedVault {
	if cfgStore == nil {
		return nil
	}
	vaults, err := cfgStore.ListVaults(ctx)
	if err != nil {
		return nil
	}
	nscs, err := cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}
	var orphans []orphanedVault
	for _, v := range vaults {
		placements, err := cfgStore.GetVaultPlacements(ctx, v.ID)
		if err != nil || len(placements) == 0 {
			continue
		}
		// Build the set of distinct nodes holding any placement for
		// this vault. If the only node in the set is the one being
		// removed, the vault is orphaned.
		holders := make(map[string]bool, len(placements))
		for _, p := range placements {
			nid := system.NodeIDForStorage(p.StorageID, nscs)
			if nid != "" {
				holders[nid] = true
			}
		}
		if len(holders) == 1 && holders[targetNodeID] {
			orphans = append(orphans, orphanedVault{ID: v.ID, Name: v.Name})
		}
	}
	return orphans
}

// orphanRefusalError builds the operator-actionable error message
// returned when the orphan-refusal gate refuses removal. Lists each
// affected vault by name and ID so the operator can either drain
// those vaults to other nodes (preferred) or re-run with --force
// (acknowledged data loss).
func orphanRefusalError(targetNodeID string, orphans []orphanedVault) error {
	names := make([]string, 0, len(orphans))
	for _, v := range orphans {
		names = append(names, fmt.Sprintf("%q (%s)", v.Name, v.ID))
	}
	return fmt.Errorf(
		"refusing to remove node %s: would orphan %d vault(s): %s — "+
			"drain these vaults to other nodes first, or re-run with --force to acknowledge data loss",
		targetNodeID, len(orphans), strings.Join(names, ", "))
}

// ErrWouldDropBelowRF is the sentinel wrapped by every RF-preservation
// refusal, so callers can distinguish "this removal degrades redundancy"
// from a genuine internal failure without matching on message text.
var ErrWouldDropBelowRF = errors.New("removal would drop a vault below its replication factor")

// degradedVault describes one vault whose surviving placements after a
// removal would be fewer than its configured replication factor, with
// too few eligible Live nodes to restore it. Carried through the error
// and the force-bypass log line so the operator sees exactly which
// vaults lose redundancy and by how much.
type degradedVault struct {
	ID glid.GLID
	// Name is the vault name at gate time.
	Name string
	// RF is the vault's configured replication factor (unset == 1).
	RF int
	// Surviving is the number of distinct nodes still holding a
	// placement after the removal.
	Surviving int
	// Eligible is the number of Live nodes that could take a new
	// placement for this vault (not the target, not already a member,
	// storage-class match per the placement manager's own rule).
	Eligible int
}

// vaultsBelowRFAfterRemoval returns the vaults that removing
// targetNodeID would leave below their replication factor with no way
// back. Used by the RF-preservation gate (gastrolog-3vyex). An empty
// return means redundancy survives the removal — either it was already
// satisfied without the target, or the placement manager has somewhere
// to re-place.
//
// A vault is reported only when surviving + eligible < RF: one eligible
// Live node that can take a replacement placement is enough to clear a
// single lost member, and the placement manager will do exactly that on
// its next reconcile. Counting eligible nodes rather than merely asking
// "is there at least one" matters at RF>=3 — a vault two members short
// with one spare node is still below RF after re-placement, and the
// operator should hear about it before the node goes away, not from the
// under-replication alarm afterwards.
//
// Eligibility is the placement manager's own rule
// (nodeEligibleForVault) intersected with NodeStateLive and
// "not already a placement member": the same three conditions
// placeFollowers applies when it picks a backfill target. Soft-offline
// and in-transition nodes (Unreachable, Maintenance, Draining,
// Decommissioning) are deliberately not counted — placement won't put
// new members there either.
func vaultsBelowRFAfterRemoval(ctx context.Context, cfgStore system.Store, targetNodeID string) []degradedVault {
	if cfgStore == nil {
		return nil
	}
	vaults, err := cfgStore.ListVaults(ctx)
	if err != nil {
		return nil
	}
	nscs, err := cfgStore.ListNodeStorageConfigs(ctx)
	if err != nil {
		return nil
	}
	nodes, err := cfgStore.ListNodes(ctx)
	if err != nil {
		return nil
	}

	liveNodes := liveReplacementCandidates(nodes, targetNodeID)
	members := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		members[n.ID.String()] = true
	}

	var degraded []degradedVault
	for _, v := range vaults {
		placements, err := cfgStore.GetVaultPlacements(ctx, v.ID)
		if err != nil || len(placements) == 0 {
			continue
		}
		if d, below := vaultBelowRFAfterRemoval(v, placements, nscs, liveNodes, members, targetNodeID); below {
			degraded = append(degraded, d)
		}
	}
	return degraded
}

// liveReplacementCandidates returns the node IDs that could take a new
// placement after targetNodeID leaves: everything in NodeStateLive
// except the departing node. Soft-offline and in-transition states
// (Unreachable, Maintenance, Draining, Decommissioning) are excluded —
// the placement manager won't put new members there either.
func liveReplacementCandidates(nodes []system.NodeConfig, targetNodeID string) []string {
	var live []string
	for _, n := range nodes {
		id := n.ID.String()
		if id == targetNodeID || n.EffectiveState() != system.NodeStateLive {
			continue
		}
		live = append(live, id)
	}
	return live
}

// vaultBelowRFAfterRemoval evaluates one vault: does removing
// targetNodeID leave it with fewer members than its replication factor,
// counting the Live nodes that could still be re-placed onto? Returns
// the populated degradedVault and true when the vault is below RF with
// no way back.
//
// members is the current cluster node set. A placement whose node is no
// longer a member does not count as surviving — it is a leftover the
// placement manager has yet to clean up, not a copy of the data. This
// is what makes back-to-back removals add up: the previous removal
// deleted that NodeConfig, so this one sees one fewer member. Synthetic
// (memory-vault) storage IDs encode their node directly and would
// otherwise keep resolving to a node that has already left.
func vaultBelowRFAfterRemoval(
	v system.VaultConfig,
	placements []system.VaultPlacement,
	nscs []system.NodeStorageConfig,
	liveNodes []string,
	members map[string]bool,
	targetNodeID string,
) (degradedVault, bool) {
	surviving := make(map[string]bool, len(placements))
	for _, nid := range system.PlacementNodeIDs(placements, nscs) {
		if nid != targetNodeID && members[nid] {
			surviving[nid] = true
		}
	}
	rf := int(v.ReplicationFactor)
	if rf <= 0 {
		rf = 1 // unset RF means a single copy
	}
	if len(surviving) >= rf {
		return degradedVault{}, false
	}
	eligible := 0
	for _, nid := range liveNodes {
		if surviving[nid] {
			continue // already holds a placement — not a new member
		}
		if nodeEligibleForVault(v, nid, nscs, placements) {
			eligible++
		}
	}
	if len(surviving)+eligible >= rf {
		return degradedVault{}, false
	}
	return degradedVault{
		ID:        v.ID,
		Name:      v.Name,
		RF:        rf,
		Surviving: len(surviving),
		Eligible:  eligible,
	}, true
}

// rfRefusalError builds the operator-actionable error the
// RF-preservation gate returns. Names every affected vault with the
// redundancy arithmetic (surviving vs configured RF, eligible spares)
// so the operator can pick a remedy: add an eligible node, drain the
// vault, or re-run with --force to accept reduced redundancy. Wraps
// ErrWouldDropBelowRF and keeps the "refusing to remove node" prefix
// the RPC layer maps to FailedPrecondition.
func rfRefusalError(targetNodeID string, degraded []degradedVault) error {
	parts := make([]string, 0, len(degraded))
	for _, v := range degraded {
		parts = append(parts, fmt.Sprintf(
			"%q (%s): %d of %d replicas would survive, %d eligible node(s) to re-place onto",
			v.Name, v.ID, v.Surviving, v.RF, v.Eligible))
	}
	return fmt.Errorf(
		"refusing to remove node %s: %w — %d vault(s) affected: %s — "+
			"add an eligible node or drain these vaults first, or re-run with --force to accept reduced redundancy",
		targetNodeID, ErrWouldDropBelowRF, len(degraded), strings.Join(parts, ", "))
}

// makeSetNodeSuffrageFunc creates the callback for the SetNodeSuffrage RPC.
func makeSetNodeSuffrageFunc(
	clusterSrv *cluster.Server,
	nodeID string,
	scheduler *orchestrator.Scheduler,
	logger *slog.Logger,
) func(ctx context.Context, targetNodeID string, voter bool) error {
	suffrageOnLeader := func(_ context.Context, targetNodeID string, voter bool) error {
		nodeAddr, err := lookupNodeAddr(clusterSrv, targetNodeID)
		if err != nil {
			return err
		}
		const timeout = 10 * time.Second
		if voter {
			logger.Info("promoting node to voter", "node_id", targetNodeID)
			if err := clusterSrv.AddVoter(targetNodeID, nodeAddr, timeout); err != nil {
				logger.Error("suffrage change failed", "node_id", targetNodeID, "voter", voter, "error", err)
				return err
			}
			logger.Info("node promoted to voter", "node_id", targetNodeID)
		} else {
			logger.Info("demoting node to nonvoter", "node_id", targetNodeID)
			if err := clusterSrv.DemoteVoter(targetNodeID, timeout); err != nil {
				logger.Error("suffrage change failed", "node_id", targetNodeID, "voter", voter, "error", err)
				return err
			}
			logger.Info("node demoted to nonvoter", "node_id", targetNodeID)
		}
		return nil
	}

	clusterSrv.SetNodeSuffrageFn(func(ctx context.Context, nodeID, nodeAddr string, voter bool) error {
		return suffrageOnLeader(ctx, nodeID, voter)
	})

	var demotingSelf atomic.Bool

	return func(ctx context.Context, targetNodeID string, voter bool) error {
		_, leaderID := clusterSrv.LeaderInfo()

		if !voter && targetNodeID == leaderID && leaderID == nodeID {
			if !demotingSelf.CompareAndSwap(false, true) {
				return errors.New("leader demotion already in progress")
			}
			submitSelfDemotion(scheduler, clusterSrv, nodeID, logger, func() {
				demotingSelf.Store(false)
			})
			return nil
		}

		if leaderID == nodeID {
			return suffrageOnLeader(ctx, targetNodeID, voter)
		}

		if leaderID == "" {
			return errors.New("no leader available")
		}
		logger.Info("forwarding suffrage change to leader", "leader_id", leaderID, "target_node_id", targetNodeID, "voter", voter)
		return forwardSuffrage(clusterSrv, leaderID, targetNodeID, voter)
	}
}

// lookupNodeAddr finds a node's cluster address in the current Raft configuration.
func lookupNodeAddr(clusterSrv *cluster.Server, targetNodeID string) (string, error) {
	servers, err := clusterSrv.Servers()
	if err != nil {
		return "", fmt.Errorf("list servers: %w", err)
	}
	for _, srv := range servers {
		if srv.ID == targetNodeID {
			return srv.Address, nil
		}
	}
	return "", fmt.Errorf("node %s not in cluster configuration", targetNodeID)
}

// forwardSuffrage forwards a suffrage change to the current leader via cluster gRPC.
func forwardSuffrage(clusterSrv *cluster.Server, leaderID, targetNodeID string, voter bool) error {
	peerConns := clusterSrv.PeerConns()
	if peerConns == nil {
		return errors.New("peer connections not available")
	}
	nodeAddr, err := lookupNodeAddr(clusterSrv, targetNodeID)
	if err != nil {
		return err
	}
	req := &gastrologv1.ForwardSetNodeSuffrageRequest{
		NodeId:   []byte(targetNodeID),
		NodeAddr: nodeAddr,
		Voter:    voter,
	}
	resp := &gastrologv1.ForwardSetNodeSuffrageResponse{}
	if err := peerConns.InvokeService(context.Background(), leaderID, cluster.PurposeSuffrage,
		"/gastrolog.v1.ClusterService/ForwardSetNodeSuffrage", req, resp); err != nil {
		return fmt.Errorf("forward suffrage to leader %s: %w", leaderID, err)
	}
	return nil
}

// submitSelfDemotion runs leader self-demotion as a background job.
func submitSelfDemotion(
	scheduler *orchestrator.Scheduler,
	clusterSrv *cluster.Server,
	nodeID string,
	logger *slog.Logger,
	done func(),
) {
	scheduler.Submit("demote-self", func(ctx context.Context, prog *orchestrator.JobProgress) {
		defer done()
		prog.SetRunning(2)

		logger.Info("transferring leadership before self-demotion")
		if err := clusterSrv.LeadershipTransfer(); err != nil {
			prog.Fail(time.Now(), fmt.Sprintf("leadership transfer: %v", err))
			return
		}
		prog.IncrChunks()

		var newLeaderID string
		for range 40 {
			time.Sleep(250 * time.Millisecond)
			_, id := clusterSrv.LeaderInfo()
			if id != "" && id != nodeID {
				newLeaderID = id
				break
			}
		}
		if newLeaderID == "" {
			prog.Fail(time.Now(), "timed out waiting for new leader after transfer")
			return
		}
		logger.Info("leadership transferred", "new_leader_id", newLeaderID)

		var lastErr error
		for attempt := range 5 {
			if attempt > 0 {
				time.Sleep(time.Duration(attempt) * time.Second)
				_, id := clusterSrv.LeaderInfo()
				if id != "" && id != nodeID {
					newLeaderID = id
				}
			}
			if err := forwardSuffrage(clusterSrv, newLeaderID, nodeID, false); err != nil {
				lastErr = err
				logger.Warn("forward demotion attempt failed", "attempt", attempt+1, "error", err)
				continue
			}
			logger.Info("self-demotion completed via new leader", "new_leader_id", newLeaderID)
			prog.IncrChunks()
			return
		}
		prog.Fail(time.Now(), fmt.Sprintf("forward demotion failed after retries: %v", lastErr))
	})
}
