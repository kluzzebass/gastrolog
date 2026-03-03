package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/cluster/tlsutil"
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/home"
	"gastrolog/internal/orchestrator"
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
		ClusterAddr: cfg.ClusterAddr,
		NodeID:      nodeID,
		TLS:         clusterTLS,
		Logger:      logger.With("component", "cluster"),
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

// startClusterServices bootstraps TLS if needed and starts the cluster gRPC server.
func startClusterServices(ctx context.Context, clusterSrv *cluster.Server, clusterTLS *cluster.ClusterTLS, cfgStore config.Store, hd home.Dir, logger *slog.Logger) error {
	if clusterSrv == nil {
		return nil
	}

	if clusterTLS.State() == nil {
		if err := bootstrapClusterTLS(ctx, cfgStore, clusterTLS, hd.ClusterTLSPath(), logger); err != nil {
			return fmt.Errorf("bootstrap cluster TLS: %w", err)
		}
	}

	clusterSrv.SetEnrollHandler(makeEnrollHandler(cfgStore, logger))
	return clusterSrv.Start()
}

// bootstrapClusterTLS generates CA, cluster cert, and join token.
func bootstrapClusterTLS(ctx context.Context, cfgStore config.Store, ctls *cluster.ClusterTLS, tlsFilePath string, logger *slog.Logger) error {
	existingCfg, err := cfgStore.Load(ctx)
	if err != nil {
		return fmt.Errorf("check existing cluster TLS: %w", err)
	}
	if existingCfg != nil && existingCfg.ClusterTLS != nil {
		existing := existingCfg.ClusterTLS
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

	if err := cfgStore.PutClusterTLS(ctx, config.ClusterTLS{
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

	return nil
}

// makeEnrollHandler creates the Enroll RPC handler for the cluster server.
func makeEnrollHandler(cfgStore config.Store, logger *slog.Logger) cluster.EnrollHandler {
	return func(ctx context.Context, req *gastrologv1.EnrollRequest) (*gastrologv1.EnrollResponse, error) {
		cfg, err := cfgStore.Load(ctx)
		if err != nil || cfg == nil || cfg.ClusterTLS == nil {
			logger.Error("enroll: read cluster TLS", "error", err)
			return nil, errors.New("cluster TLS not available")
		}
		tls := cfg.ClusterTLS

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
	proxy *config.StoreProxy,
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
		oldStore, err := openRaftConfigStore(raftStoreOpts{
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
		clusterSrv.SetApplyFn(func(ctx context.Context, data []byte) error {
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
	for _, ingesterID := range orch.ListIngesters() {
		if err := orch.RemoveIngester(ingesterID); err != nil {
			logger.Warn("join cleanup: remove ingester failed", "ingester_id", ingesterID, "error", err)
		}
	}
}

// restartClusterWithStore configures the cluster server to use the given config
// store's raft instance and starts the gRPC server.
func restartClusterWithStore(store *raftConfigStore, proxy *config.StoreProxy, clusterSrv *cluster.Server, logger *slog.Logger) error {
	clusterSrv.SetApplyFn(func(ctx context.Context, data []byte) error {
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
func validateSingleNodeCluster(proxy *config.StoreProxy, clusterSrv *cluster.Server, nodeID string) (*raftConfigStore, error) {
	rcs, ok := proxy.Inner().(*raftConfigStore)
	if !ok {
		return nil, errors.New("runtime cluster join requires raft config store")
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
	proxy *config.StoreProxy,
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

		// 5. Close old raft config store
		logger.Info("closing old raft config store")
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

		// 8. Open new raft config store
		logger.Info("opening new raft config store")
		newStore, err := openRaftConfigStore(raftStoreOpts{
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

		// 12. Request membership
		logger.Info("requesting cluster membership", "leader_addr", leaderAddr)
		joinCtx, joinCancel := context.WithTimeout(ctx, 30*time.Second)
		err = cluster.JoinCluster(joinCtx, leaderAddr, nodeID, clusterAddr, clusterTLS, true)
		joinCancel()
		if err != nil {
			return fmt.Errorf("join cluster: %w", err)
		}
		logger.Info("cluster membership granted")

		// 13. Wait for config replication
		logger.Info("waiting for config replication from leader")
		if err := waitForServerSettings(ctx, proxy, 60*time.Second, logger); err != nil {
			return fmt.Errorf("wait for config replication: %w", err)
		}

		// 14. Ensure node name
		if _, err := ensureNodeConfig(ctx, proxy, nodeID); err != nil {
			logger.Warn("failed to write node name after join", "error", err)
		}

		logger.Info("runtime cluster join complete")
		return nil
	}
}

// makeEvictionHandler creates the callback invoked when this node is evicted
// from the cluster. Reinitializes as a fresh single-node cluster.
func makeEvictionHandler(
	proxy *config.StoreProxy,
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

		rcs, ok := proxy.Inner().(*raftConfigStore)
		if !ok {
			logger.Error("eviction reinit: config store is not raft-backed, shutting down instead")
			p, _ := os.FindProcess(os.Getpid())
			_ = p.Signal(os.Interrupt)
			return
		}

		proxy.SetJoining()

		logger.Info("eviction reinit: closing old raft config store")
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

		logger.Info("eviction reinit: opening fresh raft config store")
		newStore, err := openRaftConfigStore(raftStoreOpts{
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

// makeRemoveNodeFunc creates the callback for the RemoveNode RPC.
func makeRemoveNodeFunc(
	clusterSrv *cluster.Server,
	nodeID string,
	logger *slog.Logger,
) func(ctx context.Context, targetNodeID string) error {
	removeOnLeader := func(ctx context.Context, targetNodeID string) error {
		peerConns := clusterSrv.PeerConns()
		var evictConn *cluster.NotifyEvictionClient
		if peerConns != nil {
			if c, err := peerConns.Conn(targetNodeID); err == nil {
				evictConn = cluster.NewNotifyEvictionClient(c)
			} else {
				logger.Warn("cannot pre-connect to evicted node for notification", "error", err)
			}
		}

		logger.Info("removing node from cluster", "node_id", targetNodeID)
		if err := clusterSrv.RemoveServer(targetNodeID, 10*time.Second); err != nil {
			return fmt.Errorf("remove server: %w", err)
		}
		logger.Info("node removed from cluster", "node_id", targetNodeID)

		if evictConn != nil {
			go func() {
				notifyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := evictConn.NotifyEviction(notifyCtx, "removed from cluster by leader"); err != nil {
					logger.Warn("failed to notify evicted node", "node_id", targetNodeID, "error", err)
				} else {
					logger.Info("eviction notification sent", "node_id", targetNodeID)
				}
				peerConns.Invalidate(targetNodeID)
			}()
		}

		return nil
	}

	clusterSrv.SetRemoveNodeFn(removeOnLeader)

	return func(ctx context.Context, targetNodeID string) error {
		_, leaderID := clusterSrv.LeaderInfo()

		if leaderID == nodeID {
			return removeOnLeader(ctx, targetNodeID)
		}

		if leaderID == "" {
			return errors.New("no leader available")
		}
		peerConns := clusterSrv.PeerConns()
		if peerConns == nil {
			return errors.New("peer connections not available")
		}
		conn, err := peerConns.Conn(leaderID)
		if err != nil {
			return fmt.Errorf("connect to leader %s: %w", leaderID, err)
		}
		logger.Info("forwarding node removal to leader", "leader_id", leaderID, "target_node_id", targetNodeID)
		client := cluster.NewForwardRemoveNodeClient(conn)
		return client.ForwardRemoveNode(ctx, targetNodeID)
	}
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
	conn, err := peerConns.Conn(leaderID)
	if err != nil {
		return fmt.Errorf("connect to leader %s: %w", leaderID, err)
	}
	nodeAddr, err := lookupNodeAddr(clusterSrv, targetNodeID)
	if err != nil {
		return err
	}
	client := cluster.NewForwardSetNodeSuffrageClient(conn)
	return client.ForwardSetNodeSuffrage(context.Background(), targetNodeID, nodeAddr, voter)
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
