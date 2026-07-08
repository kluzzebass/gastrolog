package app

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/alert"
	"io"
	"log/slog"
	"os"
	"strconv"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/home"
	"gastrolog/internal/logging"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/raftstore"

	petname "github.com/dustinkirkland/golang-petname"
	"github.com/hashicorp/go-hclog"
	hraft "github.com/hashicorp/raft"
)

// raftStoreOpts groups the parameters needed to open a raft-backed config store.
type raftStoreOpts struct {
	Home       home.Dir
	NodeID     string
	Init       bool
	JoinAddr   string
	ClusterSrv *cluster.Server
	ClusterTLS *cluster.ClusterTLS
	Logger     *slog.Logger
	FSMOpts    []raftfsm.Option
	// Alerts receives the WAL space-reserve alarm. Optional (nil on the
	// rejoin/rollback reopen paths, which predate an alert collector).
	Alerts *alert.Collector

	// transport is an optional pre-created Raft transport (used during rejoin
	// when the cluster server has already created a fresh transport).
	// When nil, a new transport is obtained from ClusterSrv.Transport().
	transport hraft.Transport
}

// raftClusterCtlStore wraps a raftstore.Store with cleanup logic for the
// underlying raft instance, forwarder, and boltdb store.
type raftClusterCtlStore struct {
	system.Store
	raftStore *raftstore.Store
	raft      *hraft.Raft
	wal       *raftwal.WAL
	forwarder io.Closer // *cluster.Forwarder; nil for single-node
	// liveness accumulates cluster-ctl Raft liveness events for the
	// NodeStats broadcast (gastrolog-1io54g); vault groups have their own
	// counters on the GroupManager.
	liveness raftgroup.LivenessCounters
}

// RaftLivenessSources exposes the cluster-ctl group's liveness counters and
// WAL for the node-level Raft liveness aggregation (gastrolog-1io54g).
func (s *raftClusterCtlStore) RaftLivenessSources() (*raftgroup.LivenessCounters, *raftwal.WAL) {
	return &s.liveness, s.wal
}

// WaitForLeader polls until any node in the cluster becomes leader or the
// context is cancelled.
func (s *raftClusterCtlStore) WaitForLeader(ctx context.Context, logger *slog.Logger) error {
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

// WaitForFSMCatchup blocks until the local config FSM reflects the cluster's
// latest committed state. This is a prerequisite for reading vault placements
// from the FSM at startup — without it, hraft.NewRaft leaves the FSM at the
// snapshot level, and post-snapshot committed entries (e.g. placement
// assignments) are not yet applied.
//
// Behaviour by role:
//
//   - Leader: calls raft.Barrier(), which appends a no-op log entry and
//     waits for it to commit + apply locally. Guarantees the leader's FSM
//     is current to its own last-committed index at the moment of the call.
//
//   - Follower: this is the tricky case. On a fresh restart, both
//     applied_index and commit_index are at the *snapshot's* index — they
//     appear "equal" before the follower has received a single byte from
//     the new leader. We can't just wait for `applied >= commit` because
//     it's already true at startup against stale state.
//
//     The correct check is "wait for the local log to grow past the
//     snapshot via AppendEntries from the leader, then for applied to
//     catch up to that". We use a stability window: poll last_log_index
//     until it has been STABLE (unchanged) for at least stabilityWindow.
//     If new entries are still arriving, we keep waiting. Once stable AND
//     applied_index has caught up to last_log_index, we're done.
//
//     Edge case: an idle cluster with no new entries since the snapshot.
//     The first heartbeat from the leader will advance commit_index to
//     match the leader's, even if no new log entries arrive. We bootstrap
//     stability tracking from `last_log_index` (which equals commit_index
//     in steady state) and accept any value as long as it's stable.
//
// Assumes a leader has already been elected (call WaitForLeader first).
func (s *raftClusterCtlStore) WaitForFSMCatchup(ctx context.Context, timeout time.Duration, logger *slog.Logger) error {
	if s.raft.State() == hraft.Leader {
		return s.raft.Barrier(timeout).Error()
	}

	const (
		pollInterval    = 50 * time.Millisecond
		stabilityWindow = 1 * time.Second
	)

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastSeenLogIndex uint64
	stableSince := time.Time{}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return errors.New("timed out waiting for FSM catchup")
			}
			stats := s.raft.Stats()
			lastLogIdx, err1 := strconv.ParseUint(stats["last_log_index"], 10, 64)
			appliedIdx, err2 := strconv.ParseUint(stats["applied_index"], 10, 64)
			if err1 != nil || err2 != nil {
				continue
			}

			// If last_log_index changed, the local log is still growing
			// (the leader is sending us entries). Reset stability tracking.
			if lastLogIdx != lastSeenLogIndex {
				lastSeenLogIndex = lastLogIdx
				stableSince = time.Now()
				logger.Debug("fsm catchup: log advancing",
					"last_log_index", lastLogIdx, "applied_index", appliedIdx)
				continue
			}

			// Log is stable. Wait for applied to catch up.
			if appliedIdx < lastLogIdx {
				logger.Debug("fsm catchup: applying log entries",
					"last_log_index", lastLogIdx, "applied_index", appliedIdx)
				continue
			}

			// Log is stable AND applied has caught up. Wait for the
			// stability window before declaring success — this gives
			// time for any in-flight heartbeats to bring more entries
			// or for the leader's commit_index to propagate.
			if time.Since(stableSince) >= stabilityWindow {
				logger.Debug("fsm caught up",
					"last_log_index", lastLogIdx, "applied_index", appliedIdx)
				return nil
			}
		}
	}
}

func (s *raftClusterCtlStore) Close() error {
	if s.forwarder != nil {
		_ = s.forwarder.Close()
	}
	// No pre-shutdown snapshot. During simultaneous cluster shutdown, the
	// leader's snapshot noop can't replicate (followers are also shutting
	// down), leaving Raft state dirty. Periodic snapshots (every 30s /
	// 4 entries) provide recovery; the log replay on restart is minimal.
	future := s.raft.Shutdown()
	err := future.Error()
	if s.wal != nil {
		if cerr := s.wal.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// walReserveAlarm builds the OnReserveState callback for a named WAL: losing
// the space reserve raises a storage alarm (operator action: free disk space
// NOW — without the reserve, a full volume panics Raft on the next term or
// log write), and restoring it clears the alarm. Nil-tolerant on both alerts
// and logger.
func walReserveAlarm(alerts *alert.Collector, logger *slog.Logger, walName string) func(lost bool, err error) {
	id := "wal-reserve:" + walName
	return func(lost bool, err error) {
		if !lost {
			if logger != nil {
				logger.Info("raft WAL space reserve restored", "wal", walName)
			}
			if alerts != nil {
				alerts.Clear(id)
			}
			return
		}
		if logger != nil {
			logger.Warn("raft WAL space reserve lost — consensus has no ENOSPC immunity until space frees",
				"wal", walName, "error", err)
		}
		if alerts != nil {
			alerts.Set(id, alert.Error, "storage", fmt.Sprintf(
				"Raft WAL (%s) space reserve lost: %v. Free disk space now — without the reserve, a full volume crashes consensus on this node.",
				walName, err))
		}
	}
}

// openRaftClusterCtlStore creates a raft-backed system store with WAL persistence.
func openRaftClusterCtlStore(opts raftStoreOpts) (*raftClusterCtlStore, error) {
	raftDir := opts.Home.RaftDir()
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, fmt.Errorf("create raft directory: %w", err)
	}

	walDir := opts.Home.ClusterCtlWALDir()
	wal, err := raftwal.Open(walDir, raftwal.Config{
		OnReserveState: walReserveAlarm(opts.Alerts, opts.Logger, "cluster-ctl"),
	})
	if err != nil {
		return nil, fmt.Errorf("open cluster-ctl raft WAL: %w", err)
	}
	gs := wal.GroupStore("cluster-ctl")

	clusterCtlSnapDir := opts.Home.RaftGroupDir("cluster-ctl")
	if err := os.MkdirAll(clusterCtlSnapDir, 0o750); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("create system snapshot dir: %w", err)
	}
	snapStore, err := hraft.NewFileSnapshotStore(clusterCtlSnapDir, 2, io.Discard)
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("create snapshot store: %w", err)
	}

	tp := opts.transport
	if tp == nil {
		tp = opts.ClusterSrv.Transport()
	}

	fsm := raftfsm.New(opts.FSMOpts...)
	conf := newRaftConfig(opts.NodeID, opts.Logger)

	r, err := hraft.NewRaft(conf, fsm, gs, gs, snapStore, tp)
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	ctlStore := &raftClusterCtlStore{raft: r, wal: wal}
	clusterCtlLogger := logging.NewRaftGroupSlog(compRaft.Apply(opts.Logger), raftgroup.ClusterControlPlaneGroupID)
	raftgroup.ObserveRaftDiagnostics(r, clusterCtlLogger, conf.LeaderLeaseTimeout, &ctlStore.liveness)

	if err := bootstrapAndWaitForLeader(r, wal, tp, opts, clusterCtlLogger); err != nil {
		return nil, err
	}

	clusterCtlLogger.Info("raft system store ready", "wal_dir", walDir, "snapshots", clusterCtlSnapDir)

	store := raftstore.New(r, fsm, 10*time.Second)

	opts.ClusterSrv.SetRaft(r)
	opts.ClusterSrv.SetApplyFn(func(ctx context.Context, data []byte) (uint64, error) {
		return store.ApplyRaw(data)
	})
	fwd := cluster.NewForwarder(r, opts.ClusterSrv.PeerConns())
	store.SetForwarder(fwd)

	ctlStore.Store = store
	ctlStore.raftStore = store
	ctlStore.forwarder = fwd
	return ctlStore, nil
}

// newRaftConfig creates a hashicorp/raft config with cluster-ready timeouts.
func newRaftConfig(nodeID string, logger *slog.Logger) *hraft.Config {
	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)

	// Wire Raft's internal hclog logger to the application's slog pipeline.
	// This makes election events, heartbeat timeouts, and state transitions
	// visible through the normal logging system (component "raft").
	raftLogger := logging.NewRaftGroupHclog(compRaft.Apply(logger), raftgroup.ClusterControlPlaneGroupID)
	// Suppress the noisy "entering follower state" log that fires on every
	// heartbeat timeout cycle, even when the node remains a follower.
	filtered := logging.FilterHclogMessages(raftLogger, "entering follower state")
	// Downgrade routine snapshot/pipeline noise. Do NOT downgrade
	// "failed to contact" — that substring also matches the quorum
	// step-down message and hides the primary leader-loss signal.
	downgraded := logging.DowngradeHclogToDebug(
		logging.EnsureHclogMinLevel(filtered, hclog.Warn,
			"failed to contact quorum of nodes, stepping down",
			"failed to contact",
			"failed to heartbeat to",
			"failed to appendEntries to",
			"new leader elected, stepping down",
		),
		"failed to take snapshot",
		"starting snapshot up to",
		"snapshot complete up to",
		"compacting logs",
		"no logs to truncate",
		"pipelining replication",
		"aborting pipeline replication",
		"failed to make requestVote RPC",
	)
	conf.Logger = downgraded
	conf.LogOutput = nil

	conf.SnapshotThreshold = 4
	conf.SnapshotInterval = 30 * time.Second
	conf.TrailingLogs = 64

	conf.HeartbeatTimeout, conf.ElectionTimeout, conf.LeaderLeaseTimeout = raftgroup.RaftTimeouts(raftgroup.GroupConfig{
		GroupID: raftgroup.ClusterControlPlaneGroupID,
	})
	return conf
}

// bootstrapAndWaitForLeader handles state-based Raft bootstrap and waits for
// leadership when this node should become leader.
func bootstrapAndWaitForLeader(r *hraft.Raft, boltStore io.Closer, transport hraft.Transport, opts raftStoreOpts, logger *slog.Logger) error {
	existing := r.GetConfiguration()
	if err := existing.Error(); err != nil {
		_ = r.Shutdown().Error()
		_ = boltStore.Close()
		return fmt.Errorf("get raft configuration: %w", err)
	}

	servers := existing.Configuration().Servers
	needsBootstrap := len(servers) == 0
	joining := opts.JoinAddr != ""
	shouldBootstrap := needsBootstrap && !joining

	if needsBootstrap && !shouldBootstrap {
		logger.Info("waiting to be added to cluster by leader")
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
		logger.Info("cluster bootstrapped", "node_id", opts.NodeID)
	}

	singleNode := len(servers) == 1 && string(servers[0].ID) == opts.NodeID
	if shouldBootstrap || singleNode {
		// The wait must scale with the configured failure-detector timing:
		// hashicorp/raft randomizes the first election inside
		// [ElectionTimeout, 2×ElectionTimeout], so a fixed wait shorter
		// than that window makes bootstrap fail deterministically once an
		// operator widens the timeouts (a 5s heartbeat knob broke every
		// cluster init against the old hardcoded 5s).
		_, electionTimeout, _ := raftgroup.RaftTimeouts(raftgroup.GroupConfig{
			GroupID: raftgroup.ClusterControlPlaneGroupID,
		})
		wait := max(3*electionTimeout, 5*time.Second)
		select {
		case <-r.LeaderCh():
			logger.Info("leader elected", "node_id", opts.NodeID)
		case <-time.After(wait):
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return fmt.Errorf("timed out waiting for raft leadership after %s", wait)
		}
	}

	return nil
}

// peerEvictor is the minimal contract the peer-removal observer needs —
// anything with a Delete(nodeID string) method. Many cluster-local caches
// satisfy it: PeerState, PeerJobState, PeerByteMetrics, Broadcaster (the
// failure-suppression map), and StatsCollector (per-peer rate windows).
type peerEvictor interface {
	Delete(nodeID string)
}

// observePeerRemovals registers a Raft observer for PeerObservation events
// and drives the removal loop. Blocking-mode observer so removals can't be
// silently dropped. Stops when ctx is cancelled.
//
// Every supplied evictor is called on every removal so per-peer satellite
// state can't outlive cluster membership — see gastrolog-9ohip for the
// inventory of caches that previously leaked.
func observePeerRemovals(ctx context.Context, clusterSrv *cluster.Server, logger *slog.Logger, evictors ...peerEvictor) {
	ch := make(chan hraft.Observation, 16)
	clusterSrv.RegisterPeerObserver(ch)
	go runPeerRemovalLoop(ctx, ch, logger, evictors...)
}

// runPeerRemovalLoop consumes observations from ch and evicts each removed
// peer from every supplied cache. Exposed for tests so the loop can be
// driven by synthetic observations without standing up a real Raft cluster.
func runPeerRemovalLoop(ctx context.Context, ch <-chan hraft.Observation, logger *slog.Logger, evictors ...peerEvictor) {
	for {
		select {
		case <-ctx.Done():
			return
		case obs, ok := <-ch:
			if !ok {
				return
			}
			po, ok := obs.Data.(hraft.PeerObservation)
			if !ok || !po.Removed {
				continue
			}
			id := string(po.Peer.ID)
			for _, e := range evictors {
				e.Delete(id)
			}
			logger.Info("cluster peer removed, evicted from peer caches", "node_id", id, "evictors", len(evictors))
		}
	}
}

// peerCacheReconciler is the contract the periodic peer-cache
// reconciler needs — a cache that can purge its own entries against
// an authoritative membership set. Implementations: PeerState,
// PeerJobState, PeerByteMetrics, Broadcaster, StatsCollector.
type peerCacheReconciler interface {
	ReconcilePeers(keep map[string]struct{})
}

// memberSource is the minimal contract the reconciler needs from
// cluster.Server to enumerate current Raft membership.
type memberSource interface {
	Servers() ([]cluster.RaftServer, error)
}

// reconcilePeerCachesOnce lists current Raft membership and asks
// every supplied cache to purge entries whose peer is no longer a
// member. Belt-and-suspenders for the observer path — hraft does
// not fire PeerObservation when a config change is delivered via
// snapshot install (only on log apply), so a follower behind by a
// snapshot can miss removal events.
//
// Independent of leadership: every node runs the sweep against its
// own caches. Wired into the job scheduler by the caller (see
// app.go startPeerCacheReconcile) so it shares the same scheduler
// infrastructure as the rest of the project's housekeeping work
// (cache eviction, retention, archival, etc.) rather than running
// in a hand-rolled goroutine.
func reconcilePeerCachesOnce(src memberSource, logger *slog.Logger, caches ...peerCacheReconciler) {
	servers, err := src.Servers()
	if err != nil {
		logger.Debug("peer-cache reconcile: list servers", "error", err)
		return
	}
	keep := make(map[string]struct{}, len(servers))
	for _, s := range servers {
		keep[s.ID] = struct{}{}
	}
	for _, c := range caches {
		c.ReconcilePeers(keep)
	}
}

// leaderChecker is the minimal contract observePeerAdditions needs —
// just an "am I currently the cluster-ctl leader?" predicate so the
// addition loop can gate FSM writes. cluster.Server.IsLeader satisfies it.
type leaderChecker interface {
	IsLeader() bool
}

// observePeerAdditions registers a Raft observer that writes a
// placeholder NodeConfig for every freshly admitted peer. Without this,
// new joiners briefly appear as raw GLIDs in the UI because they are
// admitted to Raft membership BEFORE their own async ensureNodeConfig
// write commits — see gastrolog-4dqfs.
//
// Strategy: the leader observes every PeerObservation.Removed==false
// event, looks up the peer's NodeConfig, and writes a petname-bearing
// placeholder if none exists yet. Followers see the same observation
// but defer to the leader — only the leader proposes Raft commands.
// The joiner's own ensureNodeConfig later updates Name to its preferred
// value (e.g. pod hostname) once it has caught up to quorum.
//
// Blocking-mode observer so additions can't be silently dropped while
// the addition loop is busy on the previous event.
func observePeerAdditions(ctx context.Context, clusterSrv *cluster.Server, cfgStore system.Store, logger *slog.Logger) {
	ch := make(chan hraft.Observation, 16)
	clusterSrv.RegisterPeerObserver(ch)
	go runPeerAdditionLoop(ctx, ch, clusterSrv, cfgStore, logger)
}

// runPeerAdditionLoop consumes observations from ch and writes a
// placeholder NodeConfig for each newly added peer when this node
// is the cluster-ctl leader. Exposed for tests so the loop can be
// driven by synthetic observations.
func runPeerAdditionLoop(ctx context.Context, ch <-chan hraft.Observation, leader leaderChecker, cfgStore system.Store, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case obs, ok := <-ch:
			if !ok {
				return
			}
			po, ok := obs.Data.(hraft.PeerObservation)
			if !ok || po.Removed {
				continue
			}
			if !leader.IsLeader() {
				continue
			}
			id := string(po.Peer.ID)
			handlePeerAddition(ctx, cfgStore, id, logger)
		}
	}
}

// handlePeerAddition writes a placeholder NodeConfig for nodeID if
// none exists. Idempotent: existing records are left untouched so the
// joiner's own ensureNodeConfig keeps authority over the Name field.
func handlePeerAddition(ctx context.Context, cfgStore system.Store, nodeID string, logger *slog.Logger) {
	id, err := glid.ParseAny(nodeID)
	if err != nil {
		logger.Warn("peer addition: parse node ID", "node_id", nodeID, "error", err)
		return
	}
	existing, err := cfgStore.GetNode(ctx, id)
	if err != nil {
		logger.Warn("peer addition: lookup NodeConfig", "node_id", nodeID, "error", err)
		return
	}
	if existing != nil {
		// Already has a NodeConfig (rejoin, or this node won the race
		// against the joiner's own write). Nothing to do.
		return
	}
	name := petname.Generate(2, "-")
	if err := cfgStore.PutNode(ctx, system.NodeConfig{
		ID:         id,
		Name:       name,
		State:      system.NodeStateLive,
		StateSince: time.Now(),
	}); err != nil {
		logger.Warn("peer addition: write placeholder NodeConfig", "node_id", nodeID, "error", err)
		return
	}
	logger.Info("peer addition: wrote placeholder NodeConfig", "node_id", nodeID, "name", name)
}
