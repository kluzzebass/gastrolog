package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/home"
	"gastrolog/internal/logging"
	"gastrolog/internal/raftwal"
	"gastrolog/internal/system"
	"gastrolog/internal/system/raftfsm"
	"gastrolog/internal/system/raftstore"

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

	// transport is an optional pre-created Raft transport (used during rejoin
	// when the cluster server has already created a fresh transport).
	// When nil, a new transport is obtained from ClusterSrv.Transport().
	transport hraft.Transport

	// TierRaftSharesWAL is set only from the main Run path when cluster mode
	// is enabled: tier Raft groups use the same raftwal instance as the
	// system store, and serveAndAwaitShutdown closes it after system raft.
	// Rejoin / rollback paths omit this so each store owns its WAL again.
	TierRaftSharesWAL bool
}

// raftSystemStore wraps a raftstore.Store with cleanup logic for the
// underlying raft instance, forwarder, and boltdb store.
type raftSystemStore struct {
	system.Store
	raftStore *raftstore.Store
	raft      *hraft.Raft
	wal       *raftwal.WAL
	ownsWAL   bool
	forwarder io.Closer // *cluster.Forwarder; nil for single-node
}

// WaitForLeader polls until any node in the cluster becomes leader or the
// context is cancelled.
func (s *raftSystemStore) WaitForLeader(ctx context.Context, logger *slog.Logger) error {
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
// latest committed state. This is a prerequisite for reading tier placements
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
func (s *raftSystemStore) WaitForFSMCatchup(ctx context.Context, timeout time.Duration, logger *slog.Logger) error {
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

func (s *raftSystemStore) Close() error {
	if s.forwarder != nil {
		_ = s.forwarder.Close()
	}
	// No pre-shutdown snapshot. During simultaneous cluster shutdown, the
	// leader's snapshot noop can't replicate (followers are also shutting
	// down), leaving Raft state dirty. Periodic snapshots (every 30s /
	// 4 entries) provide recovery; the log replay on restart is minimal.
	future := s.raft.Shutdown()
	err := future.Error()
	if s.ownsWAL && s.wal != nil {
		if cerr := s.wal.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

// openRaftSystemStore creates a raft-backed system store with WAL persistence.
func openRaftSystemStore(opts raftStoreOpts) (*raftSystemStore, error) {
	raftDir := opts.Home.RaftDir()
	if err := os.MkdirAll(raftDir, 0o750); err != nil {
		return nil, fmt.Errorf("create raft directory: %w", err)
	}

	wal, err := raftwal.Open(filepath.Join(raftDir, "wal"))
	if err != nil {
		return nil, fmt.Errorf("open system raft WAL: %w", err)
	}
	gs := wal.GroupStore("system")

	systemSnapDir := opts.Home.RaftGroupDir("system")
	if err := os.MkdirAll(systemSnapDir, 0o750); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("create system snapshot dir: %w", err)
	}
	snapStore, err := hraft.NewFileSnapshotStore(systemSnapDir, 2, io.Discard)
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

	observeLeaderChanges(r, opts.Logger)

	if err := bootstrapAndWaitForLeader(r, wal, tp, opts); err != nil {
		return nil, err
	}

	opts.Logger.Info("raft system store ready", "wal_dir", filepath.Join(raftDir, "wal"), "snapshots", systemSnapDir)

	store := raftstore.New(r, fsm, 10*time.Second)

	opts.ClusterSrv.SetRaft(r)
	opts.ClusterSrv.SetApplyFn(func(ctx context.Context, data []byte) error {
		return store.ApplyRaw(data)
	})
	fwd := cluster.NewForwarder(r, opts.ClusterTLS)
	store.SetForwarder(fwd)

	ownsWAL := !opts.TierRaftSharesWAL
	return &raftSystemStore{
		Store:     store,
		raftStore: store,
		raft:      r,
		wal:       wal,
		ownsWAL:   ownsWAL,
		forwarder: fwd,
	}, nil
}

// newRaftConfig creates a hashicorp/raft config with cluster-ready timeouts.
func newRaftConfig(nodeID string, logger *slog.Logger) *hraft.Config {
	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(nodeID)

	// Wire Raft's internal hclog logger to the application's slog pipeline.
	// This makes election events, heartbeat timeouts, and state transitions
	// visible through the normal logging system (component "raft").
	raftLogger := logging.NewHclogAdapter(logger.With("component", "raft"))
	// Suppress the noisy "entering follower state" log that fires on every
	// heartbeat timeout cycle, even when the node remains a follower.
	filtered := logging.FilterHclogMessages(raftLogger, "entering follower state")
	// Downgrade noisy Raft messages to DEBUG: heartbeat/replication failures
	// fire constantly when peers are unreachable, and snapshot lifecycle
	// messages are routine housekeeping.
	conf.Logger = logging.DowngradeHclogToDebug(filtered,
		"failed to heartbeat",
		"failed to appendEntries",
		"failed to take snapshot",
		"starting snapshot up to",
		"snapshot complete up to",
		"compacting logs",
		"pipelining replication",
		"aborting pipeline replication",
		"failed to contact",
		"failed to make requestVote RPC",
	)
	conf.LogOutput = nil

	conf.SnapshotThreshold = 4
	conf.SnapshotInterval = 30 * time.Second
	conf.TrailingLogs = 64

	conf.HeartbeatTimeout = 1000 * time.Millisecond
	conf.ElectionTimeout = 1000 * time.Millisecond
	conf.LeaderLeaseTimeout = 500 * time.Millisecond
	return conf
}

// bootstrapAndWaitForLeader handles state-based Raft bootstrap and waits for
// leadership when this node should become leader.
func bootstrapAndWaitForLeader(r *hraft.Raft, boltStore io.Closer, transport hraft.Transport, opts raftStoreOpts) error {
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
		opts.Logger.Info("raft: waiting to be added to cluster by leader")
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

	singleNode := len(servers) == 1 && string(servers[0].ID) == opts.NodeID
	if shouldBootstrap || singleNode {
		select {
		case <-r.LeaderCh():
			opts.Logger.Info("raft: leader elected", "node_id", opts.NodeID)
		case <-time.After(5 * time.Second):
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return errors.New("timed out waiting for raft leadership")
		}
	}

	return nil
}

// observeLeaderChanges registers a Raft observer that logs leader elections.
// Uses blocking mode to guarantee observations are never silently dropped.
func observeLeaderChanges(r *hraft.Raft, logger *slog.Logger) {
	ch := make(chan hraft.Observation, 16)
	r.RegisterObserver(hraft.NewObserver(ch, true, func(o *hraft.Observation) bool {
		_, ok := o.Data.(hraft.LeaderObservation)
		return ok
	}))
	go func() {
		for obs := range ch {
			if lo, ok := obs.Data.(hraft.LeaderObservation); ok {
				if lo.LeaderID == "" {
					logger.Info("cluster lost leader")
				} else {
					logger.Info("cluster leader elected",
						"node_id", string(lo.LeaderID),
						"addr", string(lo.LeaderAddr))
				}
			}
		}
	}()
}

// peerEvictor is the minimal contract the peer-removal observer needs —
// anything with a Delete(nodeID string) method. Both cluster.PeerState and
// cluster.PeerJobState satisfy it.
type peerEvictor interface {
	Delete(nodeID string)
}

// observePeerRemovals registers a Raft observer for PeerObservation events
// and drives the removal loop. Blocking-mode observer so removals can't be
// silently dropped. Stops when ctx is cancelled.
func observePeerRemovals(ctx context.Context, clusterSrv *cluster.Server, peerState, peerJobState peerEvictor, logger *slog.Logger) {
	ch := make(chan hraft.Observation, 16)
	clusterSrv.RegisterPeerObserver(ch)
	go runPeerRemovalLoop(ctx, ch, peerState, peerJobState, logger)
}

// runPeerRemovalLoop consumes observations from ch and evicts each removed
// peer from both caches. Exposed for tests so the loop can be driven by
// synthetic observations without standing up a real Raft cluster.
func runPeerRemovalLoop(ctx context.Context, ch <-chan hraft.Observation, peerState, peerJobState peerEvictor, logger *slog.Logger) {
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
			peerState.Delete(id)
			peerJobState.Delete(id)
			logger.Info("cluster peer removed, evicted from peer caches", "node_id", id)
		}
	}
}
