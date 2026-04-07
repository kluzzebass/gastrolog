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
	"gastrolog/internal/config"
	"gastrolog/internal/config/raftfsm"
	"gastrolog/internal/config/raftstore"
	"gastrolog/internal/home"
	"gastrolog/internal/logging"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
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
}

// raftConfigStore wraps a raftstore.Store with cleanup logic for the
// underlying raft instance, forwarder, and boltdb store.
type raftConfigStore struct {
	config.Store
	raftStore *raftstore.Store
	raft      *hraft.Raft
	boltDB    io.Closer
	forwarder io.Closer // *cluster.Forwarder; nil for single-node
}

// WaitForLeader polls until any node in the cluster becomes leader or the
// context is cancelled.
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

// WaitForFSMCatchup blocks until the local config FSM reflects the cluster's
// latest committed state. This is a prerequisite for reading tier placements
// from the FSM at startup — without it, hraft.NewRaft leaves the FSM at the
// snapshot level, and post-snapshot committed entries (e.g. placement
// assignments) are not yet applied.
//
// Behaviour by role:
//   - Leader: calls raft.Barrier(), which appends a no-op log entry and
//     waits for it to commit + apply locally. Guarantees the leader's FSM
//     is current to its own last-committed index at the moment of the call.
//   - Follower: polls applied_index vs commit_index from Stats() until
//     they match. Commit index advances as the leader sends AppendEntries,
//     so we're effectively waiting for the next few heartbeats to arrive.
//
// Assumes a leader has already been elected (call WaitForLeader first).
func (s *raftConfigStore) WaitForFSMCatchup(ctx context.Context, timeout time.Duration, logger *slog.Logger) error {
	if s.raft.State() == hraft.Leader {
		return s.raft.Barrier(timeout).Error()
	}

	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				return errors.New("timed out waiting for FSM catchup")
			}
			stats := s.raft.Stats()
			commit, err1 := strconv.ParseUint(stats["commit_index"], 10, 64)
			applied, err2 := strconv.ParseUint(stats["applied_index"], 10, 64)
			if err1 != nil || err2 != nil {
				continue
			}
			if applied >= commit && commit > 0 {
				logger.Debug("fsm caught up", "applied", applied, "commit", commit)
				return nil
			}
		}
	}
}

func (s *raftConfigStore) Close() error {
	if s.forwarder != nil {
		_ = s.forwarder.Close()
	}
	// No pre-shutdown snapshot. During simultaneous cluster shutdown, the
	// leader's snapshot noop can't replicate (followers are also shutting
	// down), leaving Raft state dirty. Periodic snapshots (every 30s /
	// 4 entries) provide recovery; the log replay on restart is minimal.
	future := s.raft.Shutdown()
	err := future.Error()
	if cerr := s.boltDB.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}

// openRaftConfigStore creates a raft-backed config store with BoltDB persistence.
func openRaftConfigStore(opts raftStoreOpts) (*raftConfigStore, error) {
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

	tp := opts.transport
	if tp == nil {
		tp = opts.ClusterSrv.Transport()
	}

	fsm := raftfsm.New(opts.FSMOpts...)
	conf := newRaftConfig(opts.NodeID, opts.Logger)

	r, err := hraft.NewRaft(conf, fsm, boltStore, boltStore, snapStore, tp)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create raft: %w", err)
	}

	observeLeaderChanges(r, opts.Logger)

	if err := bootstrapAndWaitForLeader(r, boltStore, tp, opts); err != nil {
		return nil, err
	}

	opts.Logger.Info("raft config store ready", "dir", raftDir)

	store := raftstore.New(r, fsm, 10*time.Second)

	opts.ClusterSrv.SetRaft(r)
	opts.ClusterSrv.SetApplyFn(func(ctx context.Context, data []byte) error {
		return store.ApplyRaw(data)
	})
	fwd := cluster.NewForwarder(r, opts.ClusterTLS)
	store.SetForwarder(fwd)

	return &raftConfigStore{
		Store:     store,
		raftStore: store,
		raft:      r,
		boltDB:    boltStore,
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
