package raftgroup

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftwal"

	hraft "github.com/hashicorp/raft"
)

// ErrGroupNotFound is returned when an operation targets a group that doesn't exist.
var ErrGroupNotFound = errors.New("group not found")

func groupNotFound(id string) error { return fmt.Errorf("%w: %s", ErrGroupNotFound, id) }

// GroupConfig describes a Raft group to create.
type GroupConfig struct {
	// GroupID is the unique identifier for this group, used for transport routing.
	GroupID string

	// FSM is the finite state machine for this group.
	FSM hraft.FSM

	// SeedMembers, when non-empty, gives a fresh Raft instance its initial
	// member configuration via hraft.BootstrapCluster. This is the only API
	// hraft exposes for seeding an empty Raft instance with a member list.
	//
	// Pass a single-element slice for a single-node group; pass the full
	// member list for a multi-node group. In a multi-node group, every
	// participating node should pass the SAME list — symmetric seeding lets
	// the nodes elect a leader through normal Raft election without any
	// node holding a special "bootstrap" role.
	//
	// Leave nil/empty when restarting an existing group: the persisted log
	// already contains a configuration, so no seeding is needed. (If a
	// non-empty SeedMembers is supplied for a group whose log is non-empty,
	// it is ignored — the existing log wins.)
	//
	// This is "Raft group seeding", not "cluster bootstrap". The cluster as
	// a whole is bootstrapped exactly once, via cmd/gastrolog --bootstrap on
	// the very first node. Per-group seeding happens whenever a new Raft
	// group needs to come into existence, throughout the cluster's lifetime.
	SeedMembers []hraft.Server

	// SnapshotThreshold is the number of log entries before a snapshot is taken.
	// Defaults to 4 if zero (matches config Raft behavior).
	SnapshotThreshold uint64

	// SnapshotInterval is how often the snapshot check runs.
	// Defaults to 30s if zero.
	SnapshotInterval time.Duration

	// TrailingLogs is the number of log entries kept after a snapshot.
	// Defaults to 64 if zero.
	TrailingLogs uint64
}

// Group is a running Raft group managed by the GroupManager.
type Group struct {
	Raft *hraft.Raft
	FSM  hraft.FSM
	dir  string
}

// GroupManager manages the lifecycle of multiple Raft groups on a node.
// Each group gets its own BoltDB (or shared WAL), snapshot store, and transport view.
type GroupManager struct {
	mu     sync.RWMutex
	groups map[string]*Group

	transport    *multiraft.Transport[string]
	nodeID       string
	baseDir      string       // <home>/raft/groups/
	shutdownLast string       // group ID to shut down last (e.g. config group)
	wal          *raftwal.WAL // optional shared WAL; nil = per-group boltdb
	logger       *slog.Logger
}

// GroupManagerConfig holds configuration for creating a GroupManager.
type GroupManagerConfig struct {
	// Transport is the multi-raft transport shared by all groups.
	Transport *multiraft.Transport[string]

	// NodeID is this node's unique identifier.
	NodeID string

	// BaseDir is the base directory for group storage.
	// Each group gets a subdirectory: <BaseDir>/<GroupID>/
	BaseDir string

	// Logger for structured logging.
	Logger *slog.Logger

	// ShutdownLast is a group ID that should be shut down after all others.
	// Typically the config group — it must remain available while tier groups
	// are shutting down so they can still replicate final state.
	ShutdownLast string

	// WAL, when non-nil, provides a shared write-ahead log for all groups.
	// Writes from all groups are batched into a single fsync, reducing disk
	// I/O at high group counts. When nil, each group gets its own boltdb.
	WAL *raftwal.WAL
}

// NewGroupManager creates a manager for Raft group lifecycle.
func NewGroupManager(cfg GroupManagerConfig) *GroupManager {
	return &GroupManager{
		groups:       make(map[string]*Group),
		transport:    cfg.Transport,
		nodeID:       cfg.NodeID,
		baseDir:      cfg.BaseDir,
		shutdownLast: cfg.ShutdownLast,
		wal:          cfg.WAL,
		logger:       logging.Default(cfg.Logger).With("component", "raft-group-manager"),
	}
}

// CreateGroup creates and starts a new Raft group. The group's persistent
// state is stored under <BaseDir>/<GroupID>/. If the directory already exists
// (node restart), the group recovers from its snapshot and log.
func (m *GroupManager) CreateGroup(cfg GroupConfig) (*Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.groups[cfg.GroupID]; exists {
		return nil, fmt.Errorf("group %q already exists", cfg.GroupID)
	}

	groupDir := filepath.Join(m.baseDir, cfg.GroupID)
	if err := os.MkdirAll(groupDir, 0o750); err != nil {
		return nil, fmt.Errorf("create group dir: %w", err)
	}

	if m.wal == nil {
		return nil, fmt.Errorf("WAL required for group %q", cfg.GroupID)
	}
	gs := m.wal.GroupStore(cfg.GroupID)

	snapStore, err := hraft.NewFileSnapshotStore(groupDir, 2, io.Discard)
	if err != nil {
		return nil, fmt.Errorf("create snapshot store for group %q: %w", cfg.GroupID, err)
	}

	tp := m.transport.GroupTransport(cfg.GroupID)
	raftCfg := m.newRaftConfig(cfg)

	r, err := hraft.NewRaft(raftCfg, cfg.FSM, gs, gs, snapStore, tp)
	if err != nil {
		return nil, fmt.Errorf("create raft for group %q: %w", cfg.GroupID, err)
	}

	if len(cfg.SeedMembers) > 0 {
		if err := m.seedGroup(r, cfg.SeedMembers); err != nil {
			_ = r.Shutdown().Error()
			return nil, fmt.Errorf("seed group %q: %w", cfg.GroupID, err)
		}
	}

	g := &Group{
		Raft: r,
		FSM:  cfg.FSM,
		dir:  groupDir,
	}
	m.groups[cfg.GroupID] = g

	m.logger.Info("raft group created",
		"group", cfg.GroupID,
		"seed_members", len(cfg.SeedMembers),
		"dir", groupDir)

	return g, nil
}

// GetGroup returns a running group by ID, or nil if not found.
func (m *GroupManager) GetGroup(groupID string) *Group {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.groups[groupID]
}

// DestroyGroup gracefully shuts down a group and removes it from the manager.
// The group's persistent state on disk is NOT deleted (for recovery).
func (m *GroupManager) DestroyGroup(groupID string) error {
	m.mu.Lock()
	g, exists := m.groups[groupID]
	if !exists {
		m.mu.Unlock()
		return groupNotFound(groupID)
	}
	delete(m.groups, groupID)
	m.mu.Unlock()

	// Shut down Raft BEFORE removing the transport group — Raft needs the
	// transport channel for its shutdown sequence.
	if g.Raft.State() == hraft.Leader {
		if f := g.Raft.Snapshot(); f.Error() != nil {
			if !errors.Is(f.Error(), hraft.ErrNothingNewToSnapshot) {
				m.logger.Warn("snapshot before shutdown failed",
					"group", groupID, "error", f.Error())
			}
		}
	}
	if err := g.Raft.Shutdown().Error(); err != nil {
		m.logger.Error("raft shutdown failed", "group", groupID, "error", err)
	}
	m.transport.RemoveGroup(groupID)

	m.logger.Info("raft group destroyed", "group", groupID)
	return nil
}

// AddMember adds a node to a group. Automatically selects voter or nonvoter
// based on the resulting group size:
//   - 2-member: nonvoter (primary is sole voter, always has quorum)
//   - 3+: voter (proper quorum-based fault tolerance)
func (m *GroupManager) AddMember(groupID string, serverID hraft.ServerID, serverAddr hraft.ServerAddress) error {
	g := m.GetGroup(groupID)
	if g == nil {
		return groupNotFound(groupID)
	}
	if m.shouldBeVoter(g) {
		return g.Raft.AddVoter(serverID, serverAddr, 0, 10*time.Second).Error()
	}
	return g.Raft.AddNonvoter(serverID, serverAddr, 0, 10*time.Second).Error()
}

// shouldBeVoter returns whether a new member should be added as a voter.
// 2-member groups get a nonvoter (sole voter = always has quorum).
// 3+ get all voters (proper fault tolerance).
func (m *GroupManager) shouldBeVoter(g *Group) bool {
	future := g.Raft.GetConfiguration()
	if future.Error() != nil {
		return true // default to voter on error
	}
	// +1 for the member being added.
	return len(future.Configuration().Servers)+1 >= 3
}

// RemoveMember removes a node from a group.
func (m *GroupManager) RemoveMember(groupID string, serverID hraft.ServerID) error {
	g := m.GetGroup(groupID)
	if g == nil {
		return groupNotFound(groupID)
	}
	return g.Raft.RemoveServer(serverID, 0, 10*time.Second).Error()
}

// Shutdown stops every registered multiraft group (e.g. vault/.../ctl).
// Groups are shut down concurrently to avoid sequential election timeout
// delays on follower nodes. If shutdownLast is set, that group is stopped after
// all others complete.
func (m *GroupManager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Shut down all groups except shutdownLast concurrently.
	var wg sync.WaitGroup
	for id, g := range m.groups {
		if id == m.shutdownLast {
			continue
		}
		wg.Add(1)
		go func(id string, g *Group) {
			defer wg.Done()
			m.shutdownGroup(id, g)
		}(id, g)
	}
	wg.Wait()

	// Shut down the designated-last group.
	if m.shutdownLast != "" {
		if g, ok := m.groups[m.shutdownLast]; ok {
			m.shutdownGroup(m.shutdownLast, g)
		}
	}

	m.groups = make(map[string]*Group)
}

// Groups returns the IDs of all running groups.
func (m *GroupManager) Groups() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.groups))
	for id := range m.groups {
		ids = append(ids, id)
	}
	return ids
}

// ---------- Internal ----------

func (m *GroupManager) shutdownGroup(id string, g *Group) {
	// Only snapshot if this node is the leader — non-leaders can't snapshot
	// and the call blocks indefinitely waiting for a leader that may never come.
	if g.Raft.State() == hraft.Leader {
		if f := g.Raft.Snapshot(); f.Error() != nil {
			if !errors.Is(f.Error(), hraft.ErrNothingNewToSnapshot) {
				m.logger.Warn("snapshot before shutdown failed", "group", id, "error", f.Error())
			}
		}
	}
	if err := g.Raft.Shutdown().Error(); err != nil {
		m.logger.Error("raft shutdown failed", "group", id, "error", err)
	}
	m.transport.RemoveGroup(id)
	m.logger.Info("raft group shut down", "group", id)
}

func (m *GroupManager) newRaftConfig(cfg GroupConfig) *hraft.Config {
	conf := hraft.DefaultConfig()
	conf.LocalID = hraft.ServerID(m.nodeID)

	raftLogger := logging.NewHclogAdapter(m.logger.With("group", cfg.GroupID))
	filtered := logging.FilterHclogMessages(raftLogger, "entering follower state")
	conf.Logger = logging.DowngradeHclogToDebug(filtered,
		"failed to heartbeat",
		"failed to appendEntries",
		"failed to take snapshot",
		"failed to install snapshot",
		"failed to send snapshot to",
		"failed to get log",
		"failed to pipeline appendEntries",
		"peer has newer term, stopping replication",
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
	if cfg.SnapshotThreshold > 0 {
		conf.SnapshotThreshold = cfg.SnapshotThreshold
	}
	conf.SnapshotInterval = 30 * time.Second
	if cfg.SnapshotInterval > 0 {
		conf.SnapshotInterval = cfg.SnapshotInterval
	}
	conf.TrailingLogs = 64
	if cfg.TrailingLogs > 0 {
		conf.TrailingLogs = cfg.TrailingLogs
	}

	conf.HeartbeatTimeout = 1000 * time.Millisecond
	conf.ElectionTimeout = 1000 * time.Millisecond
	conf.LeaderLeaseTimeout = 500 * time.Millisecond

	return conf
}

// seedGroup gives a fresh Raft instance its initial member configuration via
// hraft.BootstrapCluster. This is the only API hraft exposes for seeding an
// empty Raft instance with a member list. If the group's log is non-empty
// (restart case), seedGroup is a no-op — the existing log already contains
// the configuration.
//
// In a multi-node group, every participating node calls seedGroup with the
// same member list. The nodes then elect a leader through normal Raft election.
// No node holds a special "bootstrap" role.
func (m *GroupManager) seedGroup(r *hraft.Raft, members []hraft.Server) error {
	existing := r.GetConfiguration()
	if err := existing.Error(); err != nil {
		return fmt.Errorf("get configuration: %w", err)
	}
	if len(existing.Configuration().Servers) > 0 {
		return nil // already seeded (restart)
	}
	return r.BootstrapCluster(hraft.Configuration{Servers: members}).Error()
}
