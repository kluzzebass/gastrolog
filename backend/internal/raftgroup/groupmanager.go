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

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// ErrGroupNotFound is returned when an operation targets a group that doesn't exist.
var ErrGroupNotFound = errors.New("group not found")

func groupNotFound(id string) error { return fmt.Errorf("%w: %s", ErrGroupNotFound, id) }

// GroupConfig describes a Raft group to create or join.
type GroupConfig struct {
	// GroupID is the unique identifier for this group, used for transport routing.
	GroupID string

	// FSM is the finite state machine for this group.
	FSM hraft.FSM

	// Bootstrap indicates this group should bootstrap as a single-node cluster.
	// Only one node in the group should set this to true.
	Bootstrap bool

	// Members lists the initial cluster members as (ServerID, ServerAddress) pairs.
	// Used for bootstrapping multi-node groups. Ignored when Bootstrap is false.
	Members []hraft.Server

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

	boltDB io.Closer
	dir    string
}

// GroupManager manages the lifecycle of multiple Raft groups on a node.
// Each group gets its own BoltDB, snapshot store, and transport view.
type GroupManager struct {
	mu     sync.RWMutex
	groups map[string]*Group

	transport    *multiraft.Transport[string]
	nodeID       string
	baseDir      string // <home>/raft/groups/
	shutdownLast string // group ID to shut down last (e.g. config group)
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
}

// NewGroupManager creates a manager for Raft group lifecycle.
func NewGroupManager(cfg GroupManagerConfig) *GroupManager {
	return &GroupManager{
		groups:       make(map[string]*Group),
		transport:    cfg.Transport,
		nodeID:       cfg.NodeID,
		baseDir:      cfg.BaseDir,
		shutdownLast: cfg.ShutdownLast,
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

	boltStore, err := raftboltdb.New(raftboltdb.Options{
		Path: filepath.Join(groupDir, "raft.db"),
	})
	if err != nil {
		return nil, fmt.Errorf("open boltdb for group %q: %w", cfg.GroupID, err)
	}

	snapStore, err := hraft.NewFileSnapshotStore(groupDir, 2, io.Discard)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create snapshot store for group %q: %w", cfg.GroupID, err)
	}

	tp := m.transport.GroupTransport(cfg.GroupID)
	raftCfg := m.newRaftConfig(cfg)

	r, err := hraft.NewRaft(raftCfg, cfg.FSM, boltStore, boltStore, snapStore, tp)
	if err != nil {
		_ = boltStore.Close()
		return nil, fmt.Errorf("create raft for group %q: %w", cfg.GroupID, err)
	}

	if cfg.Bootstrap {
		if err := m.bootstrap(r, cfg); err != nil {
			_ = r.Shutdown().Error()
			_ = boltStore.Close()
			return nil, fmt.Errorf("bootstrap group %q: %w", cfg.GroupID, err)
		}
	}

	g := &Group{
		Raft:   r,
		FSM:    cfg.FSM,
		boltDB: boltStore,
		dir:    groupDir,
	}
	m.groups[cfg.GroupID] = g

	m.logger.Info("raft group created",
		"group", cfg.GroupID,
		"bootstrap", cfg.Bootstrap,
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
	if err := g.boltDB.Close(); err != nil {
		m.logger.Error("boltdb close failed", "group", groupID, "error", err)
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

// Shutdown gracefully stops all groups, tier groups first, config last.
// Tier groups are shut down concurrently to avoid sequential election
// timeout delays on follower nodes. The shutdownLast group (if set) is
// shut down after all others complete.
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
	if err := g.boltDB.Close(); err != nil {
		m.logger.Error("boltdb close failed", "group", id, "error", err)
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
		"starting snapshot up to",
		"snapshot complete up to",
		"compacting logs",
		"pipelining replication",
		"aborting pipeline replication",
		"failed to contact",
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
	conf.LeaderLeaseTimeout = 500 * time.Millisecond

	// Bootstrap nodes (config placement leaders) get a shorter election timeout
	// so they win the first election after a full cluster restart. Non-bootstrap
	// nodes get a longer timeout — they can still elect if the leader dies, but
	// won't race the bootstrap node on startup.
	if cfg.Bootstrap {
		conf.ElectionTimeout = 1000 * time.Millisecond
	} else {
		conf.ElectionTimeout = 3000 * time.Millisecond
	}

	return conf
}

// bootstrap performs state-based bootstrapping. Only bootstraps if the Raft
// log is empty (first start). If Members is empty, bootstraps as single-node.
func (m *GroupManager) bootstrap(r *hraft.Raft, cfg GroupConfig) error {
	existing := r.GetConfiguration()
	if err := existing.Error(); err != nil {
		return fmt.Errorf("get configuration: %w", err)
	}
	if len(existing.Configuration().Servers) > 0 {
		return nil // already bootstrapped (restart)
	}

	var servers []hraft.Server
	if len(cfg.Members) > 0 {
		servers = cfg.Members
	} else {
		servers = []hraft.Server{
			{ID: hraft.ServerID(m.nodeID), Address: m.transport.LocalAddr()},
		}
	}

	boot := hraft.Configuration{Servers: servers}
	return r.BootstrapCluster(boot).Error()
}

