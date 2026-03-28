package multiraft

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

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

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

	transport *Transport[string]
	nodeID    string
	baseDir   string // <home>/raft/groups/
	logger    *slog.Logger
}

// GroupManagerConfig holds configuration for creating a GroupManager.
type GroupManagerConfig struct {
	// Transport is the multi-raft transport shared by all groups.
	Transport *Transport[string]

	// NodeID is this node's unique identifier.
	NodeID string

	// BaseDir is the base directory for group storage.
	// Each group gets a subdirectory: <BaseDir>/<GroupID>/
	BaseDir string

	// Logger for structured logging.
	Logger *slog.Logger
}

// NewGroupManager creates a manager for Raft group lifecycle.
func NewGroupManager(cfg GroupManagerConfig) *GroupManager {
	return &GroupManager{
		groups:    make(map[string]*Group),
		transport: cfg.Transport,
		nodeID:    cfg.NodeID,
		baseDir:   cfg.BaseDir,
		logger:    logging.Default(cfg.Logger).With("component", "raft-group-manager"),
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
		return fmt.Errorf("group %q not found", groupID)
	}
	delete(m.groups, groupID)
	m.mu.Unlock()

	m.transport.RemoveGroup(groupID)

	// Take a snapshot before shutting down for fast recovery.
	if f := g.Raft.Snapshot(); f.Error() != nil {
		m.logger.Warn("snapshot before shutdown failed",
			"group", groupID, "error", f.Error())
	}

	if err := g.Raft.Shutdown().Error(); err != nil {
		m.logger.Error("raft shutdown failed", "group", groupID, "error", err)
	}
	if err := g.boltDB.Close(); err != nil {
		m.logger.Error("boltdb close failed", "group", groupID, "error", err)
	}

	m.logger.Info("raft group destroyed", "group", groupID)
	return nil
}

// AddMember adds a node to a group. Automatically selects voter or nonvoter
// based on the resulting group size: 2-member = nonvoter, 3+ = voter.
func (m *GroupManager) AddMember(groupID string, serverID hraft.ServerID, serverAddr hraft.ServerAddress) error {
	g := m.GetGroup(groupID)
	if g == nil {
		return fmt.Errorf("group %q not found", groupID)
	}

	voter := m.shouldBeVoter(g, true)
	if voter {
		return g.Raft.AddVoter(serverID, serverAddr, 0, 10*time.Second).Error()
	}
	return g.Raft.AddNonvoter(serverID, serverAddr, 0, 10*time.Second).Error()
}

// RemoveMember removes a node from a group.
func (m *GroupManager) RemoveMember(groupID string, serverID hraft.ServerID) error {
	g := m.GetGroup(groupID)
	if g == nil {
		return fmt.Errorf("group %q not found", groupID)
	}
	return g.Raft.RemoveServer(serverID, 0, 10*time.Second).Error()
}

// Shutdown gracefully stops all groups, tier groups first, config last.
func (m *GroupManager) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Shut down non-config groups first.
	for id, g := range m.groups {
		if id == "config" {
			continue
		}
		m.shutdownGroup(id, g)
	}

	// Config group last.
	if g, ok := m.groups["config"]; ok {
		m.shutdownGroup("config", g)
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
	if f := g.Raft.Snapshot(); f.Error() != nil {
		if !errors.Is(f.Error(), hraft.ErrNothingNewToSnapshot) {
			m.logger.Warn("snapshot before shutdown failed", "group", id, "error", f.Error())
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
	conf.ElectionTimeout = 1000 * time.Millisecond
	conf.LeaderLeaseTimeout = 500 * time.Millisecond

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

// shouldBeVoter returns whether a new member should be added as a voter.
// Rule: 2-member groups get 1 voter + 1 nonvoter. 3+ get all voters.
func (m *GroupManager) shouldBeVoter(g *Group, adding bool) bool {
	future := g.Raft.GetConfiguration()
	if future.Error() != nil {
		return true // default to voter on error
	}
	count := len(future.Configuration().Servers)
	if adding {
		count++ // include the member being added
	}
	return count >= 3
}
