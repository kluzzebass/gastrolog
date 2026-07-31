package cluster

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/multiraft"
	hraft "github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// DefaultServicePoolMaxPerPeer is the default outbound service-lane pool size
// per peer node (parallel chunk/search streams without HOL blocking).
const DefaultServicePoolMaxPerPeer = 4

const connShardCount = 32

// ConnLane identifies the cluster transport lane for a connection.
type ConnLane int

const (
	LaneService ConnLane = iota
	LaneRaft
)

func (l ConnLane) String() string {
	switch l {
	case LaneService:
		return "service"
	case LaneRaft:
		return "raft"
	default:
		return "unknown"
	}
}

// ConnSpec identifies a logical outbound connection slot.
type ConnSpec struct {
	PeerNodeID string
	Lane       ConnLane
	GroupID    string // required when Lane == LaneRaft
}

// AcquireOpts labels the subsystem acquiring a connection.
type AcquireOpts struct {
	Purpose string
}

// PeerConnSnapshot is a point-in-time view of one managed connection for
// stats broadcast and inspection.
type PeerConnSnapshot struct {
	ConnID         uint64
	PeerNodeID     string
	Lane           string
	GroupID        string
	DialAddr       string
	ServerName     string
	PoolIndex      int
	Connectivity   string
	Purposes       []string
	PurposesWindow []string
	BytesSent      int64
	BytesRecv      int64
	CreatedAt      time.Time
	LastUsedAt     time.Time
}

// PeerConnHandle is a leased outbound connection. Call Release when done.
type PeerConnHandle interface {
	Spec() ConnSpec
	Purpose() string
	GRPC() *grpc.ClientConn
	Release()
	Invalidate(err error)
}

// PeerConnManager owns all outbound cluster gRPC connections. Raft lanes use
// a singleton per (peer, group); service lanes use a bounded pool per peer.
type PeerConnManager struct {
	raft        *hraft.Raft
	clusterTLS  *ClusterTLS
	nodeID      string
	logger      *slog.Logger
	byteMetrics *PeerByteMetrics // optional; mirrors outbound bytes per peer

	servicePoolMax int

	staticResolve func(nodeID string) (string, bool)
	staticPeerIDs []string

	staticMu sync.Mutex

	shards [connShardCount]connShard

	nextConnID atomic.Uint64
}

type connShard struct {
	mu      sync.RWMutex
	entries map[string]*connEntry
}

type connPolicy int

const (
	policySingleton connPolicy = iota
	policyPool
)

type connEntry struct {
	mu     sync.Mutex
	key    string
	policy connPolicy
	peer   string

	single *managedConn
	pool   []*managedConn
	rr     int
}

type managedConn struct {
	id         uint64
	spec       ConnSpec
	dialAddr   string
	serverName string
	grpc       *grpc.ClientConn
	poolIndex  int

	purposeMu sync.Mutex
	purposes  map[string]int
	// windowPurposes accumulates every purpose that acquired this conn since the
	// last stats broadcast tick (ResetPurposeWindows).
	windowPurposes map[string]struct{}

	bytesSent atomic.Int64
	bytesRecv atomic.Int64
	createdAt time.Time
	lastUsed  atomic.Int64 // unix nano
}

type peerConnHandle struct {
	mgr      *PeerConnManager
	mc       *managedConn
	purpose  string
	released bool
}

func (h *peerConnHandle) Spec() ConnSpec         { return h.mc.spec }
func (h *peerConnHandle) Purpose() string        { return h.purpose }
func (h *peerConnHandle) GRPC() *grpc.ClientConn { return h.mc.grpc }
func (h *peerConnHandle) Release() {
	if h.released {
		return
	}
	h.released = true
	h.mgr.releasePurpose(h.mc, h.purpose)
}
func (h *peerConnHandle) Invalidate(err error) { h.mgr.invalidateManaged(h.mc, err) }

// PeerConnManagerConfig configures a new manager.
type PeerConnManagerConfig struct {
	Raft                  *hraft.Raft
	ClusterTLS            *ClusterTLS
	NodeID                string
	Logger                *slog.Logger
	ServicePoolMaxPerPeer int
	StaticResolve         func(nodeID string) (string, bool)
	// ByteMetrics receives a per-peer mirror of outbound wire bytes from every
	// managed connection (service and raft lanes). Inbound bytes are tracked on
	// the cluster server stats handler separately.
	ByteMetrics *PeerByteMetrics
}

// NewPeerConnManager creates the central outbound connection manager.
func NewPeerConnManager(cfg PeerConnManagerConfig) *PeerConnManager {
	poolMax := cfg.ServicePoolMaxPerPeer
	if poolMax <= 0 {
		poolMax = DefaultServicePoolMaxPerPeer
	}
	m := &PeerConnManager{
		raft:           cfg.Raft,
		clusterTLS:     cfg.ClusterTLS,
		nodeID:         cfg.NodeID,
		logger:         compPeerConns.Apply(logging.Default(cfg.Logger)),
		servicePoolMax: poolMax,
		staticResolve:  cfg.StaticResolve,
		byteMetrics:    cfg.ByteMetrics,
	}
	for i := range m.shards {
		m.shards[i].entries = make(map[string]*connEntry)
	}
	return m
}

// NewPeerConns creates a manager backed by cluster-ctl Raft membership resolution.
func NewPeerConns(r *hraft.Raft, clusterTLS *ClusterTLS, nodeID string) *PeerConnManager {
	return NewPeerConnManager(PeerConnManagerConfig{
		Raft:       r,
		ClusterTLS: clusterTLS,
		NodeID:     nodeID,
	})
}

// NewStaticPeerConns creates a manager for in-process harnesses.
func NewStaticPeerConns(nodeID string, resolve func(nodeID string) (string, bool)) *PeerConnManager {
	return NewPeerConnManager(PeerConnManagerConfig{
		NodeID:        nodeID,
		StaticResolve: resolve,
	})
}

// SetServicePoolMaxPerPeer adjusts the service-lane pool cap at runtime.
func (m *PeerConnManager) SetServicePoolMaxPerPeer(n int) {
	if n <= 0 {
		n = DefaultServicePoolMaxPerPeer
	}
	m.servicePoolMax = n
}

// ServicePoolMaxPerPeer returns the configured service pool size per peer.
func (m *PeerConnManager) ServicePoolMaxPerPeer() int {
	return m.servicePoolMax
}

// SetStaticPeerIDs lists peer node IDs for static address reverse lookup.
func (m *PeerConnManager) SetStaticPeerIDs(ids []string) {
	m.staticMu.Lock()
	m.staticPeerIDs = append([]string(nil), ids...)
	m.staticMu.Unlock()
}

// Acquire returns a connection handle for spec. Call Release when finished.
func (m *PeerConnManager) Acquire(spec ConnSpec, opts AcquireOpts) (PeerConnHandle, error) {
	if spec.PeerNodeID == "" {
		return nil, errors.New("peer conn manager: empty peer node ID")
	}
	if spec.Lane == LaneRaft && spec.GroupID == "" {
		return nil, errors.New("peer conn manager: raft lane requires group ID")
	}
	purpose := opts.Purpose
	if purpose == "" {
		purpose = "unknown"
	}

	key := entryKey(spec)
	sh := m.shardFor(key)
	sh.mu.RLock()
	ent := sh.entries[key]
	sh.mu.RUnlock()

	if ent == nil {
		sh.mu.Lock()
		ent = sh.entries[key]
		if ent == nil {
			ent = &connEntry{
				key:    key,
				policy: policyFor(spec),
				peer:   spec.PeerNodeID,
			}
			sh.entries[key] = ent
		}
		sh.mu.Unlock()
	}

	mc, err := m.acquireFromEntry(ent, spec, purpose)
	if err != nil {
		return nil, err
	}
	return &peerConnHandle{mgr: m, mc: mc, purpose: purpose}, nil
}

// AcquireService is shorthand for the service lane.
func (m *PeerConnManager) AcquireService(peerNodeID, purpose string) (PeerConnHandle, error) {
	return m.Acquire(ConnSpec{PeerNodeID: peerNodeID, Lane: LaneService}, AcquireOpts{Purpose: purpose})
}

// AcquireRaftPeer acquires the raft lane by peer node ID.
func (m *PeerConnManager) AcquireRaftPeer(peerNodeID, groupID, purpose string) (PeerConnHandle, error) {
	return m.Acquire(ConnSpec{PeerNodeID: peerNodeID, Lane: LaneRaft, GroupID: groupID}, AcquireOpts{Purpose: purpose})
}

// AcquireRaftByAddress resolves addr to a peer node ID and acquires the raft lane.
func (m *PeerConnManager) AcquireRaftByAddress(addr hraft.ServerAddress, groupID, purpose string) (PeerConnHandle, error) {
	nodeID, err := m.resolveNodeIDFromAddress(string(addr))
	if err != nil {
		return nil, err
	}
	return m.AcquireRaftPeer(nodeID, groupID, purpose)
}

// AcquireRaft implements multiraft.PeerConnPool.
func (m *PeerConnManager) AcquireRaft(addr hraft.ServerAddress, groupID, purpose string) (multiraft.RaftConnLease, error) {
	return m.AcquireRaftByAddress(addr, groupID, purpose)
}

// Invalidate drops cached service-lane pool entries for a peer on transport failure.
func (m *PeerConnManager) Invalidate(nodeID string, err error) {
	if !shouldInvalidate(err) {
		return
	}
	m.invalidateEntry(entryKey(ConnSpec{PeerNodeID: nodeID, Lane: LaneService}), err)
}

// InvalidateRaft drops a raft singleton for a peer group.
func (m *PeerConnManager) InvalidateRaft(nodeID, groupID string, err error) {
	if !shouldInvalidate(err) {
		return
	}
	m.invalidateEntry(entryKey(ConnSpec{PeerNodeID: nodeID, Lane: LaneRaft, GroupID: groupID}), err)
}

// CloseGroupConns tears down all raft-lane singletons for groupID.
func (m *PeerConnManager) CloseGroupConns(groupID string) {
	var toClose []*managedConn
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		for key, ent := range sh.entries {
			spec, ok := specFromEntryKey(key)
			if !ok || spec.Lane != LaneRaft || spec.GroupID != groupID {
				continue
			}
			ent.mu.Lock()
			if ent.single != nil {
				toClose = append(toClose, ent.single)
				ent.single = nil
			}
			ent.mu.Unlock()
			delete(sh.entries, key)
		}
		sh.mu.Unlock()
	}
	if len(toClose) > 0 {
		m.logger.Info("closed raft group connections",
			"group_id", groupID,
			"connections", len(toClose),
		)
	}
	for _, mc := range toClose {
		m.closeManaged(mc, "group-destroyed")
	}
}

// Snapshot returns catalog entries for stats broadcast.
func (m *PeerConnManager) Snapshot() []PeerConnSnapshot {
	var out []PeerConnSnapshot
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.RLock()
		for _, ent := range sh.entries {
			ent.mu.Lock()
			if ent.policy == policySingleton {
				if ent.single != nil && ent.single.grpc.GetState() != connectivity.Shutdown {
					out = append(out, m.snapshotManaged(ent.single))
				}
			} else {
				for _, mc := range ent.pool {
					if mc != nil && mc.grpc.GetState() != connectivity.Shutdown {
						out = append(out, m.snapshotManaged(mc))
					}
				}
			}
			ent.mu.Unlock()
		}
		sh.mu.RUnlock()
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].PeerNodeID != out[j].PeerNodeID {
			return out[i].PeerNodeID < out[j].PeerNodeID
		}
		if out[i].Lane != out[j].Lane {
			return out[i].Lane < out[j].Lane
		}
		if out[i].GroupID != out[j].GroupID {
			return out[i].GroupID < out[j].GroupID
		}
		return out[i].PoolIndex < out[j].PoolIndex
	})
	return out
}

// ResetPurposeWindows clears accumulated purpose activity since the last
// stats broadcast tick. Called once per BroadcastStats after the snapshot
// is published — not on read-only Snapshot() calls from the lifecycle API.
func (m *PeerConnManager) ResetPurposeWindows() {
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.RLock()
		for _, ent := range sh.entries {
			ent.mu.Lock()
			if ent.policy == policySingleton {
				if ent.single != nil {
					ent.single.resetPurposeWindowLocked()
				}
			} else {
				for _, mc := range ent.pool {
					if mc != nil {
						mc.resetPurposeWindowLocked()
					}
				}
			}
			ent.mu.Unlock()
		}
		sh.mu.RUnlock()
	}
}

func (mc *managedConn) resetPurposeWindowLocked() {
	mc.purposeMu.Lock()
	mc.windowPurposes = make(map[string]struct{})
	mc.purposeMu.Unlock()
}

func (m *PeerConnManager) snapshotManaged(mc *managedConn) PeerConnSnapshot {
	mc.purposeMu.Lock()
	purposes := make([]string, 0, len(mc.purposes))
	for p, n := range mc.purposes {
		if n > 0 {
			purposes = append(purposes, p)
		}
	}
	sort.Strings(purposes)
	purposesWindow := make([]string, 0, len(mc.windowPurposes))
	for p := range mc.windowPurposes {
		purposesWindow = append(purposesWindow, p)
	}
	sort.Strings(purposesWindow)
	mc.purposeMu.Unlock()

	last := time.Unix(0, mc.lastUsed.Load())
	if mc.lastUsed.Load() == 0 {
		last = mc.createdAt
	}
	return PeerConnSnapshot{
		ConnID:         mc.id,
		PeerNodeID:     mc.spec.PeerNodeID,
		Lane:           mc.spec.Lane.String(),
		GroupID:        mc.spec.GroupID,
		DialAddr:       mc.dialAddr,
		ServerName:     mc.serverName,
		PoolIndex:      mc.poolIndex,
		Connectivity:   mc.grpc.GetState().String(),
		Purposes:       purposes,
		PurposesWindow: purposesWindow,
		BytesSent:      mc.bytesSent.Load(),
		BytesRecv:      mc.bytesRecv.Load(),
		CreatedAt:      mc.createdAt,
		LastUsedAt:     last,
	}
}

func (m *PeerConnManager) acquireFromEntry(ent *connEntry, spec ConnSpec, purpose string) (*managedConn, error) {
	ent.mu.Lock()
	defer ent.mu.Unlock()

	if ent.policy == policySingleton {
		if ent.single != nil && ent.single.grpc.GetState() != connectivity.Shutdown {
			m.touchUsed(ent.single)
			m.addPurposeLocked(ent.single, purpose)
			return ent.single, nil
		}
		mc, err := m.dial(spec, 0)
		if err != nil {
			return nil, err
		}
		if ent.single != nil {
			m.closeManaged(ent.single, "replaced")
		}
		ent.single = mc
		m.addPurposeLocked(mc, purpose)
		return mc, nil
	}

	poolMax := m.servicePoolMax
	if idle := ent.pickIdleConn(); idle != nil {
		m.touchUsed(idle)
		m.addPurposeLocked(idle, purpose)
		return idle, nil
	}
	if len(ent.pool) < poolMax {
		mc, err := m.dial(spec, len(ent.pool))
		if err != nil {
			return nil, err
		}
		ent.pool = append(ent.pool, mc)
		m.logger.Info("service pool expanded",
			"peer", spec.PeerNodeID,
			"pool_size", len(ent.pool),
			"pool_max", poolMax,
			"conn_id", mc.id,
		)
		m.addPurposeLocked(mc, purpose)
		return mc, nil
	}
	if len(ent.pool) == 0 {
		return nil, fmt.Errorf("peer conn manager: empty service pool for %s", spec.PeerNodeID)
	}
	ent.rr = (ent.rr + 1) % len(ent.pool)
	mc := ent.pool[ent.rr]
	if mc == nil || mc.grpc.GetState() == connectivity.Shutdown {
		replacement, err := m.dial(spec, ent.rr)
		if err != nil {
			return nil, err
		}
		if mc != nil {
			m.closeManaged(mc, "replaced")
		}
		ent.pool[ent.rr] = replacement
		mc = replacement
	} else if mc.activePurposeCount() > 0 {
		m.logger.Debug("service pool saturated, reusing busy connection",
			append(m.connAttrs(mc), "purpose", purpose, "active_purposes", mc.activePurposeCount())...,
		)
	}
	m.touchUsed(mc)
	m.addPurposeLocked(mc, purpose)
	return mc, nil
}

func (ent *connEntry) pickIdleConn() *managedConn {
	for _, mc := range ent.pool {
		if mc == nil || mc.grpc.GetState() == connectivity.Shutdown {
			continue
		}
		st := mc.grpc.GetState()
		if (st == connectivity.Ready || st == connectivity.Idle) && mc.activePurposeCount() == 0 {
			return mc
		}
	}
	return nil
}

func (mc *managedConn) activePurposeCount() int {
	mc.purposeMu.Lock()
	defer mc.purposeMu.Unlock()
	n := 0
	for _, c := range mc.purposes {
		n += c
	}
	return n
}

func (m *PeerConnManager) dialTransportCreds(spec ConnSpec) (credentials.TransportCredentials, string) {
	if spec.Lane == LaneRaft {
		serverName := multiraft.LaneSNI(spec.GroupID)
		if m.clusterTLS != nil && m.clusterTLS.State() != nil {
			return m.clusterTLS.TransportCredentialsForServerName(serverName), serverName
		}
		return insecure.NewCredentials(), serverName
	}
	serverName := SNIServiceLane
	if m.clusterTLS != nil && m.clusterTLS.State() != nil {
		return m.clusterTLS.TransportCredentials(), serverName
	}
	return insecure.NewCredentials(), serverName
}

func (m *PeerConnManager) dial(spec ConnSpec, poolIndex int) (*managedConn, error) {
	addr, err := m.resolveAddr(spec.PeerNodeID)
	if err != nil {
		return nil, err
	}
	creds, serverName := m.dialTransportCreds(spec)

	mc := &managedConn{
		id:             m.nextConnID.Add(1),
		spec:           spec,
		dialAddr:       addr,
		serverName:     serverName,
		poolIndex:      poolIndex,
		purposes:       make(map[string]int),
		windowPurposes: make(map[string]struct{}),
		createdAt:      time.Now(),
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  500 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
		}),
		grpc.WithUnaryInterceptor(m.attachNodeIDUnaryInterceptor),
		grpc.WithStreamInterceptor(m.attachNodeIDStreamInterceptor),
		grpc.WithStatsHandler(newManagedConnStatsHandler(mc, m.byteMetrics)),
	}
	conn, err := grpc.NewClient(addr, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial node %s at %s: %w", spec.PeerNodeID, addr, err)
	}
	mc.grpc = conn
	m.touchUsed(mc)
	m.logger.Debug("dialed peer connection", append(m.connAttrs(mc), "server_name", serverName)...)
	return mc, nil
}

func (m *PeerConnManager) touchUsed(mc *managedConn) {
	mc.lastUsed.Store(time.Now().UnixNano())
}

func (m *PeerConnManager) addPurposeLocked(mc *managedConn, purpose string) {
	mc.purposeMu.Lock()
	if mc.purposes == nil {
		mc.purposes = make(map[string]int)
	}
	if mc.windowPurposes == nil {
		mc.windowPurposes = make(map[string]struct{})
	}
	mc.purposes[purpose]++
	mc.windowPurposes[purpose] = struct{}{}
	mc.purposeMu.Unlock()
}

func (m *PeerConnManager) addPurpose(mc *managedConn, purpose string) {
	m.addPurposeLocked(mc, purpose)
}

func (m *PeerConnManager) releasePurpose(mc *managedConn, purpose string) {
	mc.purposeMu.Lock()
	if n := mc.purposes[purpose]; n <= 1 {
		delete(mc.purposes, purpose)
	} else {
		mc.purposes[purpose] = n - 1
	}
	mc.purposeMu.Unlock()
}

func (m *PeerConnManager) invalidateManaged(mc *managedConn, err error) {
	if !shouldInvalidate(err) {
		return
	}
	m.logger.Debug("invalidated peer connection", append(m.connAttrs(mc), "error", err)...)
	key := entryKey(mc.spec)
	m.removeManaged(key, mc)
}

func (m *PeerConnManager) invalidateEntry(key string, cause error) {
	sh := m.shardFor(key)
	sh.mu.RLock()
	ent := sh.entries[key]
	sh.mu.RUnlock()
	if ent == nil {
		return
	}
	ent.mu.Lock()
	var drop []*managedConn
	if ent.policy == policySingleton {
		if ent.single != nil {
			drop = append(drop, ent.single)
			ent.single = nil
		}
	} else {
		for i, mc := range ent.pool {
			if mc != nil {
				drop = append(drop, mc)
				ent.pool[i] = nil
			}
		}
		ent.pool = ent.pool[:0]
	}
	peer := ent.peer
	ent.mu.Unlock()
	if len(drop) > 0 {
		spec, _ := specFromEntryKey(key)
		attrs := []any{"peer", peer, "connections", len(drop)}
		if spec.Lane == LaneRaft {
			attrs = append(attrs, "lane", "raft", "group_id", spec.GroupID)
		} else {
			attrs = append(attrs, "lane", "service")
		}
		if cause != nil {
			attrs = append(attrs, "error", cause)
		}
		m.logger.Debug("invalidated peer connection entry", attrs...)
	}
	for _, mc := range drop {
		m.closeManaged(mc, "entry-invalidate")
	}
}

func (m *PeerConnManager) removeManaged(key string, mc *managedConn) {
	sh := m.shardFor(key)
	sh.mu.RLock()
	ent := sh.entries[key]
	sh.mu.RUnlock()
	if ent == nil {
		return
	}
	ent.mu.Lock()
	if ent.policy == policySingleton {
		if ent.single == mc {
			ent.single = nil
		}
	} else {
		for i, c := range ent.pool {
			if c == mc {
				ent.pool[i] = nil
				break
			}
		}
	}
	ent.mu.Unlock()
	m.closeManaged(mc, "invalidated")
}

func (m *PeerConnManager) closeManaged(mc *managedConn, reason string) {
	if mc == nil || mc.grpc == nil {
		return
	}
	m.logger.Debug("closing peer connection", append(m.connAttrs(mc), "reason", reason)...)
	go m.deferredCloseConn(mc)
}

func (m *PeerConnManager) deferredCloseConn(mc *managedConn) {
	time.Sleep(invalidateGracePeriod)
	_ = mc.grpc.Close()
	m.logger.Debug("peer connection closed", m.connAttrs(mc)...)
}

func (m *PeerConnManager) connAttrs(mc *managedConn) []any {
	if mc == nil {
		return nil
	}
	attrs := []any{
		"conn_id", mc.id,
		"peer", mc.spec.PeerNodeID,
		"lane", mc.spec.Lane.String(),
		"addr", mc.dialAddr,
		"pool_index", mc.poolIndex,
	}
	if mc.spec.GroupID != "" {
		attrs = append(attrs, "group_id", mc.spec.GroupID)
	}
	return attrs
}

func (m *PeerConnManager) shardFor(key string) *connShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return &m.shards[h.Sum32()%connShardCount]
}

func entryKey(spec ConnSpec) string {
	if spec.Lane == LaneRaft {
		return raftConnKey(spec.PeerNodeID, spec.GroupID)
	}
	return serviceConnKey(spec.PeerNodeID)
}

func specFromEntryKey(key string) (ConnSpec, bool) {
	if len(key) >= 4 && key[:4] == "svc:" {
		return ConnSpec{PeerNodeID: key[4:], Lane: LaneService}, true
	}
	if !isRaftConnKey(key) {
		return ConnSpec{}, false
	}
	const prefix = "raft:"
	rest := key[len(prefix):]
	i := 0
	for i < len(rest) && rest[i] != ':' {
		i++
	}
	if i >= len(rest) {
		return ConnSpec{}, false
	}
	return ConnSpec{
		PeerNodeID: rest[:i],
		Lane:       LaneRaft,
		GroupID:    rest[i+1:],
	}, true
}

func policyFor(spec ConnSpec) connPolicy {
	if spec.Lane == LaneRaft {
		return policySingleton
	}
	return policyPool
}

func (m *PeerConnManager) attachNodeIDUnaryInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	return invoker(metadata.AppendToOutgoingContext(ctx, NodeIDMetadataKey, m.nodeID), method, req, reply, cc, opts...)
}

func (m *PeerConnManager) attachNodeIDStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	return streamer(metadata.AppendToOutgoingContext(ctx, NodeIDMetadataKey, m.nodeID), desc, cc, method, opts...)
}

// Peers returns all Raft servers except self.
func (m *PeerConnManager) Peers() ([]hraft.Server, error) {
	if m.raft == nil {
		return nil, errors.New("peer conn manager: no raft configuration (static pool)")
	}
	future := m.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var peers []hraft.Server
	for _, srv := range future.Configuration().Servers {
		if string(srv.ID) != m.nodeID {
			peers = append(peers, srv)
		}
	}
	return peers, nil
}

func (m *PeerConnManager) PeerIDs() []string {
	peers, err := m.Peers()
	if err != nil {
		return nil
	}
	ids := make([]string, len(peers))
	for i, srv := range peers {
		ids[i] = string(srv.ID)
	}
	return ids
}

func (m *PeerConnManager) Reset(r *hraft.Raft) {
	m.logger.Debug("resetting peer connection manager")
	m.raft = r
	_ = m.Close()
}

func (m *PeerConnManager) Close() error {
	var drop []*grpc.ClientConn
	for i := range m.shards {
		sh := &m.shards[i]
		sh.mu.Lock()
		for key, ent := range sh.entries {
			ent.mu.Lock()
			if ent.single != nil {
				drop = append(drop, ent.single.grpc)
				ent.single = nil
			}
			for _, mc := range ent.pool {
				if mc != nil {
					drop = append(drop, mc.grpc)
				}
			}
			ent.pool = nil
			ent.mu.Unlock()
			delete(sh.entries, key)
		}
		sh.mu.Unlock()
	}
	if len(drop) > 0 {
		m.logger.Info("closed peer connection manager", "connections", len(drop))
	}
	for _, conn := range drop {
		_ = conn.Close()
	}
	return nil
}

func (m *PeerConnManager) resolveAddr(nodeID string) (string, error) {
	if m.staticResolve != nil {
		if addr, ok := m.staticResolve(nodeID); ok {
			return addr, nil
		}
		return "", fmt.Errorf("node %s not found in static peer table", nodeID)
	}
	future := m.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return "", fmt.Errorf("get raft config: %w", err)
	}
	leaderAddr, leaderID := m.raft.LeaderWithID()
	return resolveAddrFromRaft(future.Configuration().Servers, leaderAddr, leaderID, nodeID)
}

// resolveAddrFromRaft maps a node ID to its cluster address using the local
// raft configuration, falling back to the observed leader address when the
// target IS the current leader but the configuration entry describing it has
// not replicated yet. A freshly-joined node learns the leader's identity and
// address from the first AppendEntries heartbeat before its log backfills
// the configuration — in that window the config scan finds nothing, but raft
// itself already holds the authoritative answer, so "leader known" must
// imply "leader dialable".
func resolveAddrFromRaft(servers []hraft.Server, leaderAddr hraft.ServerAddress, leaderID hraft.ServerID, nodeID string) (string, error) {
	for _, srv := range servers {
		if string(srv.ID) == nodeID {
			return string(srv.Address), nil
		}
	}
	if string(leaderID) == nodeID && leaderAddr != "" {
		return string(leaderAddr), nil
	}
	return "", fmt.Errorf("node %s not found in raft config", nodeID)
}

func (m *PeerConnManager) resolveNodeIDFromAddress(addr string) (string, error) {
	if m.staticResolve != nil {
		if mapped, ok := m.staticResolve(addr); ok && mapped != "" {
			return addr, nil
		}
		m.staticMu.Lock()
		ids := m.staticPeerIDs
		m.staticMu.Unlock()
		for _, id := range ids {
			if mapped, ok := m.staticResolve(id); ok && clusterAddrsEquivalent(mapped, addr) {
				return id, nil
			}
		}
		return "", fmt.Errorf("node not found for address %s", addr)
	}
	if m.raft == nil {
		return "", errors.New("peer conn manager: no raft configuration (static pool)")
	}
	future := m.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return "", fmt.Errorf("get raft config: %w", err)
	}
	for _, srv := range future.Configuration().Servers {
		if clusterAddrsEquivalent(string(srv.Address), addr) {
			return string(srv.ID), nil
		}
	}
	return "", fmt.Errorf("node not found for address %s", addr)
}

// UseService acquires a service-lane connection, runs fn, and releases it.
func (m *PeerConnManager) UseService(peerNodeID, purpose string, fn func(*grpc.ClientConn) error) error {
	h, err := m.AcquireService(peerNodeID, purpose)
	if err != nil {
		return err
	}
	defer h.Release()
	if err := fn(h.GRPC()); err != nil {
		h.Invalidate(err)
		return err
	}
	return nil
}

// UseServiceCtx is UseService with a context passed to fn.
func (m *PeerConnManager) UseServiceCtx(ctx context.Context, peerNodeID, purpose string, fn func(context.Context, *grpc.ClientConn) error) error {
	h, err := m.AcquireService(peerNodeID, purpose)
	if err != nil {
		return err
	}
	defer h.Release()
	if err := fn(ctx, h.GRPC()); err != nil {
		h.Invalidate(err)
		return err
	}
	return nil
}
