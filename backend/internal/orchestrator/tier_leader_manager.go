package orchestrator

import (
	"context"
	"gastrolog/internal/glid"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
)

const (
	// tierMembershipReconcileInterval is how often the leader epoch's
	// reconcile callback re-runs as a safety net. The primary trigger
	// for reconciliation is leadership gain (after raft.Barrier returns)
	// and explicit calls to SetDesiredMembers; the periodic tick catches
	// transient transitions where the explicit triggers were missed.
	tierMembershipReconcileInterval = 30 * time.Second

	// tierMembershipChangeTimeout bounds AddVoter / RemoveServer calls.
	tierMembershipChangeTimeout = 10 * time.Second
)

// tierLeaderManager spawns and supervises per-tier leader loops. Each tier
// Raft group gets a raftgroup.LeaderLoop whose OnLead callback runs
// membership reconciliation and leadership alignment against the
// orchestrator's view of the desired state.
//
// Membership reconciliation and leadership transfer happen ONLY on the
// tier Raft leader, from inside its leader epoch (after raft.Barrier
// returns). If the current Raft leader doesn't match the desired leader
// (set by the placement manager), TransferLeadership moves Raft
// leadership to the desired node.
type tierLeaderManager struct {
	mu            sync.Mutex
	epochs        map[glid.GLID]context.CancelFunc
	desired       *tierMembershipMap
	desiredLeader *tierDesiredLeaderMap
	rootCtx       context.Context
	rootCxl       context.CancelFunc
	logger        *slog.Logger
}

func newTierLeaderManager(logger *slog.Logger) *tierLeaderManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &tierLeaderManager{
		epochs:        make(map[glid.GLID]context.CancelFunc),
		desired:       newTierMembershipMap(),
		desiredLeader: newTierDesiredLeaderMap(),
		rootCtx:       ctx,
		rootCxl:       cancel,
		logger:        logger.With("component", "tier-leader-manager"),
	}
}

// newVaultCtlLeaderManager is like newTierLeaderManager but keys epochs and
// desired membership by vault ID for vault control-plane Raft groups.
func newVaultCtlLeaderManager(logger *slog.Logger) *tierLeaderManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &tierLeaderManager{
		epochs:        make(map[glid.GLID]context.CancelFunc),
		desired:       newTierMembershipMap(),
		desiredLeader: newTierDesiredLeaderMap(),
		rootCtx:       ctx,
		rootCxl:       cancel,
		logger:        logger.With("component", "vault-ctl-leader-manager"),
	}
}

// Start spawns a leader loop for the given tier's Raft group if one is not
// already running. Idempotent: re-calling for the same tier ID is a no-op.
func (m *tierLeaderManager) Start(tierID glid.GLID, group *raftgroup.Group) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.epochs[tierID]; ok {
		return
	}

	epochCtx, cancel := context.WithCancel(m.rootCtx)
	m.epochs[tierID] = cancel

	loop := raftgroup.NewLeaderLoop(raftgroup.LeaderLoopConfig{
		Group:  group,
		Name:   tierID.String(),
		Logger: m.logger,
		OnLead: func(ctx context.Context) {
			m.runLeaderEpoch(ctx, tierID, group)
		},
	})
	go loop.Run(epochCtx)
}

// Stop cancels the leader loop for a tier and clears its desired-member
// state. Safe to call even if no loop was started.
func (m *tierLeaderManager) Stop(tierID glid.GLID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cancel, ok := m.epochs[tierID]; ok {
		cancel()
		delete(m.epochs, tierID)
	}
	m.desired.Delete(tierID)
}

// StopAll cancels all leader loops. Called during orchestrator shutdown.
func (m *tierLeaderManager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, cancel := range m.epochs {
		cancel()
		delete(m.epochs, id)
	}
	m.rootCxl()
}

// SetDesiredMembers updates the desired member list for a tier. The next
// reconcile pass on the tier Raft leader will apply the diff against the
// current Raft configuration.
func (m *tierLeaderManager) SetDesiredMembers(tierID glid.GLID, members []hraft.Server) {
	m.desired.Set(tierID, members)
}

// SetDesiredLeader sets the node that should be the tier Raft leader.
// If the current Raft leader differs, the leader epoch will call
// LeadershipTransferToServer to align them. Pass nil to clear.
func (m *tierLeaderManager) SetDesiredLeader(tierID glid.GLID, server *hraft.Server) {
	m.desiredLeader.Set(tierID, server)
}

// runLeaderEpoch runs the per-epoch reconcile loop. Called after Barrier()
// returns successfully on the leader. Exits when ctx is cancelled.
func (m *tierLeaderManager) runLeaderEpoch(ctx context.Context, tierID glid.GLID, group *raftgroup.Group) {
	// Initial reconcile immediately after barrier.
	m.reconcile(tierID, group)

	ticker := time.NewTicker(tierMembershipReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcile(tierID, group)
		}
	}
}

// reconcile compares the desired member list for a tier against the current
// Raft configuration and applies the diff via AddVoter / RemoveServer.
// Bails on the first error (lost leadership, timeout, etc.) — the next pass
// (or the next epoch on the new leader) will pick up where we left off.
func (m *tierLeaderManager) reconcile(tierID glid.GLID, group *raftgroup.Group) {
	desired := m.desired.Get(tierID)
	if len(desired) == 0 {
		// No desired-members info yet. The orchestrator should have
		// called SetDesiredMembers before Start; if it didn't, the next
		// tick will catch up once it does.
		return
	}

	future := group.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		m.logger.Warn("get configuration failed",
			"tier", tierID, "error", err)
		return
	}
	current := future.Configuration().Servers

	desiredByID := make(map[hraft.ServerID]hraft.ServerAddress, len(desired))
	for _, srv := range desired {
		desiredByID[srv.ID] = srv.Address
	}
	currentByID := make(map[hraft.ServerID]bool, len(current))
	for _, srv := range current {
		currentByID[srv.ID] = true
	}

	// Add missing voters.
	for _, srv := range desired {
		if currentByID[srv.ID] {
			continue
		}
		fut := group.Raft.AddVoter(srv.ID, srv.Address, 0, tierMembershipChangeTimeout)
		if err := fut.Error(); err != nil {
			m.logger.Warn("AddVoter failed",
				"tier", tierID, "node", srv.ID, "error", err)
			return // bail; next epoch will retry
		}
		m.logger.Info("added voter",
			"tier", tierID, "node", srv.ID, "addr", srv.Address)
	}

	// Remove extras (voters or nonvoters that aren't in the desired set).
	for _, srv := range current {
		if _, want := desiredByID[srv.ID]; want {
			continue
		}
		fut := group.Raft.RemoveServer(srv.ID, 0, tierMembershipChangeTimeout)
		if err := fut.Error(); err != nil {
			m.logger.Warn("RemoveServer failed",
				"tier", tierID, "node", srv.ID, "error", err)
			return // bail; next epoch will retry
		}
		m.logger.Info("removed server",
			"tier", tierID, "node", srv.ID)
	}

	// Transfer leadership if the desired leader differs from the current
	// Raft leader. Only the current leader can initiate a transfer (which
	// is guaranteed — we're inside the leader epoch).
	m.transferIfNeeded(tierID, group)
}

// transferIfNeeded checks whether the tier Raft leader matches the desired
// placement leader. If not, initiates LeadershipTransferToServer so the Raft
// leader aligns with the node that owns the data. This reduces FSM apply
// latency (no forwarding hop) and simplifies the operational model.
func (m *tierLeaderManager) transferIfNeeded(tierID glid.GLID, group *raftgroup.Group) {
	want := m.desiredLeader.Get(tierID)
	if want == nil {
		return // no desired leader set (single-node or not yet configured)
	}
	currentLeader, currentID := group.Raft.LeaderWithID()
	if currentID == want.ID {
		return // already aligned
	}
	if currentLeader == "" {
		return // no leader elected yet
	}

	m.logger.Info("transferring tier Raft leadership",
		"tier", tierID,
		"from", currentID,
		"to", want.ID)

	fut := group.Raft.LeadershipTransferToServer(want.ID, want.Address)
	if err := fut.Error(); err != nil {
		m.logger.Warn("leadership transfer failed",
			"tier", tierID, "target", want.ID, "error", err)
	}
}

// tierMembershipMap is a thread-safe map of tierID → desired member list.
// Writes happen from the dispatcher's path (when config changes); reads
// happen from inside leader epoch reconcile callbacks.
type tierMembershipMap struct {
	mu      sync.RWMutex
	desired map[glid.GLID][]hraft.Server
}

func newTierMembershipMap() *tierMembershipMap {
	return &tierMembershipMap{
		desired: make(map[glid.GLID][]hraft.Server),
	}
}

func (t *tierMembershipMap) Set(tierID glid.GLID, members []hraft.Server) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cp := make([]hraft.Server, len(members))
	copy(cp, members)
	t.desired[tierID] = cp
}

func (t *tierMembershipMap) Get(tierID glid.GLID) []hraft.Server {
	t.mu.RLock()
	defer t.mu.RUnlock()
	src := t.desired[tierID]
	if src == nil {
		return nil
	}
	cp := make([]hraft.Server, len(src))
	copy(cp, src)
	return cp
}

func (t *tierMembershipMap) Delete(tierID glid.GLID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.desired, tierID)
}

// tierDesiredLeaderMap tracks which node should be the Raft leader for each tier.
type tierDesiredLeaderMap struct {
	mu      sync.RWMutex
	leaders map[glid.GLID]*hraft.Server
}

func newTierDesiredLeaderMap() *tierDesiredLeaderMap {
	return &tierDesiredLeaderMap{
		leaders: make(map[glid.GLID]*hraft.Server),
	}
}

func (t *tierDesiredLeaderMap) Set(tierID glid.GLID, srv *hraft.Server) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if srv == nil {
		delete(t.leaders, tierID)
	} else {
		cp := *srv
		t.leaders[tierID] = &cp
	}
}

func (t *tierDesiredLeaderMap) Get(tierID glid.GLID) *hraft.Server {
	t.mu.RLock()
	defer t.mu.RUnlock()
	srv := t.leaders[tierID]
	if srv == nil {
		return nil
	}
	cp := *srv
	return &cp
}
