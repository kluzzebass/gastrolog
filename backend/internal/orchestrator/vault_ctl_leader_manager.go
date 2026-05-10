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
	// vaultMembershipReconcileInterval is how often the leader epoch's
	// reconcile callback re-runs as a safety net. The primary trigger
	// for reconciliation is leadership gain (after raft.Barrier returns)
	// and explicit calls to SetDesiredMembers; the periodic tick catches
	// transient transitions where the explicit triggers were missed.
	vaultMembershipReconcileInterval = 30 * time.Second

	// vaultMembershipChangeTimeout bounds AddVoter / RemoveServer calls.
	vaultMembershipChangeTimeout = 10 * time.Second
)

// vaultCtlLeaderManager spawns and supervises per-vault leader loops. Each inst
// Raft group gets a raftgroup.LeaderLoop whose OnLead callback runs
// membership reconciliation and leadership alignment against the
// orchestrator's view of the desired state.
//
// Membership reconciliation and leadership transfer happen ONLY on the
// vault-ctl Raft leader, from inside its leader epoch (after raft.Barrier
// returns). If the current Raft leader doesn't match the desired leader
// (set by the placement manager), TransferLeadership moves Raft
// leadership to the desired node.
type vaultCtlLeaderManager struct {
	mu            sync.Mutex
	epochs        map[glid.GLID]context.CancelFunc
	desired       *vaultCtlMembershipMap
	desiredLeader *vaultCtlDesiredLeaderMap
	rootCtx       context.Context
	rootCxl       context.CancelFunc
	logger        *slog.Logger

	// onMemberRemoved fires after a successful RemoveServer call from
	// the leader's reconcile pass. The orchestrator wires this to
	// propose CmdPruneNode on every inst sub-FSM in the vault so
	// pendingDeletes ExpectedFrom obligations from the decommissioned
	// node don't block finalization. See gastrolog-51gme step 10.
	// Nil leaves the prune as a no-op (single-node tests, etc.).
	onMemberRemoved func(vaultID glid.GLID, removedNodeID string)
}

// SetOnMemberRemoved registers a callback invoked after the leader
// reconcile pass successfully removes a server from the vault-ctl Raft
// group's voter set. Idempotent: replaces any previously-registered
// callback. See gastrolog-51gme step 10.
func (m *vaultCtlLeaderManager) SetOnMemberRemoved(fn func(vaultID glid.GLID, removedNodeID string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onMemberRemoved = fn
}

// newVaultCtlLeaderManager supervises per-vault control-plane Raft leader epochs
// (membership reconcile + optional leadership transfer).
func newVaultCtlLeaderManager(logger *slog.Logger) *vaultCtlLeaderManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &vaultCtlLeaderManager{
		epochs:        make(map[glid.GLID]context.CancelFunc),
		desired:       newVaultCtlMembershipMap(),
		desiredLeader: newVaultCtlDesiredLeaderMap(),
		rootCtx:       ctx,
		rootCxl:       cancel,
		logger:        logger.With("component", "vault-ctl-leader-manager"),
	}
}

// Start spawns a leader loop for the given inst's Raft group if one is not
// already running. Idempotent: re-calling for the same inst ID is a no-op.
func (m *vaultCtlLeaderManager) Start(vaultID glid.GLID, group *raftgroup.Group) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.epochs[vaultID]; ok {
		return
	}

	epochCtx, cancel := context.WithCancel(m.rootCtx)
	m.epochs[vaultID] = cancel

	loop := raftgroup.NewLeaderLoop(raftgroup.LeaderLoopConfig{
		Group:  group,
		Name:   vaultID.String(),
		Logger: m.logger,
		OnLead: func(ctx context.Context) {
			m.runLeaderEpoch(ctx, vaultID, group)
		},
	})
	go loop.Run(epochCtx)
}

// Stop cancels the leader loop for a inst and clears its desired-member
// state. Safe to call even if no loop was started.
func (m *vaultCtlLeaderManager) Stop(vaultID glid.GLID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cancel, ok := m.epochs[vaultID]; ok {
		cancel()
		delete(m.epochs, vaultID)
	}
	m.desired.Delete(vaultID)
}

// StopAll cancels all leader loops. Called during orchestrator shutdown.
func (m *vaultCtlLeaderManager) StopAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id, cancel := range m.epochs {
		cancel()
		delete(m.epochs, id)
	}
	m.rootCxl()
}

// SetDesiredMembers updates the desired member list for a inst. The next
// reconcile pass on the vault-ctl Raft leader will apply the diff against the
// current Raft configuration.
func (m *vaultCtlLeaderManager) SetDesiredMembers(vaultID glid.GLID, members []hraft.Server) {
	m.desired.Set(vaultID, members)
}

// SetDesiredLeader sets the node that should be the vault-ctl Raft leader.
// If the current Raft leader differs, the leader epoch will call
// LeadershipTransferToServer to align them. Pass nil to clear.
func (m *vaultCtlLeaderManager) SetDesiredLeader(vaultID glid.GLID, server *hraft.Server) {
	m.desiredLeader.Set(vaultID, server)
}

// runLeaderEpoch runs the per-epoch reconcile loop. Called after Barrier()
// returns successfully on the leader. Exits when ctx is cancelled.
func (m *vaultCtlLeaderManager) runLeaderEpoch(ctx context.Context, vaultID glid.GLID, group *raftgroup.Group) {
	// Initial reconcile immediately after barrier.
	m.reconcile(vaultID, group)

	ticker := time.NewTicker(vaultMembershipReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.reconcile(vaultID, group)
		}
	}
}

// reconcile compares the desired member list for a inst against the current
// Raft configuration and applies the diff via AddVoter / RemoveServer.
// Bails on the first error (lost leadership, timeout, etc.) — the next pass
// (or the next epoch on the new leader) will pick up where we left off.
func (m *vaultCtlLeaderManager) reconcile(vaultID glid.GLID, group *raftgroup.Group) {
	desired := m.desired.Get(vaultID)
	if len(desired) == 0 {
		// No desired-members info yet. The orchestrator should have
		// called SetDesiredMembers before Start; if it didn't, the next
		// tick will catch up once it does.
		return
	}

	future := group.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		m.logger.Warn("get configuration failed",
			"vault", vaultID, "error", err)
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
		fut := group.Raft.AddVoter(srv.ID, srv.Address, 0, vaultMembershipChangeTimeout)
		if err := fut.Error(); err != nil {
			m.logger.Warn("AddVoter failed",
				"vault", vaultID, "node", srv.ID, "error", err)
			return // bail; next epoch will retry
		}
		m.logger.Info("added voter",
			"vault", vaultID, "node", srv.ID, "addr", srv.Address)
	}

	// Remove extras (voters or nonvoters that aren't in the desired set).
	for _, srv := range current {
		if _, want := desiredByID[srv.ID]; want {
			continue
		}
		fut := group.Raft.RemoveServer(srv.ID, 0, vaultMembershipChangeTimeout)
		if err := fut.Error(); err != nil {
			m.logger.Warn("RemoveServer failed",
				"vault", vaultID, "node", srv.ID, "error", err)
			return // bail; next epoch will retry
		}
		m.logger.Info("removed server",
			"vault", vaultID, "node", srv.ID)

		// Snapshot the callback under the lock so a concurrent
		// SetOnMemberRemoved doesn't race; fire outside the lock.
		// vaultID here is the vault-ctl Raft group ID (== vault ID per
		// reconfig_vaults.go's Start call site).
		m.mu.Lock()
		hook := m.onMemberRemoved
		m.mu.Unlock()
		if hook != nil {
			hook(vaultID, string(srv.ID))
		}
	}

	// Transfer leadership if the desired leader differs from the current
	// Raft leader. Only the current leader can initiate a transfer (which
	// is guaranteed — we're inside the leader epoch).
	m.transferIfNeeded(vaultID, group)
}

// transferIfNeeded checks whether the vault-ctl Raft leader matches the desired
// placement leader. If not, initiates LeadershipTransferToServer so the Raft
// leader aligns with the node that owns the data. This reduces FSM apply
// latency (no forwarding hop) and simplifies the operational model.
func (m *vaultCtlLeaderManager) transferIfNeeded(vaultID glid.GLID, group *raftgroup.Group) {
	want := m.desiredLeader.Get(vaultID)
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

	m.logger.Info("transferring vault-ctl Raft leadership",
		"vault", vaultID,
		"from", currentID,
		"to", want.ID)

	fut := group.Raft.LeadershipTransferToServer(want.ID, want.Address)
	if err := fut.Error(); err != nil {
		m.logger.Warn("leadership transfer failed",
			"vault", vaultID, "target", want.ID, "error", err)
	}
}

// vaultCtlMembershipMap is a thread-safe map of vaultID → desired member list.
// Writes happen from the dispatcher's path (when config changes); reads
// happen from inside leader epoch reconcile callbacks.
type vaultCtlMembershipMap struct {
	mu      sync.RWMutex
	desired map[glid.GLID][]hraft.Server
}

func newVaultCtlMembershipMap() *vaultCtlMembershipMap {
	return &vaultCtlMembershipMap{
		desired: make(map[glid.GLID][]hraft.Server),
	}
}

func (t *vaultCtlMembershipMap) Set(vaultID glid.GLID, members []hraft.Server) {
	t.mu.Lock()
	defer t.mu.Unlock()
	cp := make([]hraft.Server, len(members))
	copy(cp, members)
	t.desired[vaultID] = cp
}

func (t *vaultCtlMembershipMap) Get(vaultID glid.GLID) []hraft.Server {
	t.mu.RLock()
	defer t.mu.RUnlock()
	src := t.desired[vaultID]
	if src == nil {
		return nil
	}
	cp := make([]hraft.Server, len(src))
	copy(cp, src)
	return cp
}

func (t *vaultCtlMembershipMap) Delete(vaultID glid.GLID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.desired, vaultID)
}

// vaultCtlDesiredLeaderMap tracks which node should be the Raft leader for each inst.
type vaultCtlDesiredLeaderMap struct {
	mu      sync.RWMutex
	leaders map[glid.GLID]*hraft.Server
}

func newVaultCtlDesiredLeaderMap() *vaultCtlDesiredLeaderMap {
	return &vaultCtlDesiredLeaderMap{
		leaders: make(map[glid.GLID]*hraft.Server),
	}
}

func (t *vaultCtlDesiredLeaderMap) Set(vaultID glid.GLID, srv *hraft.Server) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if srv == nil {
		delete(t.leaders, vaultID)
	} else {
		cp := *srv
		t.leaders[vaultID] = &cp
	}
}

func (t *vaultCtlDesiredLeaderMap) Get(vaultID glid.GLID) *hraft.Server {
	t.mu.RLock()
	defer t.mu.RUnlock()
	srv := t.leaders[vaultID]
	if srv == nil {
		return nil
	}
	cp := *srv
	return &cp
}
