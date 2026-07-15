package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"gastrolog/internal/notify"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
)

const (
	// vaultCtlMembershipReconcileJobName is the operator-visible
	// name for the per-vault-ctl membership-reconcile safety net.
	// Keep stable across releases.
	vaultCtlMembershipReconcileJobName = "vault-ctl-membership-reconcile"

	// vaultCtlMembershipReconcileSchedule runs every 30 seconds.
	// 6-field cron (with-seconds). Primary triggers for reconcile
	// are leadership-gain (after raft.Barrier) and explicit
	// SetDesiredMembers; this scheduled tick is the periodic safety
	// net that wakes every active leader epoch via desiredChanged
	// for cases where the explicit triggers were missed.
	vaultCtlMembershipReconcileSchedule = "*/30 * * * * *"

	// vaultMembershipChangeTimeout bounds the *log append* portion of
	// AddVoter / RemoveServer — the hashicorp/raft API treats this
	// argument as a per-leader-loop submission deadline, not a commit
	// deadline. The actual wait for the configuration change to commit
	// is wrapped in awaitFutureWithTimeout below.
	vaultMembershipChangeTimeout = 10 * time.Second

	// vaultMembershipMaxPerPass caps how many membership changes a single
	// reconcile pass issues before yielding. A burst scale-out (3 → 10
	// pods) used to ask the leader epoch to serialize 7+ AddVoter calls
	// back-to-back; each waits on the previous voter to catch up before
	// quorum can commit the next configuration change. Capping at a
	// small batch keeps each pass short and re-fires the desiredChanged
	// signal so the next batch starts immediately without waiting for
	// the 30 s safety-net tick. See gastrolog-5n6xz.
	vaultMembershipMaxPerPass = 2
)

// defaultCommitTimeout is the initial value of vaultCtlLeaderManager.commitTimeout.
// Held as an atomic on the manager (not a package global) so parallel tests
// that need a shorter timeout don't leak the override into unrelated tests'
// reconcile loops. See gastrolog-5n6xz.
const defaultCommitTimeout = 15 * time.Second

// awaitFutureWithTimeout wraps hashicorp/raft's IndexFuture / Future API,
// which does not natively support cancellation. Returns the future's error
// once it resolves, or a synthetic timeout error after the deadline. The
// underlying goroutine keeps waiting on the future (a one-shot resource);
// this is intentional — leaking a single goroutine per timed-out membership
// change is acceptable, and the future will eventually resolve (commit,
// leadership loss, or shutdown).
func awaitFutureWithTimeout(fut hraft.Future, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- fut.Error() }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return fmt.Errorf("vault-ctl membership future commit timeout after %s", timeout)
	}
}

// vaultCtlLeaderManager spawns and supervises per-vault leader loops. Each instance
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

	// misaligned holds each vault's last-observed wrong-leader sighting
	// for transferIfNeeded's two-pass damping (gastrolog-5kcq5q). Guarded
	// by mu; lazily allocated.
	misaligned map[glid.GLID]misalignedObservation

	// desiredChanged broadcasts to every leader epoch goroutine whenever
	// SetDesiredMembers updates a vault's target member set OR a reconcile
	// pass bails partway through a burst with more work to do. Replaces
	// the previous "wait for the 30 s safety-net tick" semantics: after
	// K8s scale-out fires a flurry of NotifyNodeConfigPut events into
	// RefreshVaultCtlMembers, the leader epochs converge in milliseconds
	// instead of seconds. Notify is a no-op when no epoch is waiting, so
	// signals are dropped harmlessly during shutdown. See gastrolog-5n6xz.
	desiredChanged *notify.Signal

	// commitTimeout bounds how long a single reconcile pass waits on an
	// AddVoter / RemoveServer future to commit. Defaults to
	// defaultCommitTimeout; tests override via setCommitTimeoutForTest.
	commitTimeout time.Duration

	// onMemberRemoved fires after a successful RemoveServer call from
	// the leader's reconcile pass. The orchestrator wires this to
	// propose CmdPruneNode on every instance sub-FSM in the vault so
	// pendingDeletes ExpectedFrom obligations from the decommissioned
	// node don't block finalization. See gastrolog-51gme step 10.
	// Nil leaves the prune as a no-op (single-node tests, etc.).
	onMemberRemoved func(vaultID glid.GLID, removedNodeID string)

	// onLeadGained fires at the start of each vault-ctl leader epoch so the
	// orchestrator can wake pipeline chunking (manifest planner + sealed build).
	onLeadGained func(vaultID glid.GLID)
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

// SetOnLeadGained registers a callback invoked at the start of each vault-ctl
// leader epoch (after Barrier). Used to wake pipeline chunking on the leader home.
func (m *vaultCtlLeaderManager) SetOnLeadGained(fn func(vaultID glid.GLID)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onLeadGained = fn
}

// newVaultCtlLeaderManager supervises per-vault control-plane Raft leader epochs
// (membership reconcile + optional leadership transfer).
func newVaultCtlLeaderManager(logger *slog.Logger) *vaultCtlLeaderManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &vaultCtlLeaderManager{
		epochs:         make(map[glid.GLID]context.CancelFunc),
		desired:        newVaultCtlMembershipMap(),
		desiredLeader:  newVaultCtlDesiredLeaderMap(),
		desiredChanged: notify.NewSignal(),
		commitTimeout:  defaultCommitTimeout,
		rootCtx:        ctx,
		rootCxl:        cancel,
		logger:         compVaultCtlLeader.Apply(logger),
	}
}

// Start spawns a leader loop for the given instance's Raft group if one is not
// already running. Idempotent: re-calling for the same instance ID is a no-op.
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

// Stop cancels the leader loop for an instance and clears its desired-member
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

// SetDesiredMembers updates the desired member list for an instance and
// wakes every leader epoch goroutine so the reconcile pass picks up the
// new target without waiting for the 30 s safety-net tick. Idempotent —
// callers may invoke it on every NotifyNodeConfigPut without batching.
func (m *vaultCtlLeaderManager) SetDesiredMembers(vaultID glid.GLID, members []hraft.Server) {
	m.desired.Set(vaultID, members)
	m.desiredChanged.Notify()
}

// SetDesiredLeader sets the node that should be the vault-ctl Raft leader.
// If the current Raft leader differs, the leader epoch will call
// LeadershipTransferToServer to align them. Pass nil to clear.
func (m *vaultCtlLeaderManager) SetDesiredLeader(vaultID glid.GLID, server *hraft.Server) {
	m.desiredLeader.Set(vaultID, server)
}

// SetDesiredLeaderID sets the desired vault-ctl Raft leader by node ID,
// resolving its address from the vault's desired member set. Every vault-ctl
// group spans all cluster nodes (gastrolog-292yi), so without alignment an
// election can land leadership on a node outside the vault's placement set —
// and the pipeline chunking planner (gated on home ∧ vault-ctl leadership)
// would then run nowhere, stalling manifest planning cluster-wide
// (gastrolog-18f9r, Rubicon E3). Called from reloadPipelineFromConfig with
// the placement leader on every config apply / placement sweep.
//
// No-ops (keeping any previous desired leader) while the node is not yet in
// the desired member set — membership reconcile must add it first; the next
// sweep pass retries. An empty nodeID clears the desired leader.
func (m *vaultCtlLeaderManager) SetDesiredLeaderID(vaultID glid.GLID, nodeID string) {
	if nodeID == "" {
		m.desiredLeader.Set(vaultID, nil)
		return
	}
	prev := m.desiredLeader.Get(vaultID)
	if prev != nil && string(prev.ID) == nodeID {
		return // unchanged — avoid waking every leader epoch each sweep tick
	}
	for _, srv := range m.desired.Get(vaultID) {
		if string(srv.ID) == nodeID {
			m.desiredLeader.Set(vaultID, &srv)
			// Wake the current leader's epoch so the transfer happens
			// promptly instead of on the 30s safety tick.
			m.desiredChanged.Notify()
			return
		}
	}
}

// safetyTick is the task fn invoked by the
// vault-ctl-membership-reconcile scheduled job. Pokes
// desiredChanged, which wakes every active leader epoch goroutine.
// Each woken epoch runs reconcile in its OWN goroutine — that
// preserves the existing single-reconciler-per-vault invariant
// (event-driven loop in runLeaderEpoch is the sole reconcile
// driver, the scheduler just wakes it). See gastrolog-11bla.
func (m *vaultCtlLeaderManager) safetyTick() {
	m.desiredChanged.Notify()
}

// startVaultCtlMembershipReconcile registers the membership-
// reconcile safety net with the orchestrator's job scheduler.
// Returns an error if AddJob fails. The Describe text explains
// the every-node + per-group-leader semantics for the operator.
// See gastrolog-11bla.
func (o *Orchestrator) startVaultCtlMembershipReconcile() error {
	if err := o.scheduler.AddJob(vaultCtlMembershipReconcileJobName, vaultCtlMembershipReconcileSchedule, o.vaultCtlLeaders.safetyTick); err != nil {
		return err
	}
	o.scheduler.Describe(vaultCtlMembershipReconcileJobName,
		"Per-vault-ctl membership reconcile safety net. Runs on every node every 30 seconds; wakes every active leader-epoch goroutine via the desiredChanged signal. The actual reconcile work runs in each epoch's existing goroutine (one per vault this node leads), so the single-reconciler-per-vault invariant is preserved. Primary triggers (leadership gain, SetDesiredMembers) remain event-driven; this is the fallback for cases neither catches.")
	return nil
}

// runLeaderEpoch runs the per-epoch reconcile loop. Called after Barrier()
// returns successfully on the leader. Exits when ctx is cancelled.
//
// The loop is event-driven: it wakes on ctx cancellation or on the
// desiredChanged signal (fired by SetDesiredMembers, by a reconcile
// pass that yielded mid-burst, OR by the periodic safety-net
// vault-ctl-membership-reconcile scheduled job — see
// startVaultCtlMembershipReconcile + gastrolog-11bla). The
// desiredChanged path is what keeps burst scale-out responsive —
// without it, a leftover voter would wait up to the scheduler tick
// to be picked up. See gastrolog-5n6xz.
func (m *vaultCtlLeaderManager) runLeaderEpoch(ctx context.Context, vaultID glid.GLID, group *raftgroup.Group) {
	// Initial reconcile immediately after barrier.
	m.reconcile(vaultID, group)
	if m.onLeadGained != nil {
		m.mu.Lock()
		fn := m.onLeadGained
		m.mu.Unlock()
		fn(vaultID)
	}

	for {
		wakeCh := m.desiredChanged.C()
		select {
		case <-ctx.Done():
			return
		case <-wakeCh:
			m.reconcile(vaultID, group)
		}
	}
}

// reconcile compares the desired member list for an instance against the current
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
	type memberState struct {
		addr     hraft.ServerAddress
		suffrage hraft.ServerSuffrage
	}
	currentByID := make(map[hraft.ServerID]memberState, len(current))
	for _, srv := range current {
		currentByID[srv.ID] = memberState{addr: srv.Address, suffrage: srv.Suffrage}
	}

	// Three cases per desired member:
	//   1. Not present → AddNonvoter (new joiner enters as learner;
	//      gastrolog-gcbx7 promotes once caught up).
	//   2. Present as Nonvoter, same address → skip; let the promoter
	//      handle it. Even on address drift we leave it alone — once
	//      promoted the next reconcile pass picks up address refresh
	//      via the case-3 path below.
	//   3. Present as Voter:
	//        - same address → skip (no-op).
	//        - different address → AddVoter to refresh (K8s pod IP
	//          change after rolling restart). See gastrolog-4zy8a.
	//
	// Membership changes are capped at vaultMembershipMaxPerPass per
	// pass. hashicorp/raft serializes configuration log entries; on
	// burst K8s scale-out the leader epoch used to issue 7+ AddVoter
	// calls back-to-back, each blocking on the previous to catch up
	// before quorum could commit the next entry. The cap yields after
	// a small batch and re-fires desiredChanged so the next batch
	// starts immediately on the same epoch. See gastrolog-5n6xz.
	added := 0
	moreToDo := false
	for _, srv := range desired {
		cur, present := currentByID[srv.ID]
		// Already a voter at the right address — nothing to do.
		if present && cur.suffrage == hraft.Voter && cur.addr == srv.Address {
			continue
		}
		// Already a nonvoter — leave to the per-vault-ctl learner
		// promoter (gastrolog-gcbx7).
		if present && cur.suffrage == hraft.Nonvoter {
			continue
		}
		if added >= vaultMembershipMaxPerPass {
			moreToDo = true
			break
		}
		var fut hraft.IndexFuture
		var action string
		if present {
			// Present as Voter at a different address → idempotent
			// refresh via AddVoter (rewrites the address).
			fut = group.Raft.AddVoter(srv.ID, srv.Address, 0, vaultMembershipChangeTimeout)
			action = "voter address updated"
		} else {
			// Genuinely new member → enters as learner. The
			// gastrolog-gcbx7 promoter upgrades once apply-index
			// catches up.
			fut = group.Raft.AddNonvoter(srv.ID, srv.Address, 0, vaultMembershipChangeTimeout)
			action = "added learner"
		}
		if err := awaitFutureWithTimeout(fut, m.commitTimeout); err != nil {
			// On commit timeout, the previous membership change is
			// still pending in hashicorp/raft's serialized
			// configuration channel — a burst of immediate retries
			// just produces "timed out enqueuing operation" errors at
			// the log-append boundary. Yield to the 30 s safety-net
			// tick instead; by then the in-flight call has either
			// committed or errored, freeing the channel.
			m.logger.Warn("membership change failed",
				"vault", vaultID, "node", srv.ID, "addr", srv.Address, "action", action, "error", err)
			return
		}
		m.logger.Info(action,
			"vault", vaultID, "node", srv.ID, "addr", srv.Address)
		added++
	}

	// Remove extras (voters or nonvoters that aren't in the desired set).
	// Counted against the same per-pass cap so a large scale-in can't
	// starve readers any more than scale-out can.
	for _, srv := range current {
		if _, want := desiredByID[srv.ID]; want {
			continue
		}
		if added >= vaultMembershipMaxPerPass {
			moreToDo = true
			break
		}
		fut := group.Raft.RemoveServer(srv.ID, 0, vaultMembershipChangeTimeout)
		if err := awaitFutureWithTimeout(fut, m.commitTimeout); err != nil {
			m.logger.Warn("RemoveServer failed",
				"vault", vaultID, "node", srv.ID, "error", err)
			return
		}
		m.logger.Info("removed server",
			"vault", vaultID, "node", srv.ID)
		added++

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

	// Wake the next pass right away when we yielded mid-burst. Without
	// this, leftovers wait up to vaultCtlMembershipReconcileSchedule (30 s)
	// even though everything they need is already in the desired map.
	if moreToDo {
		m.desiredChanged.Notify()
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
//
// Damped (gastrolog-5kcq5q): the transfer fires only after the SAME
// misaligned leader is observed on two consecutive passes. The undamped
// aligner amplified every organic election into a multi-term cascade —
// the wrong node wins, its epoch callback transfers immediately, the
// transfer triggers another election that a third node can win, which
// transfers again (307 commanded transfers logged; term bursts surfaced
// as WARN-level election storms). One sighting of misalignment right
// after an election is expected settling noise; misalignment that
// survives to the NEXT pass is a settled wrong leader worth moving.
// Observation-count hysteresis, not wall-clock — the pass cadence
// (leader-epoch events, desiredChanged notifies, the 30s membership
// reconcile) is the clock.
func (m *vaultCtlLeaderManager) transferIfNeeded(vaultID glid.GLID, group *raftgroup.Group) {
	want := m.desiredLeader.Get(vaultID)
	if want == nil {
		return // no desired leader set (single-node or not yet configured)
	}
	currentLeader, currentID := group.Raft.LeaderWithID()
	if currentID == want.ID {
		m.clearMisalignment(vaultID)
		return // already aligned
	}
	if currentLeader == "" {
		// No leader elected yet — not evidence of a settled wrong leader.
		m.clearMisalignment(vaultID)
		return
	}
	if !m.confirmMisalignment(vaultID, currentID, want.ID) {
		return // first sighting — let the group settle before commanding a move
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
	// The commanded transfer changes leadership again; the next pass
	// re-observes from scratch either way.
	m.clearMisalignment(vaultID)
}

// misalignedObservation records one pass's sighting of a wrong vault-ctl
// leader: which leader was seen and which target was desired. A repeat
// sighting of the identical pair on the next pass confirms the
// misalignment as settled.
type misalignedObservation struct {
	currentID hraft.ServerID
	wantID    hraft.ServerID
}

// confirmMisalignment reports whether this exact (current, want)
// misalignment was already observed on a previous pass. First sightings
// (or sightings where either side changed — an election or a placement
// change happened in between) record the observation and return false.
func (m *vaultCtlLeaderManager) confirmMisalignment(vaultID glid.GLID, currentID, wantID hraft.ServerID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.misaligned == nil {
		m.misaligned = make(map[glid.GLID]misalignedObservation)
	}
	obs := misalignedObservation{currentID: currentID, wantID: wantID}
	if m.misaligned[vaultID] == obs {
		return true
	}
	m.misaligned[vaultID] = obs
	return false
}

// clearMisalignment drops the vault's pending misalignment observation —
// the group is aligned, leaderless, or a transfer was just commanded.
func (m *vaultCtlLeaderManager) clearMisalignment(vaultID glid.GLID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.misaligned, vaultID)
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

// vaultCtlDesiredLeaderMap tracks which node should be the Raft leader for each instance.
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
