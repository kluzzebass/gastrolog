package collection

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	erragg "gastrolog/internal/errs"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/notify"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("collection manager not running")

// ErrUnknownVault is returned for an unregistered vault.
var ErrUnknownVault = errors.New("unknown vault")

// VaultConfig is per-vault collection state.
type VaultConfig struct {
	Log      LogReader
	Pull     PullClient
	Receipts ReceiptCommitter
	// FSM wires SetOnPublishCompletedSegment to roll the log and collect
	// when new segment metadata is replicated. Optional when Notify is used.
	FSM *vaultctlfsm.FSM
}

// collectDeps bundles the rewireable collaborators (log reader, pull client,
// receipt committer, vault-ctl FSM). Published atomically as ONE immutable
// snapshot: RewireVaultFSM after a group snapshot Restore used to overwrite
// the fields one by one while worker goroutines were mid-pass — a data race
// and a window where a worker saw a fresh FSM with a stale pull client
// (gastrolog-50m2vi). Workers Load a snapshot per use.
type collectDeps struct {
	log      LogReader
	pull     PullClient
	receipts ReceiptCommitter
	fsm      *vaultctlfsm.FSM
}

type vaultCollect struct {
	vaultID glid.GLID
	root    string
	// deps is the current collaborator bundle; never nil after construction.
	deps atomic.Pointer[collectDeps]
	// unsubPublish removes this vault's publish-callback subscription on the
	// shared FSM fan-out; nil when no FSM was wired. Guarded by Manager.mu
	// (RegisterVault, RewireVaultFSM, UnregisterVault).
	unsubPublish func()
	// wake coalesces collect triggers for the per-vault worker goroutine.
	// FSM publish callbacks and Notify only poke this signal — they must
	// never run a collect pass inline, because the publish callback fires
	// on the Raft FSM-apply goroutine and a collect pass commits holder
	// receipts through raft.Apply on the same group. Running it inline on
	// a node that is both home and vault-ctl leader deadlocks the FSM
	// (the apply waits on an FSM that is busy running the callback) and
	// wedges every Raft group sharing the node's multiraft transport.
	wake *notify.Signal
	// stopWorker cancels the per-vault worker; nil until the worker starts.
	stopWorker context.CancelFunc

	// retryTimer re-arms the wake after a pass ends with only retryable
	// errors (catch-up race: registry lists a segment no holder can serve
	// yet, or a holder purged after seal). Those obligations have no future
	// event of their own — the publish that assigned them already fired, and
	// for the last segments of a burst no later publish arrives to piggyback
	// on (gastrolog-38snf4: follower homes stalled forever missing sealed
	// GLCB segments). One-shot with exponential backoff, armed only while an
	// obligation is failing; a healthy vault has no timer.
	retryMu    sync.Mutex
	retryTimer *time.Timer
	retryDelay time.Duration

	// passMu serializes full collect passes. collectMu protects layout and
	// receipted only — never held across pull or vault-ctl apply I/O.
	passMu    sync.Mutex
	collectMu sync.Mutex

	// layout mirrors head/ and pre-head/ segment IDs. Refreshed at the start of
	// every collect pass so a segment promoted to head/ by distribution
	// (LocalHolder) after an earlier pass is visible for receipt-without-pull.
	layout struct {
		head    map[glid.GLID]struct{}
		preHead map[glid.GLID]struct{}
	}

	// receipted tracks segment IDs this manager has already committed a holder
	// receipt for, so repeated passes (the production LogReader keeps assigning
	// a segment until the receipt replicates into its holder set) do not
	// re-commit. Bounded by the vault's live segment set; released in slice D.
	receipted map[glid.GLID]struct{}

	// Stage-throughput counters (gastrolog-10n6k8): records/bytes arriving
	// in head/ on this node, via remote pull or local-holder promotion.
	// Rates are derived downstream by the stats collector windows.
	collectedRecords atomic.Uint64
	collectedBytes   atomic.Uint64

	// collectWaiters receives the result of the worker's next pass. CollectOnce
	// registers here when the per-vault worker is running so chunking/planner
	// goroutines never block on passMu while a pass pulls segments or applies
	// holder receipts through vault-ctl Raft.
	collectWaitMu  sync.Mutex
	collectWaiters []collectWaiter
}

type collectWaiter struct {
	done chan error
}

func newVaultCollect(vaultID glid.GLID, root string, cfg VaultConfig) (*vaultCollect, error) {
	if cfg.Log == nil {
		return nil, errors.New("log reader required")
	}
	if cfg.Pull == nil {
		return nil, errors.New("pull client required")
	}
	if cfg.Receipts == nil {
		return nil, errors.New("receipt committer required")
	}
	v := &vaultCollect{
		vaultID:   vaultID,
		root:      root,
		wake:      notify.NewSignal(),
		receipted: make(map[glid.GLID]struct{}),
	}
	v.deps.Store(&collectDeps{
		log:      cfg.Log,
		pull:     cfg.Pull,
		receipts: cfg.Receipts,
		fsm:      cfg.FSM,
	})
	return v, nil
}

func (v *vaultCollect) refreshLayout() error {
	head, preHead, err := vaultSegmentLayout(v.root)
	if err != nil {
		return err
	}
	v.layout.head = head
	v.layout.preHead = preHead
	return nil
}

func (v *vaultCollect) notePreHead(segmentID glid.GLID) {
	if v.layout.preHead == nil {
		v.layout.preHead = make(map[glid.GLID]struct{})
	}
	v.layout.preHead[segmentID] = struct{}{}
}

func (v *vaultCollect) noteHead(segmentID glid.GLID) {
	if v.layout.head == nil {
		v.layout.head = make(map[glid.GLID]struct{})
	}
	v.layout.head[segmentID] = struct{}{}
	delete(v.layout.preHead, segmentID)
}

func (v *vaultCollect) collectOne(ctx context.Context, ref AssignedSegment) error {
	prePath, err := PullToPreHead(ctx, v.root, ref.VaultID, ref.SegmentID, v.deps.Load().pull)
	if err != nil {
		return err
	}
	v.collectMu.Lock()
	v.notePreHead(ref.SegmentID)
	v.collectMu.Unlock()
	dest, hdr, err := PromoteVerified(prePath, v.root)
	if err != nil {
		return err
	}
	v.noteHeadArrival(dest, hdr)
	v.collectMu.Lock()
	v.noteHead(ref.SegmentID)
	v.collectMu.Unlock()
	// The holder receipt is committed by the caller's end-of-pass batch
	// (commitReceipts), never per segment.
	return nil
}

// noteHeadArrival counts a segment's records/bytes as home-side ingress for
// the stage throughput gauges (gastrolog-10n6k8).
func (v *vaultCollect) noteHeadArrival(path string, hdr segment.Header) {
	v.collectedRecords.Add(uint64(hdr.RecordCount))
	if info, err := os.Stat(path); err == nil {
		v.collectedBytes.Add(uint64(info.Size())) //nolint:gosec // sizes are non-negative
	}
}

// commitReceipts records this node as holder for every given segment in ONE
// vault-ctl apply, then marks them locally so later passes skip re-commits.
// Batching is load-bearing: one Raft round per pass instead of per segment —
// sequential per-segment applies serialized whole passes (under passMu)
// behind the publish flood and starved leader-home GLCB builds
// (gastrolog-38snf4). Idempotent at the FSM layer too (CmdAckSegmentHolder
// de-dups per segment), so a crash between commit and marking is harmless.
// Returns how many receipts were freshly committed.
func (v *vaultCollect) commitReceipts(ctx context.Context, ids []glid.GLID) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	v.collectMu.Lock()
	fresh := make([]glid.GLID, 0, len(ids))
	seen := make(map[glid.GLID]struct{}, len(ids))
	for _, id := range ids {
		if _, done := v.receipted[id]; done {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		fresh = append(fresh, id)
	}
	v.collectMu.Unlock()
	if len(fresh) == 0 {
		return 0, nil
	}
	if err := v.deps.Load().receipts.CommitHolderReceipts(ctx, v.vaultID, fresh); err != nil {
		return 0, err
	}
	v.collectMu.Lock()
	for _, id := range fresh {
		v.receipted[id] = struct{}{}
	}
	v.collectMu.Unlock()
	return len(fresh), nil
}

type collectAction int

const (
	collectSkip collectAction = iota
	collectReceiptOnly
	collectPull
)

func (v *vaultCollect) planCollectAction(ref AssignedSegment) collectAction {
	v.collectMu.Lock()
	defer v.collectMu.Unlock()
	if _, done := v.receipted[ref.SegmentID]; done {
		if _, ok := v.layout.head[ref.SegmentID]; ok {
			return collectSkip
		}
		// Holder receipt recorded but head/ was purged after a partial
		// chunking pass — re-pull so the planner can resume the segment.
	} else if _, ok := v.layout.head[ref.SegmentID]; ok {
		return collectReceiptOnly
	}
	if _, ok := v.layout.preHead[ref.SegmentID]; ok {
		return collectSkip
	}
	return collectPull
}

// collectForRef pulls a segment when needed and reports (pulled, needsAck):
// needsAck segments are receipt-committed in one batch at the end of the
// pass (commitReceipts), never per ref.
func (v *vaultCollect) collectForRef(ctx context.Context, ref AssignedSegment) (pulled, needsAck bool, err error) {
	switch v.planCollectAction(ref) {
	case collectSkip:
		return false, false, nil
	case collectReceiptOnly:
		return false, true, nil
	case collectPull:
		if err := v.collectOne(ctx, ref); err != nil {
			return false, false, err
		}
		return true, true, nil
	}
	return false, false, nil
}

func (v *vaultCollect) collectMissing(ctx context.Context) (bool, error) {
	v.passMu.Lock()
	defer v.passMu.Unlock()

	v.collectMu.Lock()
	assigned, err := v.deps.Load().log.Roll(ctx, v.vaultID)
	if err != nil {
		v.collectMu.Unlock()
		return false, err
	}
	if len(assigned) == 0 {
		v.collectMu.Unlock()
		return false, nil
	}
	if err := v.refreshLayout(); err != nil {
		v.collectMu.Unlock()
		return false, err
	}
	work := append([]AssignedSegment(nil), assigned...)
	v.collectMu.Unlock()

	var errs []error
	var progress bool
	var toAck []glid.GLID
	for _, ref := range work {
		pulled, needsAck, err := v.collectForRef(ctx, ref)
		if pulled {
			progress = true
		}
		if needsAck {
			toAck = append(toAck, ref.SegmentID)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	committed, err := v.commitReceipts(ctx, toAck)
	if committed > 0 {
		progress = true
	}
	if err != nil {
		errs = append(errs, err)
	}
	return progress, erragg.SummaryJoin(errs...)
}

// CollectSegments pulls specific segment IDs into head/ when they are absent
// locally. Used when chunking build fails with MissingSegmentsError so bytes
// referenced by a sealed-pending manifest are re-fetched even if the planner
// resume cursor already reached RecordCount.
func (m *Manager) CollectSegments(ctx context.Context, vaultID glid.GLID, segmentIDs []glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.collectSegments(ctx, segmentIDs)
}

func (v *vaultCollect) collectSegments(ctx context.Context, segmentIDs []glid.GLID) error {
	if len(segmentIDs) == 0 {
		return nil
	}
	v.passMu.Lock()
	defer v.passMu.Unlock()

	v.collectMu.Lock()
	if err := v.refreshLayout(); err != nil {
		v.collectMu.Unlock()
		return err
	}
	ids := append([]glid.GLID(nil), segmentIDs...)
	v.collectMu.Unlock()

	var errs []error
	var toAck []glid.GLID
	for _, segmentID := range ids {
		if LocalSegmentPresent(v.root, segmentID) {
			continue
		}
		var checksum uint32
		if fsm := v.deps.Load().fsm; fsm != nil {
			if entry := fsm.GetCompletedSegment(segmentID); entry != nil {
				checksum = entry.Checksum
			}
		}
		ref := AssignedSegment{
			VaultID:   v.vaultID,
			SegmentID: segmentID,
			Checksum:  checksum,
		}
		_, needsAck, err := v.collectForRef(ctx, ref)
		if needsAck {
			toAck = append(toAck, segmentID)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	if _, err := v.commitReceipts(ctx, toAck); err != nil {
		errs = append(errs, err)
	}
	return erragg.SummaryJoin(errs...)
}

func (v *vaultCollect) awaitCollectPass(ctx context.Context) error {
	w := collectWaiter{done: make(chan error, 1)}
	v.collectWaitMu.Lock()
	v.collectWaiters = append(v.collectWaiters, w)
	v.collectWaitMu.Unlock()
	v.wake.Notify()
	select {
	case err := <-w.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// takeCollectWaiters snapshots the waiters registered so far. The worker calls
// this at pass START: a pass may only satisfy requests made before it began
// observing log state. A waiter that registers mid-pass stays queued — its
// Notify has already closed the wake channel captured before the pass, so the
// loop re-fires and the NEXT pass (which observes post-registration state)
// completes it. Completing mid-pass registrants with the in-flight pass's
// result returned stale success (gastrolog-38snf4 gate finding).
func (v *vaultCollect) takeCollectWaiters() []collectWaiter {
	v.collectWaitMu.Lock()
	defer v.collectWaitMu.Unlock()
	waiters := v.collectWaiters
	v.collectWaiters = nil
	return waiters
}

func completeCollectWaiters(waiters []collectWaiter, err error) {
	for _, w := range waiters {
		select {
		case w.done <- err:
		default:
		}
	}
}

// Config configures a CollectionManager.
type Config struct {
	Logger *slog.Logger
	// OnPassComplete fires after every collect pass that made progress,
	// including passes where other segments failed retryably — head/ changed,
	// so downstream must be woken for what landed. Wired by the orchestrator
	// to wake chunking.
	OnPassComplete func(vaultID glid.GLID)
}

// Manager pulls assigned segments into pre-head, verifies, and promotes to head.
// Collection passes are driven by vault-ctl FSM SetOnPublishCompletedSegment
// (when FSM is wired), explicit Notify calls, and CollectOnce from chunking — not by
// polling FSM or log state on a timer.
type Manager struct {
	cfg Config

	mu     sync.Mutex
	vaults map[glid.GLID]*vaultCollect
	runCtx context.Context

	running atomic.Bool
	wg      sync.WaitGroup
}

// New returns a collection manager.
func New(cfg Config) *Manager {
	cfg.Logger = compCollection.Apply(logging.Default(cfg.Logger))
	return &Manager{
		cfg:    cfg,
		vaults: make(map[glid.GLID]*vaultCollect),
	}
}

func (m *Manager) logger() *slog.Logger {
	return m.cfg.Logger
}

// RegisterVault adds a home vault collection path. Safe before or during Run.
func (m *Manager) RegisterVault(vaultID glid.GLID, root string, cfg VaultConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.vaults[vaultID]; ok {
		return errors.New("vault already registered")
	}
	v, err := newVaultCollect(vaultID, root, cfg)
	if err != nil {
		return err
	}
	m.vaults[vaultID] = v
	m.wireVaultFSMCallbacks(v, cfg.FSM)
	if m.runCtx != nil {
		m.startWorkerLocked(v)
	}
	return nil
}

// RewireVaultFSM rebinds collection to a fresh vault-ctl sub-FSM after snapshot
// Restore. The log reader and pull client must be updated too — they hold FSM
// pointers captured at register time. The whole bundle is published as one
// atomic snapshot; Manager.mu is held across the body so concurrent rewires
// (placement sweep vs route reload both reach this path) serialize and the
// unsub/wire callback bookkeeping cannot interleave (gastrolog-50m2vi).
func (m *Manager) RewireVaultFSM(vaultID glid.GLID, cfg VaultConfig) error {
	if cfg.FSM == nil {
		return errors.New("vault-ctl FSM required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vaults[vaultID]
	if !ok {
		return ErrUnknownVault
	}
	m.unwireVaultFSMCallbacks(v)
	prev := v.deps.Load()
	next := &collectDeps{
		log:      prev.log,
		pull:     prev.pull,
		receipts: prev.receipts,
		fsm:      cfg.FSM,
	}
	if cfg.Log != nil {
		next.log = cfg.Log
	}
	if cfg.Pull != nil {
		next.pull = cfg.Pull
	}
	if cfg.Receipts != nil {
		next.receipts = cfg.Receipts
	}
	v.deps.Store(next)
	m.wireVaultFSMCallbacks(v, cfg.FSM)
	v.wake.Notify()
	return nil
}

func (m *Manager) wireVaultFSMCallbacks(v *vaultCollect, fsm *vaultctlfsm.FSM) {
	if fsm == nil {
		return
	}
	v.unsubPublish = fsm.AddOnPublishCompletedSegment(func(vaultctlfsm.CompletedSegmentEntry) {
		v.wake.Notify()
	})
}

func (m *Manager) unwireVaultFSMCallbacks(v *vaultCollect) {
	if v.unsubPublish != nil {
		v.unsubPublish()
		v.unsubPublish = nil
	}
}

// UnregisterVault removes a vault from collection.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	delete(m.vaults, vaultID)
	m.mu.Unlock()
	if !ok {
		return
	}
	m.unwireVaultFSMCallbacks(v)
	if v.stopWorker != nil {
		v.stopWorker()
	}
	v.stopRetryWake()
}

// Notify triggers one collect pass for a vault when Run is active. Use when
// assignment or publish signals are not wired through the vault-ctl FSM (e.g.
// test harnesses).
func (m *Manager) Notify(vaultID glid.GLID) {
	m.triggerCollect(vaultID)
}

// CollectOnce rolls the log and collects missing segments for one vault (for tests
// and ChunkingManager materialization).
// VaultCollectStats is one vault's cumulative home-side ingress counters
// (records/bytes arrived in head/ on this node) — gastrolog-10n6k8.
type VaultCollectStats struct {
	VaultID          glid.GLID
	CollectedRecords uint64
	CollectedBytes   uint64
}

// CollectStats returns per-vault cumulative collection counters.
func (m *Manager) CollectStats() []VaultCollectStats {
	m.mu.Lock()
	vaults := make(map[glid.GLID]*vaultCollect, len(m.vaults))
	maps.Copy(vaults, m.vaults)
	m.mu.Unlock()
	out := make([]VaultCollectStats, 0, len(vaults))
	for vaultID, v := range vaults {
		out = append(out, VaultCollectStats{
			VaultID:          vaultID,
			CollectedRecords: v.collectedRecords.Load(),
			CollectedBytes:   v.collectedBytes.Load(),
		})
	}
	slices.SortFunc(out, func(a, b VaultCollectStats) int { return a.VaultID.Compare(b.VaultID) })
	return out
}

// NoteLocalHeadArrival counts a locally-promoted segment (origin == home:
// distribution renames completed/ into head/ without a pull) as home-side
// ingress, reading only the fixed header (gastrolog-10n6k8).
func (m *Manager) NoteLocalHeadArrival(vaultID, segmentID glid.GLID) {
	m.mu.Lock()
	v := m.vaults[vaultID]
	m.mu.Unlock()
	if v == nil {
		return
	}
	path := paths.HeadSegment(v.root, segmentID)
	hdr, err := segment.ReadHeader(path)
	if err != nil {
		return
	}
	v.noteHeadArrival(path, hdr)
}

func (m *Manager) CollectOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	if v.stopWorker != nil {
		return v.awaitCollectPass(ctx)
	}
	_, err := v.collectMissing(ctx)
	return err
}

// Run blocks until ctx is cancelled. Each registered vault gets a worker
// goroutine that runs an initial catch-up pass (assignments already visible
// after Raft replay) and then collects on every wake signal.
func (m *Manager) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	for _, v := range m.vaults {
		m.startWorkerLocked(v)
	}
	m.mu.Unlock()

	m.wg.Go(func() {
		<-ctx.Done()
	})

	m.wg.Wait()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()
	return ctx.Err()
}

func (m *Manager) triggerCollect(vaultID glid.GLID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return
	}
	v.wake.Notify()
}

func (m *Manager) logCollectPassErr(log *slog.Logger, err error) {
	if retryableCollectErr(err) {
		log.Debug("collect pass deferred", "error", err)
		return
	}
	log.Warn("collect pass failed", "error", err)
}

func (m *Manager) afterCollectPass(v *vaultCollect, progress bool, err error, log *slog.Logger) {
	if err != nil {
		m.logCollectPassErr(log, err)
		if retryableCollectErr(err) {
			// "Deferred" must actually defer: nothing else retries these
			// obligations once the burst's publish events are spent.
			v.scheduleRetryWake()
		}
	} else {
		v.resetRetryBackoff()
	}
	// Signal progress even when other segments in the pass failed —
	// downstream (chunking GLCB build) must be woken for what DID land.
	if progress && m.cfg.OnPassComplete != nil {
		m.cfg.OnPassComplete(v.vaultID)
	}
}

// collectRetryBaseDelay/collectRetryMaxDelay bound the deferred-pass retry
// backoff: quick first retry for transient races, 2s steady-state while an
// origin stays unreachable.
const (
	collectRetryBaseDelay = 50 * time.Millisecond
	collectRetryMaxDelay  = 2 * time.Second
)

// scheduleRetryWake arms a one-shot backoff wake so a deferred pass retries
// without depending on future publish events. Not a poll: the timer exists
// only while a retryable obligation is outstanding.
func (v *vaultCollect) scheduleRetryWake() {
	v.retryMu.Lock()
	defer v.retryMu.Unlock()
	switch {
	case v.retryDelay == 0:
		v.retryDelay = collectRetryBaseDelay
	case v.retryDelay < collectRetryMaxDelay:
		v.retryDelay *= 2
	}
	if v.retryTimer != nil {
		v.retryTimer.Stop()
	}
	v.retryTimer = time.AfterFunc(v.retryDelay, v.wake.Notify)
}

// resetRetryBackoff clears the backoff after a clean pass.
func (v *vaultCollect) resetRetryBackoff() {
	v.retryMu.Lock()
	defer v.retryMu.Unlock()
	v.retryDelay = 0
}

// stopRetryWake cancels any pending retry wake (vault unregistration).
func (v *vaultCollect) stopRetryWake() {
	v.retryMu.Lock()
	defer v.retryMu.Unlock()
	if v.retryTimer != nil {
		v.retryTimer.Stop()
		v.retryTimer = nil
	}
}

// startWorkerLocked launches the per-vault collect worker. Caller holds m.mu
// and has verified m.runCtx is non-nil. The worker decouples collect passes
// from their triggers: FSM publish callbacks fire on the Raft FSM-apply
// goroutine and must never block on a pass that itself applies Raft commands
// (holder receipts) — see the wake field comment on vaultCollect.
func (m *Manager) startWorkerLocked(v *vaultCollect) {
	if v.stopWorker != nil {
		return // already running
	}
	ctx, cancel := context.WithCancel(m.runCtx)
	v.stopWorker = cancel
	m.wg.Go(func() {
		// Waiters queued when the worker stops must not hang CollectOnce
		// callers that passed a non-cancellable context.
		defer func() { completeCollectWaiters(v.takeCollectWaiters(), ctx.Err()) }()
		// Capture the wake channel BEFORE each pass so a signal arriving
		// mid-pass re-fires the loop instead of being lost, and snapshot the
		// waiter queue AFTER capturing it — see takeCollectWaiters.
		ch := v.wake.C()
		waiters := v.takeCollectWaiters()
		log := m.logger().With("vault", v.vaultID)
		progress, err := v.collectMissing(ctx)
		if ctx.Err() == nil {
			m.afterCollectPass(v, progress, err, log)
		}
		completeCollectWaiters(waiters, err)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
			}
			ch = v.wake.C()
			waiters = v.takeCollectWaiters()
			progress, err = v.collectMissing(ctx)
			if ctx.Err() == nil {
				m.afterCollectPass(v, progress, err, log)
			}
			completeCollectWaiters(waiters, err)
		}
	})
}
