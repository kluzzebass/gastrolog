package collection

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/notify"
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

type vaultCollect struct {
	vaultID  glid.GLID
	root     string
	log      LogReader
	pull     PullClient
	receipts ReceiptCommitter
	fsm      *vaultctlfsm.FSM
	// unsubPublish removes this vault's publish-callback subscription on the
	// shared FSM fan-out; nil when no FSM was wired.
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

	// collectWaiters receives the result of the worker's next pass. CollectOnce
	// registers here when the per-vault worker is running so chunking/planner
	// goroutines never block on passMu while a pass pulls segments or applies
	// holder receipts through vault-ctl Raft.
	collectWaitMu sync.Mutex
	collectWaiters  []collectWaiter
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
	return &vaultCollect{
		vaultID:   vaultID,
		root:      root,
		log:       cfg.Log,
		pull:      cfg.Pull,
		receipts:  cfg.Receipts,
		fsm:       cfg.FSM,
		wake:      notify.NewSignal(),
		receipted: make(map[glid.GLID]struct{}),
	}, nil
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
	prePath, err := PullToPreHead(ctx, v.root, ref.VaultID, ref.SegmentID, v.pull)
	if err != nil {
		return err
	}
	v.collectMu.Lock()
	v.notePreHead(ref.SegmentID)
	v.collectMu.Unlock()
	if _, err := PromoteVerified(prePath, v.root); err != nil {
		return err
	}
	v.collectMu.Lock()
	v.noteHead(ref.SegmentID)
	v.collectMu.Unlock()
	_, err = v.commitReceipt(ctx, ref)
	return err
}

// commitReceipt records that this node holds the segment and remembers it so a
// later pass does not re-commit before the receipt replicates into the holder
// set. Idempotent at the FSM layer too (CmdAckSegmentHolder de-dups), so a
// crash between commit and marking is harmless.
func (v *vaultCollect) commitReceipt(ctx context.Context, ref AssignedSegment) (bool, error) {
	v.collectMu.Lock()
	if _, done := v.receipted[ref.SegmentID]; done {
		v.collectMu.Unlock()
		return false, nil
	}
	v.collectMu.Unlock()
	if err := v.receipts.CommitHolderReceipt(ctx, ref.VaultID, ref.SegmentID); err != nil {
		return false, err
	}
	v.collectMu.Lock()
	v.receipted[ref.SegmentID] = struct{}{}
	v.collectMu.Unlock()
	return true, nil
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

func (v *vaultCollect) collectForRef(ctx context.Context, ref AssignedSegment) (bool, error) {
	switch v.planCollectAction(ref) {
	case collectSkip:
		return false, nil
	case collectReceiptOnly:
		committed, err := v.commitReceipt(ctx, ref)
		return committed, err
	case collectPull:
		err := v.collectOne(ctx, ref)
		return err == nil, err
	}
	return false, nil
}

func (v *vaultCollect) collectMissing(ctx context.Context) (bool, error) {
	v.passMu.Lock()
	defer v.passMu.Unlock()

	v.collectMu.Lock()
	assigned, err := v.log.Roll(ctx, v.vaultID)
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
	for _, ref := range work {
		made, err := v.collectForRef(ctx, ref)
		if made {
			progress = true
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return progress, errors.Join(errs...)
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
	for _, segmentID := range ids {
		if LocalSegmentPresent(v.root, segmentID) {
			continue
		}
		var checksum uint32
		if v.fsm != nil {
			if entry := v.fsm.GetCompletedSegment(segmentID); entry != nil {
				checksum = entry.Checksum
			}
		}
		ref := AssignedSegment{
			VaultID:   v.vaultID,
			SegmentID: segmentID,
			Checksum:  checksum,
		}
		if _, err := v.collectForRef(ctx, ref); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
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

func (v *vaultCollect) completeCollectWaiters(err error) {
	v.collectWaitMu.Lock()
	waiters := v.collectWaiters
	v.collectWaiters = nil
	v.collectWaitMu.Unlock()
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
	// OnPassComplete fires after each collect pass finishes without a hard
	// error. Wired by the orchestrator to wake chunking when head/ changes.
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
// pointers captured at register time.
func (m *Manager) RewireVaultFSM(vaultID glid.GLID, cfg VaultConfig) error {
	if cfg.FSM == nil {
		return errors.New("vault-ctl FSM required")
	}
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	m.unwireVaultFSMCallbacks(v)
	v.fsm = cfg.FSM
	if cfg.Log != nil {
		v.log = cfg.Log
	}
	if cfg.Pull != nil {
		v.pull = cfg.Pull
	}
	if cfg.Receipts != nil {
		v.receipts = cfg.Receipts
	}
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
}

// Notify triggers one collect pass for a vault when Run is active. Use when
// assignment or publish signals are not wired through the vault-ctl FSM (e.g.
// test harnesses).
func (m *Manager) Notify(vaultID glid.GLID) {
	m.triggerCollect(vaultID)
}

// CollectOnce rolls the log and collects missing segments for one vault (for tests
// and ChunkingManager materialization).
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
		if retryableCollectErr(err) {
			m.logCollectPassErr(log, err)
			return
		}
		m.logCollectPassErr(log, err)
		return
	}
	if progress && m.cfg.OnPassComplete != nil {
		m.cfg.OnPassComplete(v.vaultID)
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
		// Capture the wake channel BEFORE each pass so a signal arriving
		// mid-pass re-fires the loop instead of being lost.
		ch := v.wake.C()
		log := m.logger().With("vault", v.vaultID)
		progress, err := v.collectMissing(ctx)
		if ctx.Err() == nil {
			m.afterCollectPass(v, progress, err, log)
		}
		v.completeCollectWaiters(err)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
			}
			ch = v.wake.C()
			progress, err = v.collectMissing(ctx)
			if ctx.Err() == nil {
				m.afterCollectPass(v, progress, err, log)
			}
			v.completeCollectWaiters(err)
		}
	})
}
