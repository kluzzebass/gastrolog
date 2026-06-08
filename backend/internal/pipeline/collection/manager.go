package collection

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
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

	// collectMu serializes collect passes for this vault. Passes are triggered
	// from several goroutines (Run startup, FSM publish callback, Notify,
	// CollectOnce) and must not overlap: they share the layout cache and the
	// receipted set, and concurrent pulls of the same segment are wasteful.
	collectMu sync.Mutex

	// layout caches head/pre-head segment IDs to avoid rescanning directories
	// on every collect pass once warmed.
	layout struct {
		loaded  bool
		head    map[glid.GLID]struct{}
		preHead map[glid.GLID]struct{}
	}

	// receipted tracks segment IDs this manager has already committed a holder
	// receipt for, so repeated passes (the production LogReader keeps assigning
	// a segment until the receipt replicates into its holder set) do not
	// re-commit. Bounded by the vault's live segment set; released in slice D.
	receipted map[glid.GLID]struct{}
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
		receipted: make(map[glid.GLID]struct{}),
	}, nil
}

func (v *vaultCollect) ensureLayout() error {
	if v.layout.loaded {
		return nil
	}
	head, preHead, err := vaultSegmentLayout(v.root)
	if err != nil {
		return err
	}
	v.layout.head = head
	v.layout.preHead = preHead
	v.layout.loaded = true
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
	var buf bytes.Buffer
	if err := v.pull.Pull(ctx, ref.VaultID, ref.SegmentID, &buf); err != nil {
		return err
	}
	prePath, err := ReceiveToPreHead(v.root, ref.SegmentID, &buf)
	if err != nil {
		return err
	}
	v.notePreHead(ref.SegmentID)
	if _, err := PromoteVerified(prePath, v.root); err != nil {
		return err
	}
	v.noteHead(ref.SegmentID)
	return v.commitReceipt(ctx, ref)
}

// commitReceipt records that this node holds the segment and remembers it so a
// later pass does not re-commit before the receipt replicates into the holder
// set. Idempotent at the FSM layer too (CmdAckSegmentHolder de-dups), so a
// crash between commit and marking is harmless.
func (v *vaultCollect) commitReceipt(ctx context.Context, ref AssignedSegment) error {
	if err := v.receipts.CommitHolderReceipt(ctx, ref.VaultID, ref.SegmentID); err != nil {
		return err
	}
	v.receipted[ref.SegmentID] = struct{}{}
	return nil
}

func (v *vaultCollect) collectMissing(ctx context.Context) error {
	v.collectMu.Lock()
	defer v.collectMu.Unlock()

	assigned, err := v.log.Roll(ctx, v.vaultID)
	if err != nil {
		return err
	}
	if len(assigned) == 0 {
		return nil
	}

	if err := v.ensureLayout(); err != nil {
		return err
	}
	for _, ref := range assigned {
		if _, done := v.receipted[ref.SegmentID]; done {
			continue
		}
		// Already held locally but the holder receipt is not yet recorded:
		// the origin self-assigns segments it produced (distribution promoted
		// them straight to head/ via LocalHolder), so a home that is also the
		// origin must still grow the holder set. Record the receipt without
		// pulling.
		if _, ok := v.layout.head[ref.SegmentID]; ok {
			if err := v.commitReceipt(ctx, ref); err != nil {
				return err
			}
			continue
		}
		// Received but not yet promoted (a prior or concurrent pass owns it):
		// skip until it reaches head, then the next pass records the receipt.
		if _, ok := v.layout.preHead[ref.SegmentID]; ok {
			continue
		}
		if err := v.collectOne(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}

// Config configures a CollectionManager.
type Config struct{}

// Manager pulls assigned segments into pre-head, verifies, and promotes to head.
// Collection passes are driven by vault-ctl FSM SetOnPublishCompletedSegment
// (when FSM is wired), explicit Notify calls, and CollectOnce nudges — not by
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
	return &Manager{
		cfg:    cfg,
		vaults: make(map[glid.GLID]*vaultCollect),
	}
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

	if cfg.FSM != nil {
		vid := vaultID
		v.unsubPublish = cfg.FSM.AddOnPublishCompletedSegment(func(vaultctlfsm.CompletedSegmentEntry) {
			m.triggerCollect(vid)
		})
	}
	return nil
}

// UnregisterVault removes a vault from collection.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	delete(m.vaults, vaultID)
	m.mu.Unlock()
	if ok && v.unsubPublish != nil {
		v.unsubPublish()
	}
}

// Notify triggers one collect pass for a vault when Run is active. Use when
// assignment or publish signals are not wired through the vault-ctl FSM (e.g.
// test harnesses).
func (m *Manager) Notify(vaultID glid.GLID) {
	m.triggerCollect(vaultID)
}

// CollectOnce rolls the log and collects missing segments for one vault (for tests
// and ChunkingManager nudges).
func (m *Manager) CollectOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.collectMissing(ctx)
}

// Run blocks until ctx is cancelled. On start it catches up any assignments
// already visible to the log reader (e.g. after Raft replay).
func (m *Manager) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	vaults := make([]*vaultCollect, 0, len(m.vaults))
	for _, v := range m.vaults {
		vaults = append(vaults, v)
	}
	m.mu.Unlock()

	for _, v := range vaults {
		_ = v.collectMissing(ctx)
	}

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
	ctx := m.runCtx
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok || ctx == nil {
		return
	}
	_ = v.collectMissing(ctx)
}
