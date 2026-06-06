package collection

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/glid"
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
}

type vaultCollect struct {
	vaultID  glid.GLID
	root     string
	log      LogReader
	pull     PullClient
	receipts ReceiptCommitter

	// layout caches head/pre-head segment IDs to avoid rescanning directories
	// on every poll once warmed.
	layout struct {
		loaded  bool
		head    map[glid.GLID]struct{}
		preHead map[glid.GLID]struct{}
	}
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
		vaultID:  vaultID,
		root:     root,
		log:      cfg.Log,
		pull:     cfg.Pull,
		receipts: cfg.Receipts,
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
	return v.receipts.CommitHolderReceipt(ctx, ref.VaultID, ref.SegmentID)
}

func (v *vaultCollect) collectMissing(ctx context.Context) error {
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
	for _, ref := range missingSegments(assigned, v.layout.head, v.layout.preHead) {
		if err := v.collectOne(ctx, ref); err != nil {
			return err
		}
	}
	return nil
}

// Config configures a CollectionManager.
type Config struct {
	// PollInterval is how often each vault log is rolled. Defaults to 50ms.
	PollInterval time.Duration
}

func (c Config) pollInterval() time.Duration {
	if c.PollInterval <= 0 {
		return 50 * time.Millisecond
	}
	return c.PollInterval
}

// Manager pulls assigned segments into pre-head, verifies, and promotes to head.
type Manager struct {
	cfg Config

	mu     sync.Mutex
	vaults map[glid.GLID]*vaultCollect
	runCtx context.Context

	pollScratch []*vaultCollect

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
	return nil
}

// UnregisterVault removes a vault from collection.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.mu.Lock()
	delete(m.vaults, vaultID)
	m.mu.Unlock()
}

// CollectOnce rolls the log and collects missing segments for one vault (for tests).
func (m *Manager) CollectOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.collectMissing(ctx)
}

// Run rolls vault logs and collects missing segments until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	m.mu.Unlock()

	ticker := time.NewTicker(m.cfg.pollInterval())
	defer ticker.Stop()

	m.wg.Go(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.poll(ctx)
			}
		}
	})

	m.wg.Wait()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()
	return ctx.Err()
}

func (m *Manager) poll(ctx context.Context) {
	m.mu.Lock()
	m.pollScratch = m.pollScratch[:0]
	for _, v := range m.vaults {
		m.pollScratch = append(m.pollScratch, v)
	}
	vaults := m.pollScratch
	m.mu.Unlock()

	for _, v := range vaults {
		_ = v.collectMissing(ctx)
	}
}
