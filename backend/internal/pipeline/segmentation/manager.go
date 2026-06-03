package segmentation

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("segmentation manager not running")

// ErrUnknownVault is returned when a vault was never registered.
var ErrUnknownVault = errors.New("unknown vault")

// ClosePolicy configures when a working segment is closed and renamed.
type ClosePolicy struct {
	// MaxBytes closes the segment once the on-disk file reaches this size.
	MaxBytes uint64
	// MaxAge closes the segment once this long has elapsed since it was opened.
	MaxAge time.Duration
}

// Config configures a SegmentationManager.
type Config struct {
	ClosePolicy ClosePolicy
	// SyncBatchSize is the max appended frames between fsync calls. Defaults to 16.
	SyncBatchSize int
	// SyncBatchWindow is the max wait before fsyncing a partial group. Defaults to 2ms.
	SyncBatchWindow time.Duration
	// EncodeQueueCap is the bounded channel between encode and append stages. Defaults to 64.
	EncodeQueueCap int
	// CompletedCap is the bounded completed-segment notification queue. Defaults to 16.
	CompletedCap int
	// Now returns the current time (for tests). Defaults to time.Now().UTC.
	Now func() time.Time
	// OnSync is invoked after each group fsync (for tests).
	OnSync func()
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

func (c Config) syncBatchSize() int {
	if c.SyncBatchSize <= 0 {
		return 16
	}
	return c.SyncBatchSize
}

func (c Config) syncBatchWindow() time.Duration {
	if c.SyncBatchWindow <= 0 {
		return 2 * time.Millisecond
	}
	return c.SyncBatchWindow
}

// CompletedSegment is emitted when a segment is renamed from working/ to completed/.
type CompletedSegment struct {
	VaultID glid.GLID
	Meta    segment.Meta
	Path    string
	Header  segment.Header
}

// Manager runs one pipelined segment writer per registered vault.
type Manager struct {
	cfg       Config
	completed chan CompletedSegment

	mu      sync.Mutex
	writers map[glid.GLID]*vaultWriter
	runCtx  context.Context // non-nil while Run is active

	running atomic.Bool
	wg      sync.WaitGroup
}

// New returns a manager and a read-only channel of completed segments.
func New(cfg Config) (*Manager, <-chan CompletedSegment) {
	completedCap := cfg.CompletedCap
	if completedCap <= 0 {
		completedCap = 16
	}
	completed := make(chan CompletedSegment, completedCap)
	return &Manager{
		cfg:       cfg,
		completed: completed,
		writers:   make(map[glid.GLID]*vaultWriter),
	}, completed
}

// RegisterVault starts a per-vault writer under root (creates working/ and completed/).
// Safe to call before or during Run — the orchestrator can register vaults as placement
// changes. Returns the bounded input channel routing should send records to.
func (m *Manager) RegisterVault(vaultID glid.GLID, root string) (chan<- *record.Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running.Load() && m.runCtx == nil {
		return nil, ErrNotRunning
	}
	if _, ok := m.writers[vaultID]; ok {
		return nil, errors.New("vault already registered")
	}
	w, err := newVaultWriter(vaultID, root, m.cfg, m.completed)
	if err != nil {
		return nil, err
	}
	m.writers[vaultID] = w
	if m.runCtx != nil {
		ctx := m.runCtx
		m.wg.Go(func() {
			w.run(ctx)
		})
	}
	return w.input(), nil
}

// UnregisterVault stops a vault writer and closes its input channel.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.mu.Lock()
	w, ok := m.writers[vaultID]
	if ok {
		delete(m.writers, vaultID)
	}
	m.mu.Unlock()
	if ok {
		w.stop()
	}
}

// Run starts all registered vault writers until ctx is cancelled.
func (m *Manager) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	writers := make([]*vaultWriter, 0, len(m.writers))
	for _, w := range m.writers {
		writers = append(writers, w)
	}
	m.mu.Unlock()

	m.wg.Go(func() {
		<-ctx.Done()
	})
	for _, w := range writers {
		m.wg.Go(func() {
			w.run(ctx)
		})
	}

	m.wg.Wait()

	m.mu.Lock()
	m.runCtx = nil
	for _, w := range m.writers {
		w.flushAndCloseSegment()
	}
	m.mu.Unlock()

	close(m.completed)
	return ctx.Err()
}
