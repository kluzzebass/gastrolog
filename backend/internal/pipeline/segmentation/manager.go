package segmentation

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
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

// Config configures a SegmentationManager. The commit/fsync fields are the
// node-global defaults; each vault may override them via VaultConfig at
// RegisterVault time.
type Config struct {
	ClosePolicy ClosePolicy
	// SyncBatchSize is the max appended frames between fsync calls for
	// fire-and-forget (no-ack) records. Defaults to 8192. This is a memory
	// bound (~2.5MB of frame bodies at typical record sizes), not the
	// durability bound: SyncBatchWindow (2ms) fires first at realistic
	// rates, so batches size themselves to the ingest rate. The cap only
	// binds when fsync latency dominates the commit cycle — on volumes
	// where F_FULLFSYNC costs tens to hundreds of ms, sustained throughput
	// is SyncBatchSize divided by that latency, so the cap must exceed the
	// records arriving during one flush (gastrolog-1ojsm6/oin19g: measured
	// ~200ms per F_FULLFSYNC on an external volume → 1024 capped the
	// pipeline at ~22K rec/s while the node sat idle).
	SyncBatchSize int
	// SyncBatchWindow is the max wait before fsyncing a partial fire-and-forget
	// group. Defaults to 2ms.
	SyncBatchWindow time.Duration
	// MaxCommitDelay coalesces ack-bearing records within this window before the
	// group-commit fsync. Zero (default) means pure group commit: an ack record
	// triggers a sync as soon as the currently-available burst is drained.
	MaxCommitDelay time.Duration
	// DisableFsync turns off fsync entirely (durability falls back to the OS page
	// cache); ack-bearing records ack after the in-memory append. Off by default.
	DisableFsync bool
	// EncodeQueueCap is the bounded channel between encode and append stages.
	// Defaults to 8192 so producers keep filling the next batch while the
	// current commit's fsync is in flight (gastrolog-1ojsm6).
	EncodeQueueCap int
	// CompletedCap is the bounded completed-segment notification queue. Defaults to 512.
	CompletedCap int
	// OnCompletedDropped fires when a completed segment cannot be enqueued because
	// the notification channel is full. Distribution should rescan completed/ immediately.
	OnCompletedDropped func()
	// Now returns the current time (for tests). Defaults to time.Now().UTC.
	Now func() time.Time
	// OnSync is invoked after each real fsync (for tests). It is NOT called for
	// vaults with DisableFsync set, since no fsync occurs.
	OnSync func()
	// Logger receives structured segmentation events (working-segment crash
	// recovery, degraded writers, dropped records). Nil discards.
	Logger *slog.Logger
	// Alerts raises operator alerts for degraded writers (commit failures,
	// abandoned working segments). Nil disables alerts.
	Alerts AlertSink

	// newSegmentFile overrides working-segment creation for fault-injection
	// tests. Nil uses segment.Create.
	newSegmentFile func(path string, meta segment.Meta) (segmentFile, error)
}

// AlertSink is the subset of alert.Collector segmentation raises
// degraded-writer alerts through (satisfied by the orchestrator's collector).
type AlertSink interface {
	Set(id string, severity alert.Severity, source, message string)
	Clear(id string)
}

// VaultConfig overrides per-vault commit/fsync tuning at RegisterVault time.
// Zero numeric fields inherit the manager-global Config defaults. DisableFsync
// is opt-in per vault (it also takes effect if set globally on Config).
type VaultConfig struct {
	SyncBatchSize   int
	SyncBatchWindow time.Duration
	MaxCommitDelay  time.Duration
	DisableFsync    bool
}

// Input is one record entering a vault's segmentation queue, with an optional
// durability ack. When Ack is non-nil it receives nil after the record's
// group-commit fsync (or after the in-memory append on a DisableFsync vault),
// or an error if the durable write fails.
type Input struct {
	Record *record.Record
	Ack    chan<- error
}

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now().UTC()
}

func (c Config) syncBatchSize() int {
	if c.SyncBatchSize <= 0 {
		return 8192
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

	// dropped counts records lost without an ack to carry the error (encode
	// failures and fire-and-forget records rejected while a writer is
	// degraded). Post-accept loss must be visible, not silent.
	dropped atomic.Uint64

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
		completedCap = 512
	}
	completed := make(chan CompletedSegment, completedCap)
	return &Manager{
		cfg:       cfg,
		completed: completed,
		writers:   make(map[glid.GLID]*vaultWriter),
	}, completed
}

// AppendStats is one vault's cumulative segmentation throughput counters.
// RecordsDurable lags RecordsAppended by the in-flight commit batch; the gap
// plus queue depth is the backpressure picture. Rates are computed downstream
// by the stats collector's rolling windows (gastrolog-4eh5ns).
type AppendStats struct {
	VaultID         glid.GLID
	RecordsAppended uint64
	BytesAppended   uint64
	RecordsDurable  uint64
	QueueDepth      int
	QueueCap        int
}

// AppendStats returns cumulative throughput counters for every registered
// vault writer.
func (m *Manager) AppendStats() []AppendStats {
	m.mu.Lock()
	writers := make(map[glid.GLID]*vaultWriter, len(m.writers))
	maps.Copy(writers, m.writers)
	m.mu.Unlock()
	out := make([]AppendStats, 0, len(writers))
	for vaultID, w := range writers {
		out = append(out, AppendStats{
			VaultID:         vaultID,
			RecordsAppended: w.recordsAppended.Load(),
			BytesAppended:   w.bytesAppended.Load(),
			RecordsDurable:  w.recordsDurable.Load(),
			QueueDepth:      len(w.in),
			QueueCap:        cap(w.in),
		})
	}
	slices.SortFunc(out, func(a, b AppendStats) int { return a.VaultID.Compare(b.VaultID) })
	return out
}

// DroppedRecords reports how many records this manager dropped without an ack
// to carry the error: encode failures and fire-and-forget records rejected
// while a writer was degraded.
func (m *Manager) DroppedRecords() uint64 {
	return m.dropped.Load()
}

// RegisterVault starts a per-vault writer under root (creates working/ and completed/).
// Safe to call before or during Run — the orchestrator can register vaults as placement
// changes. vc overrides the manager-global commit/fsync defaults for this vault.
// Returns the bounded input channel routing should send records to.
func (m *Manager) RegisterVault(vaultID glid.GLID, root string, vc VaultConfig) (chan<- Input, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running.Load() && m.runCtx == nil {
		return nil, ErrNotRunning
	}
	if _, ok := m.writers[vaultID]; ok {
		return nil, errors.New("vault already registered")
	}
	w, err := newVaultWriter(vaultID, root, m.cfg, vc, m.completed, &m.dropped)
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

// Submit enqueues a record directly into a registered vault's segmentation
// queue, bypassing the routing table. It is the direct-to-vault entry used by
// writers that target a specific vault and preserve the record's EventID
// (ImportRecords, export-to-vault). The send blocks on the bounded encode queue
// and is cancellable via ctx; when in.Ack is non-nil it resolves after the
// vault's group-commit fsync (or with an error). Returns ErrUnknownVault when
// the vault has no local writer on this node.
func (m *Manager) Submit(ctx context.Context, vaultID glid.GLID, in Input) error {
	m.mu.Lock()
	w, ok := m.writers[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	select {
	case w.input() <- in:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
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
