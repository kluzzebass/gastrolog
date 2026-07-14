package distribution

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/notify"
	"gastrolog/internal/pipeline/segmentation"
)

// defaultPublishQueueCap bounds staged publishes waiting for the vault-ctl
// worker. Ingress only enqueues; a dedicated worker issues Raft applies so
// pull serving never blocks behind a publish backlog.
const defaultPublishQueueCap = 512

const (
	defaultPublishWorkers   = 4
	defaultPublishBatchSize = 32
)

// publishRetryBaseDelay/publishRetryMaxDelay bound the failed-publish retry
// backoff: quick first retry for transient races (vault-ctl leadership
// settling), 2s steady-state while applies keep failing. Without this the
// retry path was a hot loop — enqueue notified the retry signal, the drain
// re-ran immediately, failed again, and re-notified (gastrolog-353kwm).
const (
	publishRetryBaseDelay = 50 * time.Millisecond
	publishRetryMaxDelay  = 2 * time.Second
)

// ErrAlreadyRunning is returned when Run is called twice.
var ErrAlreadyRunning = errors.New("distribution manager already running")

// ErrUnknownVault is returned for an unregistered vault.
var ErrUnknownVault = errors.New("unknown vault")

// Config configures a DistributionManager.
type Config struct {
	// PullQueueCap bounds incoming pull requests. Defaults to 16.
	PullQueueCap int
	// PublishQueueCap bounds staged publishes waiting for a vault-ctl publish
	// worker. Defaults to 512.
	PublishQueueCap int
	// PublishWorkers is the number of vault-ctl publish workers. Defaults to 4.
	// Workers on different vaults apply in parallel; batches for the same vault
	// are serialized by the per-vault publisher path.
	PublishWorkers int
	// PublishBatchSize is how many staged segments one worker coalesces into a
	// single vault-ctl apply when the publisher supports batching. Defaults to 32.
	PublishBatchSize int
	Logger           *slog.Logger
}

func (c Config) publishQueueCap() int {
	if c.PublishQueueCap <= 0 {
		return defaultPublishQueueCap
	}
	return c.PublishQueueCap
}

func (c Config) publishWorkers() int {
	if c.PublishWorkers <= 0 {
		return defaultPublishWorkers
	}
	return c.PublishWorkers
}

func (c Config) publishBatchSize() int {
	if c.PublishBatchSize <= 0 {
		return defaultPublishBatchSize
	}
	return c.PublishBatchSize
}

// Manager publishes completed segment metadata and serves segment pulls.
type Manager struct {
	cfg Config

	mu           sync.Mutex
	vaults       map[glid.GLID]*vaultDist
	pullIn       chan PullRequest
	stranded     *notify.Signal
	publishRetry *notify.Signal
	retryMu      sync.Mutex
	retryPending []pendingPublish
	retryDelay   time.Duration
	retryTimer   *time.Timer
	runCtx       context.Context
	running      atomic.Bool
	wg           sync.WaitGroup
}

// New returns a manager. Pull requests are sent to the returned channel.
func New(cfg Config) (*Manager, chan<- PullRequest) {
	queueCap := cfg.PullQueueCap
	if queueCap <= 0 {
		queueCap = 16
	}
	pullIn := make(chan PullRequest, queueCap)
	return &Manager{
		cfg:          cfg,
		vaults:       make(map[glid.GLID]*vaultDist),
		pullIn:       pullIn,
		stranded:     notify.NewSignal(),
		publishRetry: notify.NewSignal(),
	}, pullIn
}

// NotifyPublishRetry wakes the publish worker to drain staged retries. Call
// after vault-ctl leadership changes or other events that unblock applies.
func (m *Manager) NotifyPublishRetry() {
	if m.publishRetry != nil {
		m.publishRetry.Notify()
	}
}

// NotifyStranded wakes publish ingress to scan completed/ for segments whose
// channel notification was dropped under burst load.
func (m *Manager) NotifyStranded() {
	if m.stranded != nil {
		m.stranded.Notify()
	}
}

func (m *Manager) logger() *slog.Logger {
	if m.cfg.Logger != nil {
		return m.cfg.Logger
	}
	return slog.Default()
}

// RegisterVault adds a vault distribution path. Safe before or during Run.
func (m *Manager) RegisterVault(vaultID glid.GLID, root string, cfg VaultConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.vaults[vaultID]; ok {
		return errors.New("vault already registered")
	}
	v, err := newVaultDist(root, cfg, m.logger())
	if err != nil {
		return err
	}
	m.vaults[vaultID] = v
	return nil
}

// UnregisterVault removes a vault from pull serving.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.mu.Lock()
	delete(m.vaults, vaultID)
	m.mu.Unlock()
}

// RetireSegments marks segments released from the vault-ctl registry so rescan
// does not republish completed/ files that still exist on disk. Also drops
// in-memory pull paths.
func (m *Manager) RetireSegments(vaultID glid.GLID, segmentIDs []glid.GLID) {
	if len(segmentIDs) == 0 {
		return
	}
	m.mu.Lock()
	v := m.vaults[vaultID]
	m.mu.Unlock()
	if v == nil {
		return
	}
	v.mu.Lock()
	for _, id := range segmentIDs {
		delete(v.segments, id)
		v.retired[id] = struct{}{}
	}
	v.mu.Unlock()
}

// Run consumes completed segments and pull requests until ctx is cancelled.
func (m *Manager) Run(ctx context.Context, completed <-chan segmentation.CompletedSegment) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	m.mu.Unlock()

	publishQ := make(chan pendingPublish, m.cfg.publishQueueCap())

	m.wg.Go(func() {
		m.runPullLoop(ctx)
	})
	m.wg.Go(func() {
		m.runPublishWorkers(ctx, publishQ)
	})
	m.wg.Go(func() {
		m.runPublishIngress(ctx, completed, publishQ)
	})

	// Quiesce before waiting (see chunking/collection: Wait must not race
	// a registration's Add).
	<-ctx.Done()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()

	m.wg.Wait()
	m.stopRetryWake()
	return ctx.Err()
}

type pendingPublish struct {
	vaultID glid.GLID
	segID   glid.GLID
	path    string
	meta    Metadata
}

// runPullLoop serves segment pulls on a dedicated goroutine so vault-ctl
// publish applies cannot wedge peer collection.
func (m *Manager) runPullLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-m.pullIn:
			if !ok {
				return
			}
			m.onPull(req)
		}
	}
}

// runPublishWorkers commit staged segments to vault-ctl. Failed publishes are
// queued for retry and drained on publishRetry wake.
func (m *Manager) runPublishWorkers(ctx context.Context, publishQ chan pendingPublish) {
	workers := m.cfg.publishWorkers()
	for range workers {
		m.wg.Go(func() {
			m.publishWorkerLoop(ctx, publishQ)
		})
	}
}

func (m *Manager) publishWorkerLoop(ctx context.Context, publishQ chan pendingPublish) {
	retryCh := m.publishRetry.C()
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-publishQ:
			if !ok {
				return
			}
			m.publishBurst(ctx, publishQ, p)
		case <-retryCh:
			retryCh = m.publishRetry.C()
			m.drainPublishRetries(ctx, publishQ)
		}
	}
}

func (m *Manager) publishBurst(ctx context.Context, publishQ chan pendingPublish, first pendingPublish) {
	m.publishGroups(ctx, groupPendingByVault(m.coalesceBatch(publishQ, first)))
}

// publishGroups commits per-vault batches, queuing retryable failures behind a
// backoff wake and logging every failure — a persistent vault-ctl apply error
// previously looped forever with no log line and no delay.
func (m *Manager) publishGroups(ctx context.Context, groups map[glid.GLID][]pendingPublish) {
	for vaultID, items := range groups {
		err := m.publishVaultBatch(ctx, vaultID, items)
		if err == nil {
			m.resetRetryBackoff()
			continue
		}
		if publishRetryable(err) {
			m.logger().Warn("vault-ctl publish failed; queuing retry",
				"vault", vaultID, "segments", len(items), "error", err)
			for _, p := range items {
				m.queuePublishRetry(p)
			}
			m.scheduleRetryWake()
			continue
		}
		m.logger().Warn("publish failed terminally; dropping batch",
			"vault", vaultID, "segments", len(items), "error", err)
	}
}

func (m *Manager) coalesceBatch(publishQ chan pendingPublish, first pendingPublish) []pendingPublish {
	maxBatch := m.cfg.publishBatchSize()
	batch := make([]pendingPublish, 1, maxBatch)
	batch[0] = first
	for len(batch) < maxBatch {
		select {
		case p := <-publishQ:
			batch = append(batch, p)
		default:
			return batch
		}
	}
	return batch
}

func groupPendingByVault(batch []pendingPublish) map[glid.GLID][]pendingPublish {
	out := make(map[glid.GLID][]pendingPublish)
	for _, p := range batch {
		out[p.vaultID] = append(out[p.vaultID], p)
	}
	return out
}

func publishRetryable(err error) bool {
	return err != nil &&
		!errors.Is(err, ErrUnknownVault) &&
		!errors.Is(err, errPublishBytesMissing)
}

func (m *Manager) queuePublishRetry(p pendingPublish) {
	m.retryMu.Lock()
	defer m.retryMu.Unlock()
	for _, existing := range m.retryPending {
		if existing.segID == p.segID {
			return
		}
	}
	m.retryPending = append(m.retryPending, p)
}

// scheduleRetryWake arms a one-shot backoff wake for the queued retries. Not a
// poll: the timer exists only while failed publishes are outstanding. External
// NotifyPublishRetry calls (vault-ctl leadership changes) bypass the backoff
// deliberately.
func (m *Manager) scheduleRetryWake() {
	m.retryMu.Lock()
	defer m.retryMu.Unlock()
	switch {
	case m.retryDelay == 0:
		m.retryDelay = publishRetryBaseDelay
	case m.retryDelay < publishRetryMaxDelay:
		m.retryDelay *= 2
	}
	if m.retryTimer != nil {
		m.retryTimer.Stop()
	}
	m.retryTimer = time.AfterFunc(m.retryDelay, m.NotifyPublishRetry)
}

// resetRetryBackoff clears the backoff after a successful publish.
func (m *Manager) resetRetryBackoff() {
	m.retryMu.Lock()
	defer m.retryMu.Unlock()
	m.retryDelay = 0
}

// stopRetryWake cancels any pending retry wake (manager shutdown).
func (m *Manager) stopRetryWake() {
	m.retryMu.Lock()
	defer m.retryMu.Unlock()
	if m.retryTimer != nil {
		m.retryTimer.Stop()
		m.retryTimer = nil
	}
}

func (m *Manager) drainPublishRetries(ctx context.Context, publishQ chan pendingPublish) {
	m.retryMu.Lock()
	pending := m.retryPending
	m.retryPending = nil
	m.retryMu.Unlock()
	if len(pending) == 0 {
		return
	}
	m.publishGroups(ctx, groupPendingByVault(pending))
	for {
		select {
		case p := <-publishQ:
			m.publishBurst(ctx, publishQ, p)
		default:
			return
		}
	}
}

// runPublishIngress stages completed segments and enqueues vault-ctl publishes.
// Stranded rescans run only on explicit wake (segment channel drop or startup).
func (m *Manager) runPublishIngress(ctx context.Context, completed <-chan segmentation.CompletedSegment, publishQ chan<- pendingPublish) {
	strandedCh := m.stranded.C()
	m.rescanStranded(ctx, publishQ)
	for {
		select {
		case <-ctx.Done():
			return
		case seg, ok := <-completed:
			if !ok {
				return
			}
			// A stranded rescan may already have staged this segment before
			// its channel notification was consumed; only the first staging
			// enqueues, or the segment publishes twice (gastrolog-x5c8ge).
			p, alreadyStaged, err := m.stageForPublish(seg)
			switch {
			case err != nil:
				m.logger().Warn("staging completed segment for publish failed",
					"vault", seg.VaultID, "segment", seg.Meta.ID, "error", err)
			case !alreadyStaged:
				m.enqueuePublish(ctx, publishQ, p)
			}
		case <-strandedCh:
			strandedCh = m.stranded.C()
			m.rescanStranded(ctx, publishQ)
		}
	}
}

func (m *Manager) rescanStranded(ctx context.Context, publishQ chan<- pendingPublish) {
	for vaultID, v := range m.vaultsSnapshot() {
		// Aggregate failures per pass: a rescan racing the release purge can
		// fail the stat for every listed segment (files legitimately deleted
		// between ReadDir and stage), and per-segment warns flooded hundreds
		// of identical lines in seconds (gastrolog-4elpu1). One summary per
		// vault per pass carries the same signal.
		var failed int
		var firstErr error
		for _, seg := range v.stranded(vaultID) {
			p, alreadyStaged, err := m.stageForPublish(seg)
			switch {
			case err != nil:
				failed++
				if firstErr == nil {
					firstErr = err
				}
			case !alreadyStaged:
				m.enqueuePublish(ctx, publishQ, p)
			}
		}
		if failed > 0 {
			m.logger().Warn("stranded rescan: staging failed for some segments",
				"vault", vaultID, "failed", failed, "first_error", firstErr)
		}
	}
}

func (m *Manager) enqueuePublish(ctx context.Context, publishQ chan<- pendingPublish, p pendingPublish) {
	select {
	case <-ctx.Done():
	case publishQ <- p:
	}
}

// stageForPublish prepares a segment and reports whether an earlier staging
// already owns its publish — see vaultDist.prepare (gastrolog-x5c8ge).
func (m *Manager) stageForPublish(seg segmentation.CompletedSegment) (pendingPublish, bool, error) {
	m.mu.Lock()
	v, ok := m.vaults[seg.VaultID]
	m.mu.Unlock()
	if !ok {
		return pendingPublish{}, false, ErrUnknownVault
	}
	meta, path, alreadyStaged, err := v.prepare(seg)
	if err != nil {
		return pendingPublish{}, false, err
	}
	return pendingPublish{
		vaultID: seg.VaultID,
		segID:   seg.Meta.ID,
		path:    path,
		meta:    meta,
	}, alreadyStaged, nil
}

// vaultsSnapshot copies the vault map for iteration outside m.mu.
func (m *Manager) vaultsSnapshot() map[glid.GLID]*vaultDist {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[glid.GLID]*vaultDist, len(m.vaults))
	maps.Copy(out, m.vaults)
	return out
}

// publishVaultBatch commits one coalesced batch for a vault and promotes local
// holders after vault-ctl accepts the registry entries.
func (m *Manager) publishVaultBatch(ctx context.Context, vaultID glid.GLID, items []pendingPublish) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.publishStagedBatch(ctx, items)
}

func (m *Manager) onPull(req PullRequest) {
	m.mu.Lock()
	v, ok := m.vaults[req.VaultID]
	m.mu.Unlock()
	if !ok {
		return
	}
	_ = v.servePull(req)
}

// ServePull handles one pull synchronously (for tests and direct callers).
func (m *Manager) ServePull(req PullRequest) error {
	m.mu.Lock()
	v, ok := m.vaults[req.VaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.servePull(req)
}

// PublishCompleted handles one completed segment synchronously (for tests).
func (m *Manager) PublishCompleted(ctx context.Context, seg segmentation.CompletedSegment) error {
	m.mu.Lock()
	v, ok := m.vaults[seg.VaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.publish(ctx, seg)
}
