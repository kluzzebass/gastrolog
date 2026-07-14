package distribution

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

	"gastrolog/internal/glid"
	"gastrolog/internal/notify"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
)

// publishQueueCap bounds staged publishes waiting for the vault-ctl worker.
// Ingress only enqueues; a dedicated worker issues Raft applies so pull serving
// never blocks behind a publish backlog.
const publishQueueCap = 512

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

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("distribution manager not running")

// ErrUnknownVault is returned for an unregistered vault.
var ErrUnknownVault = errors.New("unknown vault")

// errPublishBytesMissing is returned when a queued publish runs after local
// segment bytes were purged. The worker must not retry — a stale queue item
// or publish retry after ReleaseSegments would otherwise re-commit metadata
// to vault-ctl without any on-disk copy (permanent collection wedge).
var errPublishBytesMissing = errors.New("segment bytes missing for publish")

// VaultConfig is per-vault distribution state.
type VaultConfig struct {
	// Publisher commits completed-segment metadata to vault-ctl.
	Publisher Publisher
	// LocalHolder reports whether this node holds the vault locally (completed→head rename).
	LocalHolder func() bool
	// OnLocalHeadPromoted fires after a locally-held segment lands in head/.
	// Collection uses this to commit holder receipts without waiting for the
	// next publish wake — publish applies before finalizeAfterPublish promotes.
	OnLocalHeadPromoted func(segmentID glid.GLID)
	// OnPublishCommitted fires after vault-ctl accepts segment metadata and
	// the segment is registered for pull. Wired to wake collection on the
	// same node when origin and home overlap.
	OnPublishCommitted func(segmentID glid.GLID)
}

type vaultDist struct {
	root                string
	log                 *slog.Logger
	publisher           Publisher
	localHolder         func() bool
	onLocalHeadPromoted func(glid.GLID)
	onPublishCommitted  func(glid.GLID)
	mu                  sync.RWMutex
	segments            map[glid.GLID]string   // segment ID → on-disk path
	retired             map[glid.GLID]struct{} // released from vault-ctl; skip rescan republish
	// badHeader remembers completed/ files whose fixed header failed to
	// decode, keyed by segment ID (state, not time): each corrupt file is
	// read and warned about exactly once, not on every rescan wake
	// (gastrolog-faj2yv).
	badHeader map[glid.GLID]struct{}
}

func newVaultDist(root string, cfg VaultConfig, log *slog.Logger) (*vaultDist, error) {
	if cfg.Publisher == nil {
		return nil, errors.New("publisher required")
	}
	if cfg.LocalHolder == nil {
		cfg.LocalHolder = func() bool { return false }
	}
	if log == nil {
		log = slog.Default()
	}
	return &vaultDist{
		root:                root,
		log:                 log,
		publisher:           cfg.Publisher,
		localHolder:         cfg.LocalHolder,
		onLocalHeadPromoted: cfg.OnLocalHeadPromoted,
		onPublishCommitted:  cfg.OnPublishCommitted,
		segments:            make(map[glid.GLID]string),
		retired:             make(map[glid.GLID]struct{}),
		badHeader:           make(map[glid.GLID]struct{}),
	}, nil
}

// prepare stages a completed segment for vault-ctl publish: builds metadata and
// registers the on-disk path for pull serving. Local holders keep the file in
// completed/ until publish succeeds (see finalizeAfterPublish).
//
// alreadyStaged reports that a prior prepare registered this segment — the
// stranded rescan and the completed-channel delivery can race on the same
// segment (the file exists in completed/ before its notification is consumed),
// and only the first staging may enqueue the publish (gastrolog-x5c8ge).
func (v *vaultDist) prepare(seg segmentation.CompletedSegment) (meta Metadata, path string, alreadyStaged bool, err error) {
	v.mu.RLock()
	registered, known := v.segments[seg.Meta.ID]
	v.mu.RUnlock()
	if known {
		seg.Path = registered
		meta, err := metadataForPublish(seg)
		return meta, registered, true, err
	}

	path = seg.Path
	meta, err = metadataForPublish(seg)
	if err != nil {
		return Metadata{}, "", false, err
	}

	v.mu.Lock()
	v.segments[seg.Meta.ID] = path
	v.mu.Unlock()
	return meta, path, false, nil
}

// finalizeAfterPublish moves a locally-held segment into head/ after vault-ctl
// has committed the registry entry.
func (v *vaultDist) finalizeAfterPublish(segID glid.GLID, path string) error {
	if !v.localHolder() {
		return nil
	}
	dest, err := PromoteToHead(path, v.root)
	if err != nil {
		return err
	}
	v.mu.Lock()
	v.segments[segID] = dest
	v.mu.Unlock()
	if v.onLocalHeadPromoted != nil {
		v.onLocalHeadPromoted(segID)
	}
	return nil
}

func metadataForPublish(seg segmentation.CompletedSegment) (Metadata, error) {
	hdr := seg.Header
	if seg.Path != "" && hdr.IsUnpopulated() {
		// Header-only read: this node finalized and fsynced the file, so a
		// full segment.Open here re-verified every record byte just to fetch
		// counts; the checksum travels in the header for downstream
		// verification instead (gastrolog-faj2yv).
		h, err := segment.ReadHeader(seg.Path)
		if err != nil {
			return Metadata{}, err
		}
		hdr = h
	}
	return metadataFromPath(seg.Path, seg.VaultID, seg.Meta.ID, hdr)
}

// stranded returns completed segments on disk that this manager has not
// prepared yet — segments whose channel notification was dropped (burst) or
// that predate this process (restart). Cost is one directory listing plus one
// fixed-header read per unknown segment: a restart backlog must not re-verify
// every byte of every completed file before the first publish
// (gastrolog-faj2yv).
func (v *vaultDist) stranded(vaultID glid.GLID) []segmentation.CompletedSegment {
	ids, err := paths.ListSegmentIDs(paths.CompletedDir(v.root))
	if err != nil {
		v.log.Warn("stranded rescan: reading completed/ failed", "vault", vaultID, "error", err)
		return nil
	}
	// Publish in segment-ID order (GLIDs are time-ordered), matching the old
	// name-sorted directory walk.
	sorted := slices.SortedFunc(maps.Keys(ids), glid.GLID.Compare)
	var out []segmentation.CompletedSegment
	for _, segID := range sorted {
		v.mu.RLock()
		_, known := v.segments[segID]
		_, retired := v.retired[segID]
		_, badHeader := v.badHeader[segID]
		v.mu.RUnlock()
		if known || retired || badHeader {
			continue
		}
		path := paths.CompletedSegment(v.root, segID)
		hdr, err := segment.ReadHeader(path)
		if err != nil {
			if os.IsNotExist(err) {
				// Raced a release purge between the listing and the read; the
				// ID drops out of the next listing on its own.
				continue
			}
			v.mu.Lock()
			v.badHeader[segID] = struct{}{}
			v.mu.Unlock()
			v.log.Warn("stranded rescan: completed segment header unreadable; skipping",
				"vault", vaultID, "segment", segID, "path", path, "error", err)
			continue
		}
		out = append(out, segmentation.CompletedSegment{
			VaultID: vaultID,
			Meta:    segment.Meta{ID: segID, VaultID: vaultID},
			Path:    path,
			Header:  hdr,
		})
	}
	return out
}

func (v *vaultDist) publish(ctx context.Context, seg segmentation.CompletedSegment) error {
	// Synchronous path (PublishCompleted): publishes regardless of prior
	// staging; the FSM treats an identical re-publish as a no-op.
	meta, path, _, err := v.prepare(seg)
	if err != nil {
		return err
	}
	return v.publishStaged(ctx, meta, seg.Meta.ID, path)
}

func (v *vaultDist) publishStaged(ctx context.Context, meta Metadata, segID glid.GLID, path string) error {
	if v.isRetired(segID) {
		return nil
	}
	if !v.segmentBytesPresent(segID, path) {
		v.forgetSegment(segID)
		v.log.Warn("segment bytes missing at publish; forgetting segment",
			"segment", segID, "path", path)
		return errPublishBytesMissing
	}
	if err := v.publisher.Publish(ctx, meta); err != nil {
		return err
	}
	if v.isRetired(segID) {
		return nil
	}
	if err := v.finalizeAfterPublish(segID, path); err != nil {
		return err
	}
	if v.onPublishCommitted != nil {
		v.onPublishCommitted(segID)
	}
	return nil
}

func (v *vaultDist) publishStagedBatch(ctx context.Context, items []pendingPublish) error {
	live := make([]pendingPublish, 0, len(items))
	for _, p := range items {
		if v.isRetired(p.segID) {
			continue
		}
		if !v.segmentBytesPresent(p.segID, p.path) {
			// Forget THIS item only. Failing the whole coalesced batch here
			// stranded the surviving batchmates permanently: the batch error
			// was classified non-retryable, the items stayed in v.segments,
			// and the stranded rescan skipped them as known — durable
			// segments invisible to vault-ctl until restart (gastrolog-353kwm).
			v.forgetSegment(p.segID)
			v.log.Warn("segment bytes missing at publish; forgetting segment",
				"segment", p.segID, "path", p.path)
			continue
		}
		live = append(live, p)
	}
	if len(live) == 0 {
		return nil
	}
	if len(live) == 1 {
		p := live[0]
		return v.publishStaged(ctx, p.meta, p.segID, p.path)
	}
	metas := make([]Metadata, len(live))
	for i, p := range live {
		metas[i] = p.meta
	}
	var err error
	if bp, ok := v.publisher.(BatchPublisher); ok {
		err = bp.PublishBatch(ctx, metas)
	} else {
		for _, meta := range metas {
			if pubErr := v.publisher.Publish(ctx, meta); pubErr != nil {
				err = pubErr
				break
			}
		}
	}
	if err != nil {
		return err
	}
	for _, p := range live {
		if v.isRetired(p.segID) {
			continue
		}
		if err := v.finalizeAfterPublish(p.segID, p.path); err != nil {
			return err
		}
		if v.onPublishCommitted != nil {
			v.onPublishCommitted(p.segID)
		}
	}
	return nil
}

func (v *vaultDist) servePull(req PullRequest) error {
	path, ok := v.segmentPathForPull(req.SegmentID)
	if !ok {
		return ErrSegmentNotFound
	}
	return StreamSegment(path, req.Dest)
}

// segmentPathForPull resolves a segment for pull serving. The registered path
// can go stale during completed→head promotion; fall back to the layout dirs.
func (v *vaultDist) segmentPathForPull(segmentID glid.GLID) (string, bool) {
	v.mu.RLock()
	registered, known := v.segments[segmentID]
	v.mu.RUnlock()
	if known {
		if _, err := os.Stat(registered); err == nil {
			return registered, true
		}
	}
	// Probe order: head/, completed/, pre-head/.
	if path, ok := paths.FindSegment(v.root, segmentID,
		paths.AreaHead, paths.AreaCompleted, paths.AreaPreHead); ok {
		v.mu.Lock()
		v.segments[segmentID] = path
		v.mu.Unlock()
		return path, true
	}
	return "", false
}

func (v *vaultDist) isRetired(segID glid.GLID) bool {
	v.mu.RLock()
	_, ok := v.retired[segID]
	v.mu.RUnlock()
	return ok
}

// segmentBytesPresent reports whether this vault still holds the segment in
// staging (completed/, head/, or pre-head/). Publish must not commit vault-ctl
// metadata when bytes are gone — RetireSegments only guards the rescan path,
// not pending queue items or publish retries.
func (v *vaultDist) segmentBytesPresent(segID glid.GLID, path string) bool {
	if path != "" {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}
	// Probe order: head/, completed/, pre-head/.
	_, ok := paths.FindSegment(v.root, segID,
		paths.AreaHead, paths.AreaCompleted, paths.AreaPreHead)
	return ok
}

func (v *vaultDist) forgetSegment(segID glid.GLID) {
	v.mu.Lock()
	delete(v.segments, segID)
	v.retired[segID] = struct{}{}
	v.mu.Unlock()
}

// Config configures a DistributionManager.
type Config struct {
	// PullQueueCap bounds incoming pull requests. Defaults to 16.
	PullQueueCap int
	// PublishWorkers is the number of vault-ctl publish workers. Defaults to 4.
	// Workers on different vaults apply in parallel; batches for the same vault
	// are serialized by the per-vault publisher path.
	PublishWorkers int
	// PublishBatchSize is how many staged segments one worker coalesces into a
	// single vault-ctl apply when the publisher supports batching. Defaults to 32.
	PublishBatchSize int
	Logger           *slog.Logger
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
		return ErrNotRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	m.mu.Unlock()

	publishQ := make(chan pendingPublish, publishQueueCap)

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
