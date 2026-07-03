package distribution

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"sync"
	"sync/atomic"

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
	// Root is the vault storage root (contains segmentation completed/).
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
	publisher           Publisher
	localHolder         func() bool
	onLocalHeadPromoted func(glid.GLID)
	onPublishCommitted  func(glid.GLID)
	mu                  sync.RWMutex
	segments            map[glid.GLID]string   // segment ID → on-disk path
	retired             map[glid.GLID]struct{} // released from vault-ctl; skip rescan republish
}

func newVaultDist(root string, cfg VaultConfig) (*vaultDist, error) {
	if cfg.Publisher == nil {
		return nil, errors.New("publisher required")
	}
	if cfg.LocalHolder == nil {
		cfg.LocalHolder = func() bool { return false }
	}
	return &vaultDist{
		root:                root,
		publisher:           cfg.Publisher,
		localHolder:         cfg.LocalHolder,
		onLocalHeadPromoted: cfg.OnLocalHeadPromoted,
		onPublishCommitted:  cfg.OnPublishCommitted,
		segments:            make(map[glid.GLID]string),
		retired:             make(map[glid.GLID]struct{}),
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
	if seg.Path != "" && hdr.RecordCount == 0 && hdr.SegmentChecksum == 0 {
		sf, err := segment.Open(seg.Path)
		if err != nil {
			return Metadata{}, err
		}
		hdr = sf.Header()
		_ = sf.Close()
	}
	return metadataFromPath(seg.Path, seg.VaultID, seg.Meta.ID, hdr)
}

// stranded returns completed segments on disk that this manager has not
// prepared yet — segments whose channel notification was dropped (burst) or
// that predate this process (restart).
func (v *vaultDist) stranded(vaultID glid.GLID) []segmentation.CompletedSegment {
	entries, err := os.ReadDir(paths.CompletedDir(v.root))
	if err != nil {
		return nil
	}
	var out []segmentation.CompletedSegment
	for _, ent := range entries {
		if ent.IsDir() {
			continue
		}
		segID, err := glid.Parse(ent.Name())
		if err != nil {
			continue
		}
		v.mu.RLock()
		_, known := v.segments[segID]
		_, retired := v.retired[segID]
		v.mu.RUnlock()
		if known || retired {
			continue
		}
		path := paths.CompletedSegment(v.root, segID)
		sf, err := segment.Open(path)
		if err != nil {
			continue
		}
		hdr := sf.Header()
		_ = sf.Close()
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
			v.forgetSegment(p.segID)
			return errPublishBytesMissing
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
	for _, path := range []string{
		paths.HeadSegment(v.root, segmentID),
		paths.CompletedSegment(v.root, segmentID),
		paths.PreHeadSegment(v.root, segmentID),
	} {
		if _, err := os.Stat(path); err == nil {
			v.mu.Lock()
			v.segments[segmentID] = path
			v.mu.Unlock()
			return path, true
		}
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
	for _, p := range []string{
		paths.HeadSegment(v.root, segID),
		paths.CompletedSegment(v.root, segID),
		paths.PreHeadSegment(v.root, segID),
	} {
		if _, err := os.Stat(p); err == nil {
			return true
		}
	}
	return false
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
	v, err := newVaultDist(root, cfg)
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

	m.wg.Go(func() {
		<-ctx.Done()
	})

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

	m.wg.Wait()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()
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
	batch := m.coalesceBatch(publishQ, first)
	for vaultID, items := range groupPendingByVault(batch) {
		if err := m.publishVaultBatch(ctx, vaultID, items); publishRetryable(err) {
			for _, p := range items {
				m.enqueuePublishRetry(p)
			}
		}
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

func (m *Manager) enqueuePublishRetry(p pendingPublish) {
	m.retryMu.Lock()
	for _, existing := range m.retryPending {
		if existing.segID == p.segID {
			m.retryMu.Unlock()
			m.NotifyPublishRetry()
			return
		}
	}
	m.retryPending = append(m.retryPending, p)
	m.retryMu.Unlock()
	m.NotifyPublishRetry()
}

func (m *Manager) drainPublishRetries(ctx context.Context, publishQ chan pendingPublish) {
	m.retryMu.Lock()
	pending := m.retryPending
	m.retryPending = nil
	m.retryMu.Unlock()
	if len(pending) == 0 {
		return
	}
	for vaultID, items := range groupPendingByVault(pending) {
		if err := m.publishVaultBatch(ctx, vaultID, items); publishRetryable(err) {
			for _, p := range items {
				m.enqueuePublishRetry(p)
			}
		}
	}
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
			if p, alreadyStaged, err := m.stageForPublish(seg); err == nil && !alreadyStaged {
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
		for _, seg := range v.stranded(vaultID) {
			if p, alreadyStaged, err := m.stageForPublish(seg); err == nil && !alreadyStaged {
				m.enqueuePublish(ctx, publishQ, p)
			}
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

// onCompleted stages and publishes one completed segment. Returns metadata, the
// durable path (completed/ until publish succeeds), and retryable=true when
// staging succeeded but vault-ctl publish failed — the file stays in completed/
// for local holders until a retry commits the registry entry.
func (m *Manager) onCompleted(ctx context.Context, seg segmentation.CompletedSegment) (Metadata, string, bool, error) {
	p, _, err := m.stageForPublish(seg)
	if err != nil {
		return Metadata{}, "", false, err
	}
	if err := m.publishMeta(ctx, p.vaultID, p.meta, p.segID, p.path); err != nil {
		return p.meta, p.path, true, err
	}
	return p.meta, p.path, false, nil
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

// publishMeta re-attempts vault-ctl publish and promotes to head/ on success.
func (m *Manager) publishMeta(ctx context.Context, vaultID glid.GLID, meta Metadata, segID glid.GLID, path string) error {
	return m.publishVaultBatch(ctx, vaultID, []pendingPublish{{
		vaultID: vaultID,
		segID:   segID,
		path:    path,
		meta:    meta,
	}})
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
