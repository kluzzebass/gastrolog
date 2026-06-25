package distribution

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/segmentation"
)

// publishRetryInterval is how often failed vault-ctl publishes are retried.
// Publish failures are expected transients (no vault-ctl leader during an
// election or leadership transfer, forwarding RPC timeout); the completed
// segment stays on disk and registered for pulls, so the retry only needs to
// re-announce the metadata.
const publishRetryInterval = time.Second

// rescanInterval is how often each vault's completed/ directory is scanned
// for segments that never arrived on the completed channel. The segmentation
// writer's channel send is non-blocking (a full channel must not stall the
// fsync path), so under burst load a completed segment can be silently
// dropped from the channel — and a restart loses everything buffered. The
// scan is the durable catch-up: anything in completed/ that this manager has
// not prepared yet gets prepared and published.
const rescanInterval = 2 * time.Second

// publishQueueCap bounds staged publishes waiting for the vault-ctl worker.
// Ingress only enqueues; a dedicated worker issues Raft applies so pull serving
// never blocks behind a publish backlog.
const publishQueueCap = 512

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
}

type vaultDist struct {
	root        string
	publisher   Publisher
	localHolder func() bool
	onLocalHeadPromoted func(glid.GLID)
	mu          sync.RWMutex
	segments    map[glid.GLID]string // segment ID → on-disk path
	retired     map[glid.GLID]struct{} // released from vault-ctl; skip rescan republish
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
		segments:            make(map[glid.GLID]string),
		retired:             make(map[glid.GLID]struct{}),
	}, nil
}

// prepare stages a completed segment for vault-ctl publish: builds metadata and
// registers the on-disk path for pull serving. Local holders keep the file in
// completed/ until publish succeeds (see finalizeAfterPublish).
func (v *vaultDist) prepare(seg segmentation.CompletedSegment) (Metadata, string, error) {
	v.mu.RLock()
	registered, known := v.segments[seg.Meta.ID]
	v.mu.RUnlock()
	if known {
		seg.Path = registered
		meta, err := metadataForPublish(seg)
		return meta, registered, err
	}

	path := seg.Path
	meta, err := metadataForPublish(seg)
	if err != nil {
		return Metadata{}, "", err
	}

	v.mu.Lock()
	v.segments[seg.Meta.ID] = path
	v.mu.Unlock()
	return meta, path, nil
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
	meta, path, err := v.prepare(seg)
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
	return v.finalizeAfterPublish(segID, path)
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
	Logger       *slog.Logger
}

// Manager publishes completed segment metadata and serves segment pulls.
type Manager struct {
	cfg Config

	mu      sync.Mutex
	vaults  map[glid.GLID]*vaultDist
	pullIn  chan PullRequest
	runCtx  context.Context
	running atomic.Bool
	wg      sync.WaitGroup
}

// New returns a manager. Pull requests are sent to the returned channel.
func New(cfg Config) (*Manager, chan<- PullRequest) {
	queueCap := cfg.PullQueueCap
	if queueCap <= 0 {
		queueCap = 16
	}
	pullIn := make(chan PullRequest, queueCap)
	return &Manager{
		cfg:    cfg,
		vaults: make(map[glid.GLID]*vaultDist),
		pullIn: pullIn,
	}, pullIn
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
		m.runPublishWorker(ctx, publishQ)
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

// runPublishWorker commits staged segments to vault-ctl one at a time. Failed
// publishes are re-queued after publishRetryInterval.
func (m *Manager) runPublishWorker(ctx context.Context, publishQ chan pendingPublish) {
	for {
		select {
		case <-ctx.Done():
			return
		case p, ok := <-publishQ:
			if !ok {
				return
			}
			if err := m.publishMeta(ctx, p.vaultID, p.meta, p.segID, p.path); err != nil &&
				!errors.Is(err, ErrUnknownVault) &&
				!errors.Is(err, errPublishBytesMissing) {
				pCopy := p
				time.AfterFunc(publishRetryInterval, func() {
					m.enqueuePublish(ctx, publishQ, pCopy)
				})
			}
		}
	}
}

// runPublishIngress stages completed segments and enqueues vault-ctl publishes.
// It never blocks on Raft; rescans catch channel drops and restarts.
func (m *Manager) runPublishIngress(ctx context.Context, completed <-chan segmentation.CompletedSegment, publishQ chan<- pendingPublish) {
	rescan := time.NewTicker(rescanInterval)
	defer rescan.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case seg, ok := <-completed:
			if !ok {
				return
			}
			if p, err := m.stageForPublish(seg); err == nil {
				m.enqueuePublish(ctx, publishQ, p)
			}
		case <-rescan.C:
			for vaultID, v := range m.vaultsSnapshot() {
				for _, seg := range v.stranded(vaultID) {
					if p, err := m.stageForPublish(seg); err == nil {
						m.enqueuePublish(ctx, publishQ, p)
					}
				}
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

func (m *Manager) stageForPublish(seg segmentation.CompletedSegment) (pendingPublish, error) {
	m.mu.Lock()
	v, ok := m.vaults[seg.VaultID]
	m.mu.Unlock()
	if !ok {
		return pendingPublish{}, ErrUnknownVault
	}
	meta, path, err := v.prepare(seg)
	if err != nil {
		return pendingPublish{}, err
	}
	return pendingPublish{
		vaultID: seg.VaultID,
		segID:   seg.Meta.ID,
		path:    path,
		meta:    meta,
	}, nil
}

// onCompleted stages and publishes one completed segment. Returns metadata, the
// durable path (completed/ until publish succeeds), and retryable=true when
// staging succeeded but vault-ctl publish failed — the file stays in completed/
// for local holders until a retry commits the registry entry.
func (m *Manager) onCompleted(ctx context.Context, seg segmentation.CompletedSegment) (Metadata, string, bool, error) {
	p, err := m.stageForPublish(seg)
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

// publishMeta re-attempts vault-ctl publish and promotes to head/ on success.
func (m *Manager) publishMeta(ctx context.Context, vaultID glid.GLID, meta Metadata, segID glid.GLID, path string) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.publishStaged(ctx, meta, segID, path)
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
