package distribution

import (
	"context"
	"errors"
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

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("distribution manager not running")

// ErrUnknownVault is returned for an unregistered vault.
var ErrUnknownVault = errors.New("unknown vault")

// VaultConfig is per-vault distribution state.
type VaultConfig struct {
	// Root is the vault storage root (contains segmentation completed/).
	Publisher Publisher
	// LocalHolder reports whether this node holds the vault locally (completed→head rename).
	LocalHolder func() bool
}

type vaultDist struct {
	root        string
	publisher   Publisher
	localHolder func() bool
	mu          sync.RWMutex
	segments    map[glid.GLID]string // segment ID → on-disk path
}

func newVaultDist(root string, cfg VaultConfig) (*vaultDist, error) {
	if cfg.Publisher == nil {
		return nil, errors.New("publisher required")
	}
	if cfg.LocalHolder == nil {
		cfg.LocalHolder = func() bool { return false }
	}
	return &vaultDist{
		root:        root,
		publisher:   cfg.Publisher,
		localHolder: cfg.LocalHolder,
		segments:    make(map[glid.GLID]string),
	}, nil
}

// prepare stages a completed segment for publishing: builds the metadata,
// promotes the file to head/ when this node holds the vault locally, and
// registers the (possibly moved) path for pull serving. Idempotent: an
// already-prepared segment (e.g. seen by both the completed channel and the
// directory rescan) rebuilds metadata from the registered path without
// repeating the head/ rename.
func (v *vaultDist) prepare(seg segmentation.CompletedSegment) (Metadata, error) {
	v.mu.RLock()
	registered, known := v.segments[seg.Meta.ID]
	v.mu.RUnlock()
	if known {
		seg.Path = registered
		return MetadataFrom(seg)
	}

	meta, err := MetadataFrom(seg)
	if err != nil {
		return Metadata{}, err
	}

	path := seg.Path
	if v.localHolder() {
		path, err = PromoteToHead(path, v.root)
		if err != nil {
			return Metadata{}, err
		}
	}

	v.mu.Lock()
	v.segments[seg.Meta.ID] = path
	v.mu.Unlock()
	return meta, nil
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
		v.mu.RUnlock()
		if known {
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
	meta, err := v.prepare(seg)
	if err != nil {
		return err
	}
	return v.publisher.Publish(ctx, meta)
}

func (v *vaultDist) servePull(req PullRequest) error {
	v.mu.RLock()
	path, ok := v.segments[req.SegmentID]
	v.mu.RUnlock()
	if !ok {
		return ErrSegmentNotFound
	}
	return StreamSegment(path, req.Dest)
}

// Config configures a DistributionManager.
type Config struct {
	// PullQueueCap bounds incoming pull requests. Defaults to 16.
	PullQueueCap int
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

	m.wg.Go(func() {
		m.runPublishLoop(ctx, completed)
	})

	m.wg.Wait()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()
	return ctx.Err()
}

type pendingPublish struct {
	vaultID glid.GLID
	meta    Metadata
}

// runPublishLoop consumes completed segments, pull requests, publish retries,
// and periodic rescans until ctx is cancelled or completed closes.
func (m *Manager) runPublishLoop(ctx context.Context, completed <-chan segmentation.CompletedSegment) {
	var pending []pendingPublish
	retry := time.NewTicker(publishRetryInterval)
	defer retry.Stop()
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
			if meta, retryable, err := m.onCompleted(ctx, seg); err != nil && retryable {
				pending = append(pending, pendingPublish{vaultID: seg.VaultID, meta: meta})
			}
		case req, ok := <-m.pullIn:
			if !ok {
				return
			}
			m.onPull(req)
		case <-retry.C:
			pending = m.drainPublishRetries(ctx, pending)
		case <-rescan.C:
			pending = m.rescanStranded(ctx, pending)
		}
	}
}

func (m *Manager) drainPublishRetries(ctx context.Context, pending []pendingPublish) []pendingPublish {
	remaining := pending[:0]
	for _, p := range pending {
		err := m.publishMeta(ctx, p.vaultID, p.meta)
		if err != nil && !errors.Is(err, ErrUnknownVault) {
			remaining = append(remaining, p)
		}
	}
	return remaining
}

func (m *Manager) rescanStranded(ctx context.Context, pending []pendingPublish) []pendingPublish {
	for vaultID, v := range m.vaultsSnapshot() {
		for _, seg := range v.stranded(vaultID) {
			if meta, retryable, err := m.onCompleted(ctx, seg); err != nil && retryable {
				pending = append(pending, pendingPublish{vaultID: vaultID, meta: meta})
			}
		}
	}
	return pending
}

// onCompleted prepares and publishes one completed segment. Returns the
// prepared metadata and retryable=true when the prepare succeeded but the
// vault-ctl publish failed — the caller queues it for retry. Prepare failures
// (stat/rename) and unknown vaults are not retryable.
func (m *Manager) onCompleted(ctx context.Context, seg segmentation.CompletedSegment) (Metadata, bool, error) {
	m.mu.Lock()
	v, ok := m.vaults[seg.VaultID]
	m.mu.Unlock()
	if !ok {
		return Metadata{}, false, ErrUnknownVault
	}
	meta, err := v.prepare(seg)
	if err != nil {
		return Metadata{}, false, err
	}
	if err := v.publisher.Publish(ctx, meta); err != nil {
		return meta, true, err
	}
	return meta, false, nil
}

// vaultsSnapshot copies the vault map for iteration outside m.mu.
func (m *Manager) vaultsSnapshot() map[glid.GLID]*vaultDist {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[glid.GLID]*vaultDist, len(m.vaults))
	maps.Copy(out, m.vaults)
	return out
}

// publishMeta re-attempts the vault-ctl publish for already-prepared segment
// metadata. The segment file was promoted/registered by the original prepare.
func (m *Manager) publishMeta(ctx context.Context, vaultID glid.GLID, meta Metadata) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.publisher.Publish(ctx, meta)
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
