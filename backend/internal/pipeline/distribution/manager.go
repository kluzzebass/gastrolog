package distribution

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segmentation"
)

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

func (v *vaultDist) publish(ctx context.Context, seg segmentation.CompletedSegment) error {
	meta, err := MetadataFrom(seg)
	if err != nil {
		return err
	}

	path := seg.Path
	if v.localHolder() {
		path, err = PromoteToHead(path, v.root)
		if err != nil {
			return err
		}
	}

	v.mu.Lock()
	v.segments[seg.Meta.ID] = path
	v.mu.Unlock()

	if err := v.publisher.Publish(ctx, meta); err != nil {
		v.mu.Lock()
		delete(v.segments, seg.Meta.ID)
		v.mu.Unlock()
		return err
	}
	return nil
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
		for {
			select {
			case <-ctx.Done():
				return
			case seg, ok := <-completed:
				if !ok {
					return
				}
				m.onCompleted(ctx, seg)
			case req, ok := <-m.pullIn:
				if !ok {
					return
				}
				m.onPull(req)
			}
		}
	})

	m.wg.Wait()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()
	return ctx.Err()
}

func (m *Manager) onCompleted(ctx context.Context, seg segmentation.CompletedSegment) {
	m.mu.Lock()
	v, ok := m.vaults[seg.VaultID]
	m.mu.Unlock()
	if !ok {
		return
	}
	_ = v.publish(ctx, seg)
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
