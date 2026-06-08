package chunking

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("chunking manager not running")

// VaultCtlApplier applies marshaled vault-ctl commands for one vault.
type VaultCtlApplier interface {
	Apply(data []byte) error
}

// CollectionNudger triggers segment collection when manifest segments are missing locally.
type CollectionNudger interface {
	CollectMissing(ctx context.Context) error
}

// VaultConfig is per-vault chunking execution state.
type VaultConfig struct {
	VaultID   glid.GLID
	VaultRoot string
	ChunkRoot string
	FSM       *vaultctlfsm.FSM
	Locate    SegmentLocator
	Nudge     CollectionNudger
	Applier   VaultCtlApplier
	IsLeader  func() bool
	Policy    ManifestRotationPolicy
	NewChunkID func() chunk.ChunkID
}

type vaultChunking struct {
	cfg VaultConfig

	mu        sync.Mutex
	planMu    sync.Mutex
	doneBuild buildKey
	// unsubPublish removes this vault's publish-callback subscription on the
	// shared FSM fan-out.
	unsubPublish func()
}

type buildKey struct {
	chunkID  chunk.ChunkID
	sealedAt time.Time
}

func (c VaultConfig) newChunkID() chunk.ChunkID {
	if c.NewChunkID != nil {
		return c.NewChunkID()
	}
	return chunk.NewChunkID()
}

func newVaultChunking(cfg VaultConfig) (*vaultChunking, error) {
	if cfg.FSM == nil {
		return nil, errors.New("vault-ctl FSM required")
	}
	if cfg.Locate == nil {
		return nil, errors.New("segment locator required")
	}
	if cfg.ChunkRoot == "" {
		return nil, errors.New("chunk root required")
	}
	if cfg.IsLeader == nil {
		cfg.IsLeader = func() bool { return false }
	}
	return &vaultChunking{cfg: cfg}, nil
}

// Config configures a ChunkingManager.
type Config struct{}

// Manager runs per-home chunking for registered vaults. The vault leader
// proposes manifest edits via Plan (event-driven FSM callbacks); every home
// builds GLCB at seal via SetOnSealedManifest.
type Manager struct {
	cfg Config

	mu     sync.Mutex
	vaults map[glid.GLID]*vaultChunking
	runCtx context.Context

	running atomic.Bool
	wg      sync.WaitGroup
}

// New returns a chunking manager.
func New(cfg Config) *Manager {
	return &Manager{
		cfg:    cfg,
		vaults: make(map[glid.GLID]*vaultChunking),
	}
}

// RegisterVault adds a vault chunking path and wires vault-ctl FSM callbacks.
// Safe before or during Run.
func (m *Manager) RegisterVault(vaultID glid.GLID, cfg VaultConfig) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.vaults[vaultID]; ok {
		return errors.New("vault already registered")
	}
	cfg.VaultID = vaultID
	v, err := newVaultChunking(cfg)
	if err != nil {
		return err
	}
	m.vaults[vaultID] = v

	vid := vaultID
	cfg.FSM.SetOnSealedManifest(func(*vaultctlfsm.OpenChunkManifest) {
		m.triggerBuild(vid)
	})
	v.unsubPublish = cfg.FSM.AddOnPublishCompletedSegment(func(vaultctlfsm.CompletedSegmentEntry) {
		m.triggerPlan(vid)
	})
	cfg.FSM.SetOnOpenChunkManifest(func(*vaultctlfsm.OpenChunkManifest) {
		m.triggerPlan(vid)
	})
	cfg.FSM.SetOnOpenChunkRefAdded(func(*vaultctlfsm.OpenChunkManifest) {
		m.triggerPlan(vid)
	})
	return nil
}

// UnregisterVault removes a vault from chunking dispatch.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	delete(m.vaults, vaultID)
	m.mu.Unlock()
	if ok {
		v.cfg.FSM.SetOnSealedManifest(nil)
		if v.unsubPublish != nil {
			v.unsubPublish()
		}
		v.cfg.FSM.SetOnOpenChunkManifest(nil)
		v.cfg.FSM.SetOnOpenChunkRefAdded(nil)
	}
}

// PlanOnce runs one leader planner step for a vault (for tests).
func (m *Manager) PlanOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.planOnce(ctx, false)
}

// RotateCron runs one leader planner step with the cron rotation trigger set,
// sealing a non-empty open manifest on schedule. It is the entry point for the
// orchestrator's shared scheduler; the planner no-ops for non-leaders, so the
// job can run on every home and self-select the leader.
func (m *Manager) RotateCron(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.planOnce(ctx, true)
}

// BuildOnce runs one build pass for a vault (for tests).
func (m *Manager) BuildOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.buildOnce(ctx)
}

// Run blocks until ctx is cancelled. On start it catches up any sealed manifest
// awaiting build and any planner work when this node is vault leader.
func (m *Manager) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	vaults := make([]*vaultChunking, 0, len(m.vaults))
	for _, v := range m.vaults {
		vaults = append(vaults, v)
	}
	m.mu.Unlock()

	for _, v := range vaults {
		_ = v.planCatchUp(ctx)
		_ = v.buildOnce(ctx)
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

func (m *Manager) triggerBuild(vaultID glid.GLID) {
	m.mu.Lock()
	ctx := m.runCtx
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok || ctx == nil {
		return
	}
	_ = v.buildOnce(ctx)
}

func (m *Manager) triggerPlan(vaultID glid.GLID) {
	m.mu.Lock()
	ctx := m.runCtx
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok || ctx == nil {
		return
	}
	_ = v.planOnce(ctx, false)
}

func (v *vaultChunking) buildOnce(ctx context.Context) error {
	pending := v.cfg.FSM.SealedManifest()
	if pending == nil {
		return nil
	}

	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	v.mu.Lock()
	if v.doneBuild == key {
		v.mu.Unlock()
		return nil
	}
	v.mu.Unlock()

	result, err := v.build(ctx, pending)
	if err != nil {
		return err
	}

	v.mu.Lock()
	v.doneBuild = key
	v.mu.Unlock()

	if v.cfg.IsLeader() && v.cfg.Applier != nil {
		return v.cfg.Applier.Apply(vaultctlfsm.MarshalSealChunk(
			pending.ChunkID,
			result.WriteEnd,
			int64(result.RecordCount),
			result.Bytes,
			result.IngestStart,
			result.IngestEnd,
			result.SourceEnd,
			result.IngestTSMonotonic,
		))
	}
	return nil
}

func (v *vaultChunking) build(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest) (BuildResult, error) {
	input := BuildInput{
		Manifest:  sealedManifestFromFSM(pending),
		VaultID:   v.cfg.VaultID,
		ChunkRoot: v.cfg.ChunkRoot,
		Locate:    v.cfg.Locate,
	}
	result, err := BuildSealedChunk(input)
	if err == nil {
		return result, nil
	}
	var missing *MissingSegmentsError
	if !errors.As(err, &missing) || v.cfg.Nudge == nil {
		return BuildResult{}, err
	}
	if nudgeErr := v.cfg.Nudge.CollectMissing(ctx); nudgeErr != nil {
		return BuildResult{}, err
	}
	return BuildSealedChunk(input)
}

func sealedManifestFromFSM(m *vaultctlfsm.OpenChunkManifest) SealedManifest {
	out := SealedManifest{
		ChunkID:  m.ChunkID,
		OpenedAt: m.OpenedAt,
		SealedAt: m.SealedAt,
		Refs:     make([]ManifestRefEntry, len(m.Refs)),
	}
	for i, ref := range m.Refs {
		out.Refs[i] = ManifestRefEntry{
			SegmentID:         ref.SegmentID,
			FirstRecordNumber: ref.FirstRecordNumber,
			LastRecordNumber:  ref.LastRecordNumber,
		}
	}
	return out
}

// HeadSegmentLocator resolves segments present under vaultRoot/head/.
type HeadSegmentLocator struct {
	Root string
}

func (l HeadSegmentLocator) SegmentPath(segmentID glid.GLID) (string, bool) {
	path := paths.HeadSegment(l.Root, segmentID)
	if _, err := os.Stat(path); err != nil {
		return "", false
	}
	return path, true
}

// ErrUnknownVault is returned for an unregistered vault.
var ErrUnknownVault = errors.New("unknown vault")
