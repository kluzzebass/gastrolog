package chunking

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/notify"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrNotRunning is returned when Run is called twice.
var ErrNotRunning = errors.New("chunking manager not running")

// replanInterval is the worker's periodic catch-up pass. The wake-signal
// event chain has gaps no FSM callback covers: a collection pull finishing
// (planner skips segments not yet in head/, and the pull is a local file
// operation with no Raft event) and a failed/timed-out planner Apply. One
// successful tick-driven step re-arms the event chain, so this only bounds
// stall recovery, not steady-state latency.
const replanInterval = 2 * time.Second

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
	// OnBuilt fires after this node successfully builds a sealed GLCB
	// (every home, not just the leader). Homes that finish building AFTER
	// the leader's CmdSealChunk applied use it to register the GLCB for
	// local queries — the FSM onSeal callback already ran and found no
	// file on disk. Optional.
	OnBuilt func(chunk.ChunkID)
	// IndexOpener opens a completed segment for planner indexing. Defaults
	// to BuildOrderedIndex when nil (tests may inject a counting wrapper).
	IndexOpener func(path string) (*OrderedIndex, error)
}

type vaultChunking struct {
	cfg VaultConfig

	mu        sync.Mutex
	planMu    sync.Mutex
	// segmentIndexCache holds open EventID indexes for active registry
	// segments between planner steps. loadSegmentViews reuses entries
	// instead of re-opening every segment on each planOnce.
	segmentIndexCache map[glid.GLID]*OrderedIndex
	doneBuild         buildKey
	// unsubPublish removes this vault's publish-callback subscription on the
	// shared FSM fan-out.
	unsubPublish func()
	// wake coalesces plan/build triggers for the per-vault worker goroutine.
	// FSM callbacks (publish, open-manifest, ref-added, sealed-manifest) fire
	// on the Raft FSM-apply goroutine and only poke this signal — running the
	// planner or builder inline would deadlock: both propose manifest edits
	// through raft.Apply on the same group, and that apply cannot complete
	// while the FSM goroutine is parked inside the callback.
	wake *notify.Signal
	// stopWorker cancels the per-vault worker; nil until the worker starts.
	stopWorker context.CancelFunc
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
	return &vaultChunking{
		cfg:               cfg,
		wake:              notify.NewSignal(),
		segmentIndexCache: make(map[glid.GLID]*OrderedIndex),
	}, nil
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

	// All four FSM callbacks coalesce into the same wake signal; the worker
	// runs a plan step followed by a build step on every wake, and both
	// no-op quickly when there is nothing to do.
	cfg.FSM.SetOnSealedManifest(func(*vaultctlfsm.OpenChunkManifest) {
		v.wake.Notify()
	})
	v.unsubPublish = cfg.FSM.AddOnPublishCompletedSegment(func(vaultctlfsm.CompletedSegmentEntry) {
		v.wake.Notify()
	})
	cfg.FSM.SetOnOpenChunkManifest(func(*vaultctlfsm.OpenChunkManifest) {
		v.wake.Notify()
	})
	cfg.FSM.SetOnOpenChunkRefAdded(func(*vaultctlfsm.OpenChunkManifest) {
		v.wake.Notify()
	})
	// Without this wake, segments published while a build was in flight
	// are only chunked when a future publish arrives — the planner refuses
	// to open a new manifest while a sealed one is pending.
	cfg.FSM.SetOnSealedManifestCleared(func(chunk.ChunkID) {
		v.wake.Notify()
	})
	if m.runCtx != nil {
		m.startWorkerLocked(v)
	}
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
		v.cfg.FSM.SetOnSealedManifestCleared(nil)
		if v.stopWorker != nil {
			v.stopWorker()
		}
		v.closeSegmentIndexCache()
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

// NotifyVault wakes the per-vault plan/build worker. Used when vault-ctl
// leadership aligns on the placement leader so catch-up runs after startup
// elections, not only on the first worker tick.
func (m *Manager) NotifyVault(vaultID glid.GLID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if ok {
		v.wake.Notify()
	}
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

// Run blocks until ctx is cancelled. Each registered vault gets a worker
// goroutine that first catches up any sealed manifest awaiting build and any
// planner work (when this node is vault leader), then plans + builds on every
// wake signal.
func (m *Manager) Run(ctx context.Context) error {
	if !m.running.CompareAndSwap(false, true) {
		return ErrNotRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	for _, v := range m.vaults {
		m.startWorkerLocked(v)
	}
	m.mu.Unlock()

	m.wg.Go(func() {
		<-ctx.Done()
	})

	m.wg.Wait()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()
	return ctx.Err()
}

// startWorkerLocked launches the per-vault plan/build worker. Caller holds
// m.mu and has verified m.runCtx is non-nil. The worker decouples planner and
// builder passes from the FSM callbacks that trigger them — see the wake
// field comment on vaultChunking for the deadlock this prevents.
func (m *Manager) startWorkerLocked(v *vaultChunking) {
	if v.stopWorker != nil {
		return // already running
	}
	ctx, cancel := context.WithCancel(m.runCtx)
	v.stopWorker = cancel
	m.wg.Go(func() {
		// Capture the wake channel BEFORE each pass so a signal arriving
		// mid-pass re-fires the loop instead of being lost.
		ch := v.wake.C()
		if err := v.planCatchUp(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("chunking plan catch-up failed", "vault", v.cfg.VaultID, "error", err)
		}
		if err := v.buildOnce(ctx); err != nil && ctx.Err() == nil {
			slog.Warn("chunking build failed", "vault", v.cfg.VaultID, "error", err)
		}
		tick := time.NewTicker(replanInterval)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
			case <-tick.C:
			}
			ch = v.wake.C()
			if err := v.planCatchUp(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("chunking plan catch-up failed", "vault", v.cfg.VaultID, "error", err)
			}
			if err := v.buildOnce(ctx); err != nil && ctx.Err() == nil {
				slog.Warn("chunking build failed", "vault", v.cfg.VaultID, "error", err)
			}
		}
	})
}

func (v *vaultChunking) buildOnce(ctx context.Context) error {
	pending := v.cfg.FSM.SealedManifest()
	if pending == nil {
		return nil
	}

	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	v.mu.Lock()
	alreadyBuilt := v.doneBuild == key
	v.mu.Unlock()

	var result BuildResult
	var err error
	switch {
	case !alreadyBuilt:
		result, err = v.build(ctx, pending)
		if err != nil {
			return err
		}
		v.mu.Lock()
		v.doneBuild = key
		v.mu.Unlock()
	case v.cfg.Applier != nil:
		// Local GLCB already exists (often from a prior pass on this or another
		// home). Rebuild is idempotent and supplies SealChunk metadata for a
		// retry when the prior CmdSealChunk commit failed or was not attempted.
		result, err = v.build(ctx, pending)
		if err != nil {
			return err
		}
	default:
		return nil
	}

	// Any home may propose CmdSealChunk after a successful build. Production
	// Applier forwards to the vault-ctl Raft leader; the leader gate lived only
	// on this node and left GLCBs on disk without FSM sealed entries when the
	// Raft leader was not the home that finished building (gastrolog-4trvb).
	if v.cfg.Applier != nil {
		if err := v.cfg.Applier.Apply(vaultctlfsm.MarshalSealChunk(
			pending.ChunkID,
			result.WriteEnd,
			int64(result.RecordCount),
			result.Bytes,
			result.IngestStart,
			result.IngestEnd,
			result.SourceEnd,
			result.IngestTSMonotonic,
		)); err != nil {
			return err
		}
	}

	if v.cfg.OnBuilt != nil {
		v.cfg.OnBuilt(pending.ChunkID)
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
