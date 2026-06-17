package chunking

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
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

// sealRetryInterval bounds how often a home retries CmdSealChunk after a
// failed forward/apply. Without it every replanInterval tick on every home
// logs and hammers the vault-ctl leader while sealedManifest is pending.
const sealRetryInterval = 15 * time.Second

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
	// RequiredHolders returns vault placement member node IDs that must appear
	// in each segment's holder set before the leader proposes ReleaseSegments.
	// Nil or empty means no holder gate (single-node tests).
	RequiredHolders func() []string
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
	// donePostSeal tracks head-purge + release-queue work already done for a
	// sealed manifest so seal retries do not re-purge or re-enqueue segments.
	donePostSeal buildKey
	// doneSealProposed stops vault-ctl CmdSealChunk replays after the first
	// successful Apply for a sealed manifest; without it every wake/tick on
	// every home floods Raft while sealedManifest is still pending locally.
	doneSealProposed buildKey
	// sealAttemptKey/lastSealAttempt rate-limit CmdSealChunk retries after
	// forward failures (e.g. stale vault-ctl leader) so tick loops stay quiet.
	sealAttemptKey  buildKey
	lastSealAttempt time.Time
	// doneOnBuilt ensures OnBuilt fires once per sealed manifest build.
	doneOnBuilt buildKey
	// lastBuild caches the most recent GLCB build for seal retries without
	// re-reading every segment on each wake/tick while sealedManifest pending.
	lastBuild struct {
		key    buildKey
		result BuildResult
		ok     bool
	}
	// pendingSeal is a copy of the sealed manifest retained after CmdSealChunk
	// clears sealedManifest cluster-wide so follower homes can still build.
	pendingSeal *vaultctlfsm.OpenChunkManifest
	// pendingRelease holds segment IDs awaiting ReleaseSegments once every
	// required vault home has committed a holder receipt.
	pendingRelease []glid.GLID
	// unsubPublish removes this vault's publish-callback subscription on the
	// shared FSM fan-out.
	unsubPublish func()
	// unsubAckHolder removes the holder-ack subscription used to retry release.
	unsubAckHolder func()
	// wake coalesces plan/build triggers for the per-vault worker goroutine.
	// FSM callbacks (publish, open-manifest, ref-added, sealed-manifest) fire
	// on the Raft FSM-apply goroutine and only poke this signal — running the
	// planner or builder inline would deadlock: both propose manifest edits
	// through raft.Apply on the same group, and that apply cannot complete
	// while the FSM goroutine is parked inside the callback.
	wake *notify.Signal
	// releaseWake coalesces holder-ack triggers for releaseOnce only. Holder
	// receipts are frequent during ingest; waking the full plan/build loop on
	// each ack rebuilds GLCBs and floods vault-ctl with duplicate proposals.
	releaseWake *notify.Signal
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
		releaseWake:       notify.NewSignal(),
		segmentIndexCache: make(map[glid.GLID]*OrderedIndex),
	}, nil
}

// Config configures a ChunkingManager.
type Config struct {
	Logger *slog.Logger
}

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
	cfg.Logger = compChunking.Apply(logging.Default(cfg.Logger))
	return &Manager{
		cfg:    cfg,
		vaults: make(map[glid.GLID]*vaultChunking),
	}
}

func (m *Manager) logger() *slog.Logger {
	if m.cfg.Logger != nil {
		return m.cfg.Logger
	}
	return slog.Default()
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
	cfg.FSM.SetOnSealedManifest(func(m *vaultctlfsm.OpenChunkManifest) {
		v.mu.Lock()
		v.pendingSeal = m
		v.mu.Unlock()
		v.wake.Notify()
	})
	v.unsubPublish = cfg.FSM.AddOnPublishCompletedSegment(func(vaultctlfsm.CompletedSegmentEntry) {
		v.wake.Notify()
	})
	v.unsubAckHolder = cfg.FSM.AddOnAckSegmentHolder(func(glid.GLID) {
		v.releaseWake.Notify()
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
	cfg.FSM.SetOnSealedManifestCleared(func(id chunk.ChunkID) {
		v.mu.Lock()
		var pending *vaultctlfsm.OpenChunkManifest
		if v.pendingSeal != nil && v.pendingSeal.ChunkID == id {
			sealedCopy := *v.pendingSeal
			sealedCopy.Refs = slices.Clone(v.pendingSeal.Refs)
			pending = &sealedCopy
		}
		v.doneSealProposed = buildKey{}
		v.sealAttemptKey = buildKey{}
		v.doneOnBuilt = buildKey{}
		v.mu.Unlock()
		if pending != nil {
			v.afterSealBuild(pending)
		}
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
		if v.unsubAckHolder != nil {
			v.unsubAckHolder()
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
	if err := v.buildOnce(ctx); err != nil {
		return err
	}
	return v.releaseOnce(ctx)
}

// ReleaseOnce runs one holder-gated ReleaseSegments pass for a vault (for tests).
func (m *Manager) ReleaseOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.releaseOnce(ctx)
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
		log := m.logger().With("vault", v.cfg.VaultID)
		if err := v.recoverOnce(ctx); err != nil && ctx.Err() == nil {
			log.Warn("chunking recover failed", "error", err)
		}
		v.purgeStaleHeadCatchUp()
		m.runBuildPass(ctx, v, log, false)
		tick := time.NewTicker(replanInterval)
		defer tick.Stop()
		releaseCh := v.releaseWake.C()
		for {
			select {
			case <-ctx.Done():
				return
			case <-releaseCh:
				if err := v.releaseOnce(ctx); err != nil && ctx.Err() == nil {
					log.Warn("chunking release failed", "error", err)
				}
				releaseCh = v.releaseWake.C()
				continue
			case <-ch:
				m.runBuildPass(ctx, v, log, false)
			case <-tick.C:
				if err := v.releaseOnce(ctx); err != nil && ctx.Err() == nil {
					log.Warn("chunking release failed", "error", err)
				}
				m.runBuildPass(ctx, v, log, true)
			}
			ch = v.wake.C()
		}
	})
}

func (m *Manager) runBuildPass(ctx context.Context, v *vaultChunking, log *slog.Logger, onTick bool) {
	if err := v.planCatchUp(ctx); err != nil && ctx.Err() == nil {
		log.Warn("chunking plan catch-up failed", "error", err)
	}
	if !v.buildDue(time.Now(), onTick) {
		return
	}
	if err := v.buildOnce(ctx); err != nil && ctx.Err() == nil {
		log.Warn("chunking build failed", "error", err)
	}
}

func (v *vaultChunking) buildDue(now time.Time, onTick bool) bool {
	pending := v.sealedManifestForBuild()
	if pending == nil {
		return false
	}
	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	if entry := v.cfg.FSM.Get(pending.ChunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
		v.mu.Lock()
		alreadyBuilt := v.doneBuild == key
		v.mu.Unlock()
		if alreadyBuilt {
			return false
		}
		// Sealed cluster-wide but this home still needs to materialize GLCB.
		return true
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.doneBuild != key {
		return true
	}
	if v.doneSealProposed == key {
		return false
	}
	if !onTick {
		return true
	}
	return v.sealAttemptKey != key || now.Sub(v.lastSealAttempt) >= sealRetryInterval
}

func (v *vaultChunking) sealedManifestForBuild() *vaultctlfsm.OpenChunkManifest {
	if pending := v.cfg.FSM.SealedManifest(); pending != nil {
		v.mu.Lock()
		v.pendingSeal = pending
		v.mu.Unlock()
		return pending
	}
	v.mu.Lock()
	pending := v.pendingSeal
	v.mu.Unlock()
	return pending
}

func (v *vaultChunking) releaseOnce(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !v.cfg.IsLeader() || v.cfg.Applier == nil {
		return nil
	}
	v.mu.Lock()
	pending := v.pendingRelease
	v.pendingRelease = nil
	v.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	required := v.requiredHolders()
	ready, stillPending := partitionPendingRelease(v.cfg.FSM, pending, required)
	if len(ready) == 0 {
		v.mu.Lock()
		v.pendingRelease = append(stillPending, v.pendingRelease...)
		v.mu.Unlock()
		return nil
	}
	if err := v.cfg.Applier.Apply(vaultctlfsm.MarshalReleaseSegments(ready)); err != nil {
		v.mu.Lock()
		v.pendingRelease = append(pending, v.pendingRelease...)
		v.mu.Unlock()
		return err
	}
	v.mu.Lock()
	v.pendingRelease = append(stillPending, v.pendingRelease...)
	v.mu.Unlock()
	return nil
}

func (v *vaultChunking) requiredHolders() []string {
	if v.cfg.RequiredHolders == nil {
		return nil
	}
	return v.cfg.RequiredHolders()
}

func (v *vaultChunking) afterSealBuild(pending *vaultctlfsm.OpenChunkManifest) {
	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	v.mu.Lock()
	if v.donePostSeal == key {
		v.mu.Unlock()
		return
	}
	v.donePostSeal = key
	v.mu.Unlock()

	segmentIDs := releasableSegmentIDs(v.cfg.FSM, pending)
	v.flushHeadPurgeForManifest(pending, segmentIDs)
	if v.cfg.IsLeader() && len(segmentIDs) > 0 {
		v.mu.Lock()
		v.pendingRelease = appendUniqueGLIDs(v.pendingRelease, segmentIDs)
		v.mu.Unlock()
	}
	v.mu.Lock()
	if v.pendingSeal != nil && v.pendingSeal.ChunkID == pending.ChunkID && v.doneBuild == key {
		v.pendingSeal = nil
	}
	v.mu.Unlock()
}

func (v *vaultChunking) buildOnce(ctx context.Context) error {
	pending := v.sealedManifestForBuild()
	if pending == nil {
		return nil
	}

	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	if done, err := v.buildOnceIfSealedElsewhere(pending, key); done || err != nil {
		return err
	}

	result, builtNow, err := v.runBuildOncePass(ctx, pending, key)
	if err != nil || (!builtNow && v.cfg.Applier == nil) {
		return err
	}

	if err := v.proposeSealOnce(pending, key, result); err != nil {
		return err
	}

	v.fireOnBuiltOnce(pending, key, builtNow)
	v.finishBuildOnce(pending, key)
	return nil
}

// buildOnceIfSealedElsewhere handles the case where CmdSealChunk already
// applied cluster-wide. Returns true when no further build work is needed.
func (v *vaultChunking) buildOnceIfSealedElsewhere(pending *vaultctlfsm.OpenChunkManifest, key buildKey) (bool, error) {
	entry := v.cfg.FSM.Get(pending.ChunkID)
	if entry == nil || entry.State != chunk.ChunkStateSealed {
		return false, nil
	}

	v.mu.Lock()
	alreadyBuilt := v.doneBuild == key
	if alreadyBuilt && v.pendingSeal != nil && v.pendingSeal.ChunkID == pending.ChunkID {
		v.pendingSeal = nil
	}
	v.mu.Unlock()
	if !alreadyBuilt {
		return false, nil
	}

	// Another home proposed CmdSealChunk first; this home still needs local
	// head purge and release-queue work (gastrolog-3vlse).
	v.afterSealBuild(pending)
	if v.cfg.OnBuilt != nil {
		v.mu.Lock()
		fire := v.doneOnBuilt != key
		if fire {
			v.doneOnBuilt = key
		}
		v.mu.Unlock()
		if fire {
			v.cfg.OnBuilt(pending.ChunkID)
		}
	}
	return true, nil
}

func (v *vaultChunking) runBuildOncePass(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey) (BuildResult, bool, error) {
	v.mu.Lock()
	alreadyBuilt := v.doneBuild == key
	cached := v.lastBuild
	v.mu.Unlock()

	var result BuildResult
	var err error
	builtNow := false
	switch {
	case !alreadyBuilt:
		result, err = v.build(ctx, pending)
		if err != nil {
			return BuildResult{}, false, err
		}
		builtNow = true
		v.mu.Lock()
		v.doneBuild = key
		v.lastBuild = struct {
			key    buildKey
			result BuildResult
			ok     bool
		}{key: key, result: result, ok: true}
		v.mu.Unlock()
	case v.cfg.Applier != nil && cached.ok && cached.key == key:
		// Retry CmdSealChunk with the prior build output; do not re-read every
		// segment on each wake/tick while the sealed manifest is still pending.
		result = cached.result
	case v.cfg.Applier != nil:
		result, err = v.build(ctx, pending)
		if err != nil {
			return BuildResult{}, false, err
		}
		builtNow = true
		v.mu.Lock()
		v.lastBuild = struct {
			key    buildKey
			result BuildResult
			ok     bool
		}{key: key, result: result, ok: true}
		v.mu.Unlock()
	default:
		return BuildResult{}, false, nil
	}
	return result, builtNow, nil
}

func (v *vaultChunking) proposeSealOnce(pending *vaultctlfsm.OpenChunkManifest, key buildKey, result BuildResult) error {
	if v.cfg.Applier == nil {
		return nil
	}
	v.mu.Lock()
	alreadyProposed := v.doneSealProposed == key
	v.mu.Unlock()
	if alreadyProposed {
		return nil
	}
	v.mu.Lock()
	v.sealAttemptKey = key
	v.lastSealAttempt = time.Now()
	v.mu.Unlock()
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
	v.mu.Lock()
	v.doneSealProposed = key
	v.mu.Unlock()
	v.afterSealBuild(pending)
	return nil
}

func (v *vaultChunking) fireOnBuiltOnce(pending *vaultctlfsm.OpenChunkManifest, key buildKey, builtNow bool) {
	if !builtNow || v.cfg.OnBuilt == nil {
		return
	}
	v.mu.Lock()
	fire := v.doneOnBuilt != key
	if fire {
		v.doneOnBuilt = key
	}
	v.mu.Unlock()
	if fire {
		v.cfg.OnBuilt(pending.ChunkID)
	}
}

func (v *vaultChunking) finishBuildOnce(pending *vaultctlfsm.OpenChunkManifest, key buildKey) {
	v.mu.Lock()
	postSeal := v.donePostSeal == key
	v.mu.Unlock()
	if postSeal {
		v.flushHeadPurgeForManifest(pending, releasableSegmentIDs(v.cfg.FSM, pending))
	}
	v.mu.Lock()
	if v.doneBuild == key && v.pendingSeal != nil && v.pendingSeal.ChunkID == pending.ChunkID {
		v.pendingSeal = nil
	}
	v.mu.Unlock()
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
