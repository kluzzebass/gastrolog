package chunking

import (
	"context"
	"errors"
	"fmt"
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

// sealRetryInterval removed — seal/build retries wake on FSM events and
// vault-ctl leadership changes, not timed polls.

// VaultCtlApplier applies marshaled vault-ctl commands for one vault.
type VaultCtlApplier interface {
	Apply(data []byte) error
}

// SegmentCollector pulls segment bytes onto this home. Chunking invokes it as a
// build prerequisite (every manifest ref must be local before GLCB merge) and
// during planner catch-up when eligible registry segments are not yet in head/.
type SegmentCollector interface {
	// CollectOnce runs a full assignment-log pass for this vault.
	CollectOnce(ctx context.Context) error
	// CollectSegments pulls the given segment IDs when manifest refs require
	// bytes this home does not yet hold.
	CollectSegments(ctx context.Context, segmentIDs []glid.GLID) error
}

// VaultConfig is per-vault chunking execution state.
type VaultConfig struct {
	VaultID   glid.GLID
	VaultRoot string
	ChunkRoot string
	FSM       *vaultctlfsm.FSM
	// LookupFSM returns the live vault-ctl sub-FSM after snapshot Restore replaces
	// the object RegisterVault captured. Falls back to FSM when nil.
	LookupFSM func() *vaultctlfsm.FSM
	Locate    SegmentLocator
	Collector SegmentCollector
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
	// OnManifestOpened fires when CmdOpenChunkManifest applies (pipeline
	// vaults). Optional — orchestrator uses it for operator audit logging.
	OnManifestOpened func(*vaultctlfsm.OpenChunkManifest)
	// RequiredHolders returns vault placement member node IDs that must appear
	// in each segment's holder set before the leader proposes ReleaseSegments.
	// Nil or empty means no holder gate (single-node tests).
	RequiredHolders func() []string
	// IndexOpener opens a completed segment for planner indexing. Defaults
	// to BuildOrderedIndex when nil (tests may inject a counting wrapper).
	IndexOpener func(path string) (*OrderedIndex, error)
	// Now overrides wall clock for MaxAge rotation on the open manifest.
	// Nil uses time.Now (production). Tests inject a fixed clock.
	Now func() time.Time
}

type vaultChunking struct {
	cfg VaultConfig

	mu        sync.Mutex
	planMu    sync.Mutex
	// segmentIndexCache holds open EventID indexes for segments the planner has
	// opened lazily. pruneSegmentIndexCache evicts released or exhausted entries.
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
	// unsubRelease removes the ReleaseSegments subscription used to purge head/.
	unsubRelease func()
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
	// buildRunning gates a single in-flight GLCB build so planCatchUp can keep
	// running on wake while materialization/build proceeds asynchronously.
	buildRunning atomic.Bool
	// log is the per-vault logger; set when the worker starts.
	log *slog.Logger
}

func (v *vaultChunking) logger() *slog.Logger {
	if v.log != nil {
		return v.log
	}
	return slog.Default()
}

func (v *vaultChunking) fsm() *vaultctlfsm.FSM {
	if v.cfg.LookupFSM != nil {
		if f := v.cfg.LookupFSM(); f != nil {
			return f
		}
	}
	return v.cfg.FSM
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
	m.wireVaultFSMCallbacks(v, cfg)
	if m.runCtx != nil {
		m.startWorkerLocked(v)
	}
	return nil
}

// RewireVaultFSM rebinds chunking to a fresh vault-ctl sub-FSM after a group-level
// snapshot Restore replaces the old object. Without this the planner reads a frozen
// manifest/registry and stalls cluster-wide after leadership transfer.
func (m *Manager) RewireVaultFSM(vaultID glid.GLID, fsm *vaultctlfsm.FSM, applier VaultCtlApplier) error {
	if fsm == nil {
		return errors.New("vault-ctl FSM required")
	}
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	m.unwireVaultFSMCallbacks(v)
	v.cfg.FSM = fsm
	if v.cfg.LookupFSM == nil {
		v.cfg.LookupFSM = func() *vaultctlfsm.FSM { return fsm }
	}
	if applier != nil {
		v.cfg.Applier = applier
	}
	cfg := v.cfg
	cfg.FSM = fsm
	m.wireVaultFSMCallbacks(v, cfg)
	if pending := fsm.SealedManifest(); pending != nil {
		v.mu.Lock()
		v.pendingSeal = pending
		// Hot FSM restore can leave doneSealProposed set from when this home
		// was a follower; clear so the vault-ctl leader can commit CmdSealChunk.
		if entry := fsm.Get(pending.ChunkID); entry == nil || !entry.IsSealed() {
			v.doneSealProposed = buildKey{}
			v.sealAttemptKey = buildKey{}
		}
		v.mu.Unlock()
	}
	v.wake.Notify()
	return nil
}

// HasVault reports whether chunking is registered for a vault on this home.
func (m *Manager) HasVault(vaultID glid.GLID) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.vaults[vaultID]
	return ok
}

func (m *Manager) unwireVaultFSMCallbacks(v *vaultChunking) {
	fsm := v.cfg.FSM
	if fsm == nil {
		return
	}
	fsm.SetOnSealedManifest(nil)
	if v.unsubPublish != nil {
		v.unsubPublish()
		v.unsubPublish = nil
	}
	if v.unsubAckHolder != nil {
		v.unsubAckHolder()
		v.unsubAckHolder = nil
	}
	if v.unsubRelease != nil {
		v.unsubRelease()
		v.unsubRelease = nil
	}
	fsm.SetOnOpenChunkManifest(nil)
	fsm.SetOnOpenChunkRefAdded(nil)
	fsm.SetOnSealedManifestCleared(nil)
}

func (m *Manager) wireVaultFSMCallbacks(v *vaultChunking, cfg VaultConfig) {
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
	v.unsubRelease = cfg.FSM.AddOnReleaseSegments(func(ids []glid.GLID) {
		v.purgeReleasedHead(ids)
		v.purgeStaleHeadCatchUp()
	})
	cfg.FSM.SetOnOpenChunkManifest(func(m *vaultctlfsm.OpenChunkManifest) {
		if m != nil && cfg.OnManifestOpened != nil {
			cfg.OnManifestOpened(m)
		}
		v.wake.Notify()
	})
	cfg.FSM.SetOnOpenChunkRefAdded(func(*vaultctlfsm.OpenChunkManifest) {
		v.wake.Notify()
	})
	// Without this wake, segments published while a build was in flight
	// are only chunked when a future publish arrives.
	//
	// Must NOT call afterSealBuild inline: holder-receipt Apply on the Raft
	// FSM apply goroutine deadlocks the vault-ctl leader. Dispatch post-seal
	// work on a goroutine (same pattern as onRequestDelete acks).
	cfg.FSM.SetOnSealedManifestCleared(func(_ chunk.ChunkID) {
		v.mu.Lock()
		var pending *vaultctlfsm.OpenChunkManifest
		if v.pendingSeal != nil {
			sealedCopy := *v.pendingSeal
			sealedCopy.Refs = slices.Clone(v.pendingSeal.Refs)
			pending = &sealedCopy
		}
		v.doneSealProposed = buildKey{}
		v.sealAttemptKey = buildKey{}
		v.doneOnBuilt = buildKey{}
		v.mu.Unlock()
		if pending != nil {
			go v.afterSealBuild(context.Background(), pending)
		}
		v.wake.Notify()
	})
}

// UnregisterVault removes a vault from chunking dispatch.
func (m *Manager) UnregisterVault(vaultID glid.GLID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	delete(m.vaults, vaultID)
	m.mu.Unlock()
	if ok {
		m.unwireVaultFSMCallbacks(v)
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

// PlanCatchUp runs leader catch-up planner passes for a vault (for tests).
func (m *Manager) PlanCatchUp(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	return v.planCatchUp(ctx)
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
		v.log = log
		if err := v.recoverOnce(ctx); err != nil && ctx.Err() == nil {
			log.Warn("chunking recover failed", "error", err)
		}
		v.purgeStaleHeadCatchUp()
		m.runBuildPass(ctx, v, log)
		releaseCh := v.releaseWake.C()
		for {
			select {
			case <-ctx.Done():
				for v.buildRunning.Load() {
					time.Sleep(5 * time.Millisecond)
				}
				return
			case <-releaseCh:
				if err := v.releaseOnce(ctx); err != nil && ctx.Err() == nil {
					log.Warn("chunking release failed", "error", err)
				}
				v.purgeStaleHeadCatchUp()
				releaseCh = v.releaseWake.C()
				continue
			case <-ch:
				m.runBuildPass(ctx, v, log)
			}
			ch = v.wake.C()
		}
	})
}

func (m *Manager) runBuildPass(ctx context.Context, v *vaultChunking, log *slog.Logger) {
	if err := v.planCatchUp(ctx); err != nil && ctx.Err() == nil {
		log.Warn("chunking plan catch-up failed", "error", err)
	}
	if v.cfg.IsLeader() {
		v.releaseWake.Notify()
	}
	if !v.buildDue() {
		return
	}
	if !v.buildRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer v.buildRunning.Store(false)
		if err := v.buildOnce(ctx); err != nil && ctx.Err() == nil {
			log.Warn("chunking build failed", "error", err)
		}
		v.wake.Notify()
	}()
}

func (v *vaultChunking) buildDue() bool {
	pending := v.sealedManifestForBuild()
	if pending == nil {
		return false
	}
	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	if entry := v.fsm().Get(pending.ChunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
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
		if v.cfg.IsLeader() && !v.chunkSealCommitted(pending.ChunkID) {
			v.doneSealProposed = buildKey{}
			v.sealAttemptKey = buildKey{}
			return true
		}
		return false
	}
	return true
}

func (v *vaultChunking) sealedManifestForBuild() *vaultctlfsm.OpenChunkManifest {
	if pending := v.fsm().SealedManifest(); pending != nil {
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
	v.enqueueRegistryReleaseCandidates()
	v.mu.Lock()
	pending := v.pendingRelease
	v.pendingRelease = nil
	v.mu.Unlock()
	if len(pending) == 0 {
		return nil
	}
	required := v.requiredHolders()
	holdersWired := v.cfg.RequiredHolders != nil
	ready, stillPending := partitionPendingRelease(v.fsm(), pending, required, holdersWired)
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

func (v *vaultChunking) afterSealBuild(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest) {
	if pending == nil {
		return
	}
	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	v.mu.Lock()
	if v.donePostSeal == key {
		v.mu.Unlock()
		return
	}
	built := v.doneBuild == key
	v.mu.Unlock()
	// OnSealedManifestCleared can fire on follower homes before local GLCB
	// build finishes. Do not mark post-seal work done until doneBuild is set,
	// or finishBuildOnce cannot run afterSealBuild later (gastrolog-3vlse).
	if !built {
		return
	}
	v.mu.Lock()
	v.donePostSeal = key
	v.mu.Unlock()

	segmentIDs := releasableSegmentIDs(v.fsm(), pending)
	v.flushHeadPurgeForManifest(ctx, pending, segmentIDs)
	if v.cfg.IsLeader() {
		if len(segmentIDs) > 0 {
			v.mu.Lock()
			v.pendingRelease = appendUniqueGLIDs(v.pendingRelease, segmentIDs)
			v.mu.Unlock()
		}
		v.releaseWake.Notify()
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
	if len(pending.Refs) == 0 && pending.TotalRecords == 0 {
		return v.discardEmptySealedManifest(pending)
	}

	key := buildKey{chunkID: pending.ChunkID, sealedAt: pending.SealedAt}
	if done, err := v.buildOnceIfSealedElsewhere(ctx, pending, key); done || err != nil {
		return err
	}

	result, builtNow, err := v.runBuildOncePass(ctx, pending, key)
	if errors.Is(err, ErrAwaitingLocalSegments) {
		return nil
	}
	if err != nil || (!builtNow && v.cfg.Applier == nil) {
		return err
	}

	if err := v.proposeSealOnce(ctx, pending, key, result); err != nil {
		return err
	}

	v.fireOnBuiltOnce(pending, key, builtNow)
	v.finishBuildOnce(ctx, pending, key)
	return nil
}

// buildOnceIfSealedElsewhere handles the case where CmdSealChunk already
// applied cluster-wide. Returns true when no further build work is needed.
func (v *vaultChunking) buildOnceIfSealedElsewhere(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey) (bool, error) {
	entry := v.fsm().Get(pending.ChunkID)
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
	v.afterSealBuild(ctx, pending)
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
		if result, ok, err := v.adoptExistingGLCBIfPresent(pending, key); err != nil {
			return BuildResult{}, false, err
		} else if ok {
			return result, false, nil
		}
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
		if result, ok, err := v.adoptExistingGLCBIfPresent(pending, key); err != nil {
			return BuildResult{}, false, err
		} else if ok {
			return result, false, nil
		}
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

// adoptExistingGLCBIfPresent loads BuildResult when data.glcb is already on
// disk (BuildGLCBFile only renames into place after a complete build).
func (v *vaultChunking) adoptExistingGLCBIfPresent(pending *vaultctlfsm.OpenChunkManifest, key buildKey) (BuildResult, bool, error) {
	glcbPath := ChunkGLCBPath(v.cfg.ChunkRoot, pending.ChunkID)
	if _, err := os.Stat(glcbPath); err != nil {
		if os.IsNotExist(err) {
			return BuildResult{}, false, nil
		}
		return BuildResult{}, false, err
	}
	sealedAt := pending.SealedAt
	if sealedAt.IsZero() {
		if entry := v.fsm().Get(pending.ChunkID); entry != nil && !entry.WriteEnd.IsZero() {
			sealedAt = entry.WriteEnd
		}
	}
	result, readErr := BuildResultFromExistingGLCB(glcbPath, sealedAt)
	if readErr != nil {
		return BuildResult{}, false, nil //nolint:nilerr // corrupt GLCB; caller falls through to full rebuild
	}
	adoptKey := key
	if adoptKey.sealedAt.IsZero() {
		adoptKey.sealedAt = result.WriteEnd
	}
	v.mu.Lock()
	v.doneBuild = adoptKey
	v.lastBuild = struct {
		key    buildKey
		result BuildResult
		ok     bool
	}{key: adoptKey, result: result, ok: true}
	v.mu.Unlock()
	return result, true, nil
}

// clearSealProposedIfLeaderUncommitted drops a stale doneSealProposed marker
// when this home is now vault-ctl leader but CmdSealChunk never committed
// (e.g. leadership transferred after a follower build pass).
func (v *vaultChunking) clearSealProposedIfLeaderUncommitted(pending *vaultctlfsm.OpenChunkManifest, key buildKey) bool {
	if pending == nil || !v.cfg.IsLeader() || v.chunkSealCommitted(pending.ChunkID) {
		return false
	}
	v.mu.Lock()
	stale := v.doneSealProposed == key
	if stale {
		v.doneSealProposed = buildKey{}
		v.sealAttemptKey = buildKey{}
	}
	v.mu.Unlock()
	return stale
}

func (v *vaultChunking) proposeSealOnce(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey, result BuildResult) error {
	if v.cfg.Applier == nil {
		return nil
	}
	v.mu.Lock()
	alreadyProposed := v.doneSealProposed == key
	v.mu.Unlock()
	if alreadyProposed {
		if !v.clearSealProposedIfLeaderUncommitted(pending, key) {
			return nil
		}
	}
	// Only the vault-ctl leader commits CmdSealChunk. Follower homes
	// materialize the GLCB locally; proposing seal from a follower
	// forwards to the leader but local FSM verification races replication.
	if !v.cfg.IsLeader() {
		v.mu.Lock()
		v.doneSealProposed = key
		v.mu.Unlock()
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
		v.now(),
	)); err != nil {
		return err
	}
	if !v.chunkSealCommitted(pending.ChunkID) {
		return fmt.Errorf("chunking: CmdSealChunk did not commit seal for %s", pending.ChunkID)
	}
	v.mu.Lock()
	v.doneSealProposed = key
	v.mu.Unlock()
	v.afterSealBuild(ctx, pending)
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

func (v *vaultChunking) finishBuildOnce(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest, key buildKey) {
	v.mu.Lock()
	doneBuild := v.doneBuild == key
	postSeal := v.donePostSeal == key
	v.mu.Unlock()
	// Follower homes: CmdSealChunk often replicates before local build completes.
	// OnSealedManifestCleared is a no-op until doneBuild; finish the purge here.
	if doneBuild && !postSeal {
		if entry := v.fsm().Get(pending.ChunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
			v.afterSealBuild(ctx, pending)
			v.mu.Lock()
			postSeal = v.donePostSeal == key
			v.mu.Unlock()
		}
	}
	if postSeal {
		v.flushHeadPurgeForManifest(ctx, pending, releasableSegmentIDs(v.fsm(), pending))
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.doneBuild != key || v.pendingSeal == nil || v.pendingSeal.ChunkID != pending.ChunkID {
		return
	}
	// Retain pendingSeal until CmdSealChunk commits so
	// OnSealedManifestCleared can run afterSealBuild on follower homes.
	entry := v.fsm().Get(pending.ChunkID)
	if entry != nil && entry.State == chunk.ChunkStateSealed {
		v.pendingSeal = nil
	}
}

func (v *vaultChunking) discardEmptySealedManifest(pending *vaultctlfsm.OpenChunkManifest) error {
	if pending == nil || v.cfg.Applier == nil || !v.cfg.IsLeader() {
		return nil
	}
	return v.cfg.Applier.Apply(vaultctlfsm.MarshalDiscardOpenChunkManifest(pending.ChunkID))
}

func (v *vaultChunking) build(ctx context.Context, pending *vaultctlfsm.OpenChunkManifest) (BuildResult, error) {
	manifest := sealedManifestFromFSM(pending)
	if err := v.materializeManifestSegments(ctx, manifest); err != nil {
		return BuildResult{}, err
	}
	input := BuildInput{
		Manifest:  manifest,
		VaultID:   v.cfg.VaultID,
		ChunkRoot: v.cfg.ChunkRoot,
		Locate:    v.cfg.Locate,
	}
	return BuildSealedChunk(input)
}

// materializeManifestSegments ensures every segment referenced by a sealed
// manifest awaiting GLCB build is present under this home's head/ or
// completed/. Collection is the normal multi-home replication path — each
// placement member builds the GLCB locally from the same manifest refs.
func (v *vaultChunking) materializeManifestSegments(ctx context.Context, manifest SealedManifest) error {
	if v.cfg.Locate == nil {
		return nil
	}
	missing := missingManifestSegmentIDs(manifest, v.cfg.Locate)
	if len(missing) == 0 {
		return nil
	}
	if v.cfg.Collector == nil {
		if !v.cfg.IsLeader() {
			return ErrAwaitingLocalSegments
		}
		return &MissingSegmentsError{SegmentIDs: missing}
	}
	if err := v.cfg.Collector.CollectSegments(ctx, missing); err != nil {
		if !v.cfg.IsLeader() {
			return ErrAwaitingLocalSegments
		}
		return err
	}
	stillMissing := missingManifestSegmentIDs(manifest, v.cfg.Locate)
	if len(stillMissing) == 0 {
		return nil
	}
	if !v.cfg.IsLeader() {
		return ErrAwaitingLocalSegments
	}
	return &MissingSegmentsError{SegmentIDs: stillMissing}
}

// chunkSealCommitted reports whether CmdSealChunk took effect cluster-wide.
// After a forwarded apply the local FSM may lag briefly; a cleared pending
// sealed manifest is treated as success even before the entry shows Sealed.
func (v *vaultChunking) chunkSealCommitted(chunkID chunk.ChunkID) bool {
	if entry := v.fsm().Get(chunkID); entry != nil && entry.State == chunk.ChunkStateSealed {
		return true
	}
	sm := v.fsm().SealedManifest()
	return sm == nil || sm.ChunkID != chunkID
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
