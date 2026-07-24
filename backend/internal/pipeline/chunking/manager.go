package chunking

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/logging"
	"gastrolog/internal/notify"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrAlreadyRunning is returned when Run is called twice.
var ErrAlreadyRunning = errors.New("chunking manager already running")

// sealRetryInterval removed — seal/build retries wake on FSM events and
// vault-ctl leadership changes, not timed polls.

// VaultCtlApplier applies marshaled vault-ctl commands for one vault.
type VaultCtlApplier interface {
	Apply(data []byte) error
}

// SegmentCollector pulls segment bytes onto this home. Chunking invokes it as a
// build prerequisite (every manifest ref must be local before GLCB merge) and
// during planner catch-up when eligible registry segments are not yet in head/.
//
// Deliberately NO blocking full-pass method: the chunking worker must never
// wait on a collection pass. Under backlog a pass takes minutes-to-hours and
// the serial seal loop stalled at one chunk per pass (gastrolog-1b51yf).
// Collection wakes chunking on every pass completion (OnPassComplete), so a
// non-blocking Nudge is all the worker ever needs.
type SegmentCollector interface {
	// CollectSegments pulls the given segment IDs when manifest refs require
	// bytes this home does not yet hold.
	CollectSegments(ctx context.Context, segmentIDs []glid.GLID) error
	// Nudge wakes the collection worker for this vault without waiting.
	Nudge()
}

// VaultConfig is per-vault chunking execution state.
type VaultConfig struct {
	VaultID   glid.GLID
	VaultRoot string
	ChunkRoot string
	FSM       *vaultctlfsm.FSM
	// LookupFSM returns the live vault-ctl sub-FSM after snapshot Restore replaces
	// the object RegisterVault captured. Falls back to FSM when nil.
	LookupFSM  func() *vaultctlfsm.FSM
	Locate     SegmentLocator
	Collector  SegmentCollector
	Applier    VaultCtlApplier
	IsLeader   func() bool
	Policy     ManifestRotationPolicy
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
	// in each segment's holder set before the leader proposes ReleaseSegments
	// or a home purges its head/ copy, plus whether the placement lookup
	// resolved. Mandatory — registration rejects nil (gastrolog-4w1vt: a nil
	// field used to silently disable the holder gate). ok=false means the
	// lookup is unresolved (config load failure, vault missing from config):
	// every release and purge gate fails closed so a multi-home vault never
	// drops segments on a bad lookup. A vault that genuinely requires no
	// holders (single-node tests) opts out EXPLICITLY with NoRequiredHolders.
	RequiredHolders func() (required []string, ok bool)
	// RetentionGiveUpTTL returns the vault's delete-disposition retention TTL
	// (the shortest, when several rules apply) and whether a give-up bound is
	// in effect. A registry segment whose records out-age this TTL is released
	// as a COUNTED expiry even though it was never chunked (island origin, no
	// reachable holder): had the records been chunked, retention would already
	// have deleted them. Route-disposition retention vetoes the bound — those
	// records must be routed, not dropped. Nil disables (design-notes 28).
	RetentionGiveUpTTL func() (time.Duration, bool)
	// IndexOpener opens a completed segment for planner indexing. Defaults
	// to BuildOrderedIndex when nil (tests may inject a counting wrapper).
	IndexOpener func(path string) (*OrderedIndex, error)
	// Now overrides wall clock for MaxAge rotation on the open manifest.
	// Nil uses time.Now (production). Tests inject a fixed clock.
	Now func() time.Time
	// Alerts raises operator alerts for blocked GLCB builds (sealed manifest
	// referencing segments missing on this node — chunks pinned in Sealing,
	// gastrolog-67c9b0). Nil inherits the manager Config sink; both nil
	// disables alarms.
	Alerts alert.Sink
}

// NoRequiredHolders is the explicit no-holder-gate RequiredHolders source: it
// resolves to an empty requirement, so exhausted segments release immediately
// and head/ copies purge right after build. Vaults that genuinely need no
// holder gate — single-node tests without placements — opt out with THIS,
// never with a nil RequiredHolders: nil used to silently disable the gate and
// registration now rejects it (gastrolog-4w1vt).
func NoRequiredHolders() ([]string, bool) { return nil, true }

type vaultChunking struct {
	cfg VaultConfig

	// Stage-throughput counters (gastrolog-10n6k8): records/bytes this home
	// materialized into sealed GLCBs.
	sealedRecords atomic.Uint64
	sealedBytes   atomic.Uint64

	// Chunk-lifecycle stage counters (gastrolog-4r784a), monotonic per vault:
	//   chunksPlanned  — open manifests the leader opened (leader-owned).
	//   chunksBuilt    — GLCBs this home materialized (home-owned).
	//   chunksSealed   — CmdSealChunk commits the leader landed (leader-owned).
	//   segmentsReleased — segments the leader released from the registry.
	//   headPurged     — head/ segment copies purged (home-owned). A dedicated
	//     monotonic total; purgedReleased/purgedStale below are transient
	//     log-aggregation accumulators reset via Swap and cannot serve as a
	//     cumulative counter.
	chunksPlanned    atomic.Uint64
	chunksBuilt      atomic.Uint64
	chunksSealed     atomic.Uint64
	segmentsReleased atomic.Uint64
	headPurged       atomic.Uint64

	mu     sync.Mutex
	planMu sync.Mutex
	// segmentIndexCache holds open EventID indexes for segments the planner has
	// opened lazily. pruneSegmentIndexCache evicts released or exhausted entries.
	segmentIndexCache map[glid.GLID]*OrderedIndex
	// rewired, when non-nil, overrides cfg.FSM/LookupFSM/Applier after a
	// vault-ctl snapshot Restore. Atomic publication — workers read via
	// fsm()/applier() while RewireVaultFSM stores under Manager.mu
	// (gastrolog-50m2vi).
	rewired atomic.Pointer[chunkRewire]
	// progress is the exactly-once state machine for the sealed-manifest
	// build/seal/post-seal/OnBuilt lifecycle. Owns its own lock.
	progress sealProgress
	// Head-purge log aggregation: totals accumulate across passes and one
	// summary line emits per throttle interval instead of a line per pass.
	purgedReleased   atomic.Int64
	purgedStale      atomic.Int64
	purgeLogThrottle logging.Throttle

	// underReplicatedAlerted tracks the under-replicated-segments alert
	// state so planner passes log/alert only on transitions
	// (gastrolog-4bl9xx). Guarded by planMu like the planner pass itself.
	underReplicatedAlerted bool
	// planFailures tracks segments whose on-disk index cannot be opened or
	// read (corrupt index, unreadable file). Without it a corrupt segment
	// was skipped silently forever: never planned into a sealed manifest,
	// head purge blocked (gastrolog-6wwdos). Guarded by planMu like the
	// planner pass itself.
	planFailures map[glid.GLID]*planFailure
	// planFailureAlerted tracks the unplannable-segment alert state so
	// planner passes raise/clear only on transitions. Guarded by planMu.
	planFailureAlerted bool
	// corruptMu guards corruptGLCBs. Its own lock:
	// corruption is flagged from the build pass (under buildMu) and restart
	// recovery, and cleared from those plus the orchestrator's peer re-pull
	// (Manager.NoteGLCBRestored) — see glcb_corrupt.go (gastrolog-687m11).
	corruptMu sync.Mutex
	// corruptGLCBs maps chunks whose existing sealed GLCB was detected
	// unreadable (and quarantined) to the read-error detail, feeding the
	// corrupt-GLCB operator alert until every flagged chunk heals.
	corruptGLCBs map[chunk.ChunkID]string
	// pendingRelease holds segment IDs awaiting ReleaseSegments once every
	// required vault home has committed a holder receipt.
	pendingRelease []glid.GLID
	// pendingPurge holds released segment IDs queued by the wake-only
	// ReleaseSegments FSM callback; the worker's release branch drains it
	// (purging on the Raft apply goroutine deadlocked teardown).
	purgeMu      sync.Mutex
	pendingPurge []glid.GLID
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
	// buildMu serializes actual build passes. The worker's spawned build and
	// the Manager.BuildOnce test entry point must not run buildOnce
	// concurrently: the winner seals and purges head/ while the loser is
	// mid-merge reading those segment files (gastrolog-2qj3pw).
	buildMu sync.Mutex
	// buildWG tracks the in-flight build goroutine so worker shutdown waits
	// for it without polling.
	buildWG sync.WaitGroup
	// log is the per-vault logger; set when the worker starts.
	log *slog.Logger
}

func (v *vaultChunking) logger() *slog.Logger {
	if v.log != nil {
		return v.log
	}
	return slog.Default()
}

// chunkRewire is the rewireable collaborator bundle, published atomically by
// RewireVaultFSM after a group snapshot Restore. Field-by-field mutation of
// v.cfg raced live build/plan workers reading it (gastrolog-50m2vi); workers
// go through fsm()/applier() which Load one immutable snapshot.
type chunkRewire struct {
	fsm       *vaultctlfsm.FSM
	lookupFSM func() *vaultctlfsm.FSM
	applier   VaultCtlApplier
}

func (v *vaultChunking) fsm() *vaultctlfsm.FSM {
	if rw := v.rewired.Load(); rw != nil {
		if rw.lookupFSM != nil {
			if f := rw.lookupFSM(); f != nil {
				return f
			}
		}
		return rw.fsm
	}
	if v.cfg.LookupFSM != nil {
		if f := v.cfg.LookupFSM(); f != nil {
			return f
		}
	}
	return v.cfg.FSM
}

// applier returns the current vault-ctl applier, honoring a rewire.
func (v *vaultChunking) applier() VaultCtlApplier {
	if rw := v.rewired.Load(); rw != nil {
		return rw.applier
	}
	return v.cfg.Applier
}

// wiredFSM returns the FSM object callbacks are currently attached to (the
// last rewired object, NOT the LookupFSM indirection) — unwire must target
// the same object wire used.
func (v *vaultChunking) wiredFSM() *vaultctlfsm.FSM {
	if rw := v.rewired.Load(); rw != nil {
		return rw.fsm
	}
	return v.cfg.FSM
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
	if cfg.RequiredHolders == nil {
		return nil, errors.New("required-holders source required (NoRequiredHolders is the explicit no-holder-gate opt-out)")
	}
	if cfg.IsLeader == nil {
		cfg.IsLeader = func() bool { return false }
	}
	return &vaultChunking{
		cfg:               cfg,
		purgeLogThrottle:  logging.Throttle{Interval: retryLogInterval},
		wake:              notify.NewSignal(),
		releaseWake:       notify.NewSignal(),
		segmentIndexCache: make(map[glid.GLID]*OrderedIndex),
	}, nil
}

// Config configures a ChunkingManager.
type Config struct {
	Logger *slog.Logger
	// Alerts is the default alarm sink for vaults registered without one.
	Alerts alert.Sink
	// DeferWrites, when non-nil and returning true, pauses GLCB build
	// passes: builds create bytes and must stop while the node sheds disk
	// obligations (disk protect). Deferred work re-fires on the next wake
	// or plan tick once the pressure clears — under protect the last free
	// megabytes belong to the WAL, not to build attempts that ENOSPC-loop.
	DeferWrites func() bool
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

	// buildFailLog throttles the per-retry build-failure warn to one line
	// per vault per interval with a suppressed count (gastrolog-4elpu1).
	buildFailLog logging.Throttle
}

// New returns a chunking manager.
func New(cfg Config) *Manager {
	cfg.Logger = compChunking.Apply(logging.Default(cfg.Logger))
	return &Manager{
		cfg:          cfg,
		vaults:       make(map[glid.GLID]*vaultChunking),
		buildFailLog: logging.Throttle{Interval: retryLogInterval},
	}
}

// retryLogInterval spaces identical retry-failure warn lines per vault; the
// retry loop itself is unthrottled (gastrolog-4elpu1).
const retryLogInterval = 30 * time.Second

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
	if cfg.Alerts == nil {
		cfg.Alerts = m.cfg.Alerts
	}
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
//
// The FSM/LookupFSM/Applier trio is published as ONE atomic chunkRewire
// snapshot instead of mutating v.cfg under live build/plan workers, and
// Manager.mu is held across the body so concurrent rewires serialize
// (gastrolog-50m2vi).
func (m *Manager) RewireVaultFSM(vaultID glid.GLID, fsm *vaultctlfsm.FSM, applier VaultCtlApplier) error {
	if fsm == nil {
		return errors.New("vault-ctl FSM required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.vaults[vaultID]
	if !ok {
		return ErrUnknownVault
	}
	m.unwireVaultFSMCallbacks(v)
	// lookupFSM stays the registration-time live getter (cfg is immutable
	// now); when none was registered, fsm() falls back to rw.fsm directly.
	// The old code synthesized a closure capturing the first rewire's FSM,
	// which went stale on a second rewire.
	rw := &chunkRewire{fsm: fsm, lookupFSM: v.cfg.LookupFSM}
	if prev := v.rewired.Load(); prev != nil {
		rw.applier = prev.applier
	} else {
		rw.applier = v.cfg.Applier
	}
	if applier != nil {
		rw.applier = applier
	}
	v.rewired.Store(rw)
	cfg := v.cfg
	cfg.FSM = fsm
	m.wireVaultFSMCallbacks(v, cfg)
	if pending := fsm.SealedManifest(); pending != nil {
		v.progress.setPending(pending)
		// Hot FSM restore can leave the seal-proposed guard set from when this
		// home was a follower; clear so the vault-ctl leader can commit
		// CmdSealChunk.
		if entry := fsm.Get(pending.ChunkID); entry == nil || !entry.IsSealed() {
			v.progress.resetSealProposal()
		}
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
	fsm := v.wiredFSM()
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
		v.progress.setPending(m)
		v.wake.Notify()
	})
	v.unsubPublish = cfg.FSM.AddOnPublishCompletedSegment(func(vaultctlfsm.CompletedSegmentEntry) {
		v.wake.Notify()
	})
	v.unsubAckHolder = cfg.FSM.AddOnAckSegmentHolder(func(glid.GLID) {
		v.releaseWake.Notify()
	})
	v.unsubRelease = cfg.FSM.AddOnReleaseSegments(func(ids []glid.GLID) {
		// Wake-only, like every other FSM callback: this fires on the Raft
		// FSM-apply goroutine. Purging inline did disk I/O there and — via
		// purgeStaleHeadCatchUp → LookupFSM → GroupManager.GetGroup —
		// acquired the group-manager lock, which Shutdown holds while
		// waiting for this very apply goroutine to exit: a teardown
		// deadlock (gastrolog-38snf4 gate forensics). The worker's release
		// branch drains the queued IDs.
		v.purgeMu.Lock()
		v.pendingPurge = append(v.pendingPurge, ids...)
		v.purgeMu.Unlock()
		v.releaseWake.Notify()
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
		if pending := v.progress.noteSealCleared(); pending != nil {
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
// VaultSealStats is one vault's cumulative seal counters on this home
// (records/bytes materialized into sealed GLCBs) — gastrolog-10n6k8.
type VaultSealStats struct {
	VaultID       glid.GLID
	SealedRecords uint64
	SealedBytes   uint64
}

// SealStats returns per-vault cumulative seal counters.
func (m *Manager) SealStats() []VaultSealStats {
	m.mu.Lock()
	vaults := make(map[glid.GLID]*vaultChunking, len(m.vaults))
	maps.Copy(vaults, m.vaults)
	m.mu.Unlock()
	out := make([]VaultSealStats, 0, len(vaults))
	for vaultID, v := range vaults {
		out = append(out, VaultSealStats{
			VaultID:       vaultID,
			SealedRecords: v.sealedRecords.Load(),
			SealedBytes:   v.sealedBytes.Load(),
		})
	}
	slices.SortFunc(out, func(a, b VaultSealStats) int { return a.VaultID.Compare(b.VaultID) })
	return out
}

// VaultStageStats is one vault's cumulative chunk-lifecycle stage counters on
// this node (gastrolog-4r784a). Planned/Sealed/Released are leader-owned;
// Built and HeadPurges are home-owned. HeadPurges reuses the existing
// released+stale head-purge accumulators.
type VaultStageStats struct {
	VaultID          glid.GLID
	ChunksPlanned    uint64
	ChunksBuilt      uint64
	ChunksSealed     uint64
	SegmentsReleased uint64
	HeadPurges       uint64
}

// StageStats returns per-vault cumulative chunk-lifecycle stage counters.
func (m *Manager) StageStats() []VaultStageStats {
	m.mu.Lock()
	vaults := make(map[glid.GLID]*vaultChunking, len(m.vaults))
	maps.Copy(vaults, m.vaults)
	m.mu.Unlock()
	out := make([]VaultStageStats, 0, len(vaults))
	for vaultID, v := range vaults {
		out = append(out, VaultStageStats{
			VaultID:          vaultID,
			ChunksPlanned:    v.chunksPlanned.Load(),
			ChunksBuilt:      v.chunksBuilt.Load(),
			ChunksSealed:     v.chunksSealed.Load(),
			SegmentsReleased: v.segmentsReleased.Load(),
			HeadPurges:       v.headPurged.Load(),
		})
	}
	slices.SortFunc(out, func(a, b VaultStageStats) int { return a.VaultID.Compare(b.VaultID) })
	return out
}

func (m *Manager) NotifyVault(vaultID glid.GLID) {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if ok {
		v.wake.Notify()
	}
}

// BuildOnce runs one build pass for a vault (for tests). It serializes with
// the worker's in-flight build via buildMu: an unserialized foreground build
// races the background build+seal+head-purge and hits ENOENT mid-merge
// (gastrolog-2qj3pw).
func (m *Manager) BuildOnce(ctx context.Context, vaultID glid.GLID) error {
	m.mu.Lock()
	v, ok := m.vaults[vaultID]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownVault
	}
	v.buildMu.Lock()
	err := v.buildOnce(ctx)
	v.buildMu.Unlock()
	if err != nil {
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
		return ErrAlreadyRunning
	}

	m.mu.Lock()
	m.runCtx = ctx
	for _, v := range m.vaults {
		m.startWorkerLocked(v)
	}
	m.mu.Unlock()

	// Quiesce before waiting: clearing runCtx under m.mu guarantees no
	// registration can m.wg.Go a new worker after this point, so the Wait
	// below cannot race a concurrent Add — the WaitGroup misuse the race
	// detector flagged intermittently across the pipeline managers.
	<-ctx.Done()

	m.mu.Lock()
	m.runCtx = nil
	m.mu.Unlock()

	m.wg.Wait()
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
		// Recovery must not run before the vault-ctl FSM has replayed: at
		// process start the registry is briefly empty, and a recovery pass
		// against an empty FSM registers nothing and never re-runs — a node
		// once held 297 complete GLCBs its chunk manager knew nothing about
		// while retention, backfill, and queries silently starved. Retry on
		// a short ticker until the FSM is ready, then recover exactly once,
		// in this background worker — never on the serving path (startup
		// stays sub-3s per the vision).
		recoverTick := time.NewTicker(time.Second)
		defer recoverTick.Stop()
		recovered := false
		tryRecover := func() {
			if recovered {
				return
			}
			if f := v.fsm(); f != nil && !f.Ready() {
				return
			}
			recovered = true
			recoverTick.Stop()
			if err := v.recoverOnce(ctx); err != nil && ctx.Err() == nil {
				log.Warn("chunking recover failed", "error", err)
			}
		}
		tryRecover()
		v.drainReleasedPurge()
		v.purgeStaleHeadCatchUp()
		m.runBuildPass(ctx, v, log)
		releaseCh := v.releaseWake.C()
		for {
			select {
			case <-ctx.Done():
				v.buildWG.Wait()
				return
			case <-recoverTick.C:
				tryRecover()
				continue
			case <-releaseCh:
				if err := v.releaseOnce(ctx); err != nil && ctx.Err() == nil {
					log.Warn("chunking release failed", "error", err)
				}
				v.drainReleasedPurge()
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
	if m.cfg.DeferWrites != nil && m.cfg.DeferWrites() {
		return // disk protect: builds re-fire on the next wake/plan tick
	}
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
	v.buildWG.Go(func() {
		defer v.buildRunning.Store(false)
		v.buildMu.Lock()
		err := v.buildOnce(ctx)
		v.buildMu.Unlock()
		if err != nil && ctx.Err() == nil {
			// Retrying is correct; logging every retry is a firehose — a 6h
			// blocked build once emitted 244k of these (gastrolog-4elpu1).
			if n, ok := m.buildFailLog.Allow(v.cfg.VaultID.String()); ok {
				log.Warn("chunking build failed", "error", err, "suppressed", n)
			}
		}
		v.wake.Notify()
	})
}

func (v *vaultChunking) releaseOnce(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !v.cfg.IsLeader() || v.applier() == nil {
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
	required, resolved := v.requiredHolders()
	giveUpTTL, giveUpNow := v.giveUpBound()
	scan := v.fsm().SnapshotReleaseScan(v.plannerMinHolders())
	ready, stillPending := partitionPendingRelease(scan, pending, required, resolved, giveUpTTL, giveUpNow)
	if len(ready) == 0 {
		v.mu.Lock()
		v.pendingRelease = append(stillPending, v.pendingRelease...)
		v.mu.Unlock()
		return nil
	}
	if err := v.applier().Apply(vaultctlfsm.MarshalReleaseSegments(ready)); err != nil {
		v.mu.Lock()
		v.pendingRelease = append(pending, v.pendingRelease...)
		v.mu.Unlock()
		return err
	}
	// Leader-owned segment-release milestone (gastrolog-4r784a).
	v.segmentsReleased.Add(uint64(len(ready)))
	v.mu.Lock()
	v.pendingRelease = append(stillPending, v.pendingRelease...)
	v.mu.Unlock()
	return nil
}

// requiredHolders reads the live placement requirement. resolved=false means
// the lookup is unresolved and every release/purge gate must fail closed.
func (v *vaultChunking) requiredHolders() (required []string, resolved bool) {
	return v.cfg.RequiredHolders()
}

// ErrUnknownVault is returned for an unregistered vault.
var ErrUnknownVault = errors.New("unknown vault")
