// Package orchestrator coordinates ingestion, indexing, and querying
// without owning business logic. It filters records to chunk managers,
// schedules index builds on seal events, and delegates queries.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/locktrack"
	"gastrolog/internal/logging"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator/pipeline"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/collection"
	"gastrolog/internal/pipeline/digestion"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/pipeline/segmentation"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// IngesterStats tracks per-ingester metrics using atomic counters.
// Safe for concurrent reads (from API handlers) and writes (from ingest loop).
type IngesterStats struct {
	MessagesIngested atomic.Int64
	BytesIngested    atomic.Int64
	Errors           atomic.Int64
	Alive            atomic.Bool // true while Run() is executing, false during retry sleep
}

// RouteStats is a point-in-time snapshot of global routing counters sourced
// from the pipeline routing manager.
type RouteStats struct {
	Routed    int64 // total records that entered routing (matched + unmatched)
	Unmatched int64 // records that matched no route (intentional, counted drop)
	Matched   int64 // records that matched a route and were fanned out
}

// VaultRouteStats is a point-in-time snapshot of per-vault routing counters.
type VaultRouteStats struct {
	Matched int64 // records routed to this vault
}

// PerRouteStats is a point-in-time snapshot of per-route routing counters.
type PerRouteStats struct {
	Matched int64 // records matched by this route
}

// ingesterInfo holds metadata about an ingester for logging and reconcile
// diffing. The Ingester interface is a bare Run() — metadata lives alongside it.
type ingesterInfo struct {
	Name    string
	Type    string
	Passive bool // true for listener ingesters that should retry on failure
	// Params is the config-store parameter snapshot last used to build the
	// running instance. Compared on reconcile so param edits restart the
	// ingester without a disable/enable toggle.
	Params map[string]string
}

var (
	// ErrNoChunkManagers is returned when no chunk managers are registered.
	ErrNoChunkManagers = errors.New("no chunk managers registered")
	// ErrNoQueryEngines is returned when no query engines are registered.
	ErrNoQueryEngines = errors.New("no query engines registered")
	// ErrUnknownRegistry is returned when a registry key is not found.
	ErrUnknownRegistry = errors.New("unknown registry key")
	// ErrAlreadyRunning is returned when Start is called on a running orchestrator.
	ErrAlreadyRunning = errors.New("orchestrator already running")
	// ErrNotRunning is returned when Stop is called on a stopped orchestrator.
	ErrNotRunning = errors.New("orchestrator not running")
)

// drainState tracks an in-progress vault drain (migration to another node).
type drainState struct {
	TargetNodeID string
	JobID        string
	Cancel       context.CancelFunc
}

// ChunkReplicator sequences all replication commands for an instance on a single
// ordered stream per follower. Nil in single-node mode.
//
// Caller role: always invoked on the instance **leader** node. Each method sends
// a command to a follower (`nodeID`) that applies it locally. Callers must
// verify they hold leadership for (`vaultID`, `vaultID`) before invoking —
// the replicator itself does not re-check.
//
// Validation: methods assume the (`vaultID`, `vaultID`) pair is consistent
// (instance belongs to vault). The receiver on the remote node rejects mismatches
// via instance lookup; callers should not rely on the replicator to catch
// programmer errors.
//
// Readiness: the leader's own Vault.ReadinessErr gate fires upstream of
// these calls — by the time a replication command is sent, the local FSM
// has applied the commit that authorized it. The follower's readiness is
// its own concern; transient follower-not-ready errors are retried by
// higher-level catchup scheduling (see ScheduleCatchup).
type ChunkReplicator interface {
	ImportSealedChunk(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, next chunk.RecordIterator) error
	DeleteChunk(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID) error

	// RequestReplicaCatchup is the follower→leader inverse of the other
	// methods on this interface. Sent by a follower's lifecycle reconciler
	// after detecting sealed chunks in its FSM that are missing on its
	// local disk; the placement leader fans pushes out asynchronously via
	// existing replicateToFollower machinery. Returns the count of
	// pushes scheduled (after leader-side filtering). See gastrolog-2dgvj.
	RequestReplicaCatchup(ctx context.Context, leaderNodeID string, vaultID glid.GLID, chunkIDs []chunk.ChunkID, requesterNodeID string) (uint32, error)
}

// RemoteTransferrer sends records to a remote node for cross-node chunk
// migration. Synchronous and reliable — the caller blocks until the remote
// node confirms delivery.
type RemoteTransferrer interface {
	// TransferRecords streams records to a remote node, which imports them
	// as a new sealed chunk. Used by MoveChunk and DrainVault where
	// preserving chunk boundaries is desired.
	TransferRecords(ctx context.Context, nodeID string, vaultID glid.GLID, next chunk.RecordIterator) error

	// WaitVaultReady blocks until the vault is registered and accepting
	// records on the given node, or ctx expires. Used by DrainVault to
	// synchronize with the target node's AddVault before unregistering
	// the vault locally.
	WaitVaultReady(ctx context.Context, nodeID string, vaultID glid.GLID) error
}

// Orchestrator coordinates ingestion, indexing, and querying.
// It filters records to chunk managers, observes seal events to trigger
// index builds, and delegates queries to query engines.
//
// Orchestrator does not contain business logic - it only wires components.
//
// Concurrency model:
//   - Register* methods are expected to be called at startup only, before
//     any Ingest or Search calls. After setup, registries are effectively
//     read-only. This is enforced by convention, not by the type system.
//   - Ingest is serialized (one writer at a time) to support seal detection.
//   - Search methods can run concurrently with each other and with Ingest.
//   - A RWMutex protects registry access: Register* takes write lock,
//     Ingest and Search* take read lock.
//
// Ingester lifecycle:
//   - Ingesters are registered before Start() is called.
//   - Start() launches one goroutine per ingester plus an ingest loop.
//   - Stop() cancels all ingesters and the ingest loop via context.
//   - Ingesters emit IngestMessages; orchestrator resolves identity and filters.
//
// Filtering:
//   - Each vault has a filter expression that determines which messages it receives.
//   - Filters are compiled at registration time and evaluated against message attrs.
//   - Special filters: "*" (catch-all), "+" (catch-the-rest), "" (receives nothing).
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Orchestrator owns its scoped logger (component="orchestrator")
//   - Subcomponents receive child loggers with additional context
//   - Logging is intentionally sparse; only lifecycle events are logged
type Orchestrator struct {
	// mu guards the registries below. locktrack.RWMutex is a drop-in
	// sync.RWMutex that, when tracking is on (default; GLOG_LOCK_TRACKING=off
	// disables), records acquisition stacks so an orphaned hold or a stuck
	// write waiter is reported with its exact acquisition site — a node-wide
	// deadlock from a leaked read lock was undiagnosable from goroutine
	// dumps alone (gastrolog-1ug3rq).
	mu locktrack.RWMutex

	// backfillLogThrottle spaces cloud-backfill failure logging per vault;
	// the sweep retries failing chunks every few seconds indefinitely.
	backfillLogThrottle logging.Throttle
	// registerSkipLog spaces skipped-GLCB-registration warnings per vault.
	registerSkipLog logging.Throttle
	// retentionLeaderlessLog spaces "vault-ctl raft group has no leader"
	// warnings per vault (gastrolog-1xl29s case 1). Distinct from the
	// vault-leaderless ALARM (leaderless_alarm.go), which tracks config
	// PLACEMENT leader resolution: this is the vault-ctl Raft group's own
	// election state (hasLeader callback == r.Leader() != "", see
	// buildVaultRaftCallbacks in reconfig_vaults.go), which can be
	// momentarily unelected even when placement resolves cleanly. See
	// retentionTargetForInstance.
	retentionLeaderlessLog logging.Throttle

	// backfillMu guards backfillFailures — a dedicated lock (not o.mu) since
	// backfillCloudUploads runs under o.mu.RLock() while the scheduler job
	// that populates this map runs later, off that lock, asynchronously.
	backfillMu sync.Mutex
	// backfillFailures tracks per-chunk retry backoff for cloud-backfill
	// uploads that failed and were not resolved by registration repair
	// (gastrolog-4ryguo). In-memory only, mirroring retention's
	// unreadableEntry: a restart clears it and the first sweep re-establishes
	// state from scratch, which is fine — the point is to stop hammering a
	// chunk that keeps failing, not to remember why forever.
	backfillFailures map[chunk.ChunkID]*backfillFailureEntry

	// Vault registry. Each vault bundles Chunks, Indexes, and Query.
	vaults map[glid.GLID]*Vault

	// Ingester management.
	ingesters        map[glid.GLID]Ingester
	ingesterStats    map[glid.GLID]*IngesterStats     // per-ingester metrics
	ingesterMeta     map[glid.GLID]ingesterInfo       // per-ingester name/type for logging
	ingesterAdapters map[glid.GLID]ingestion.Ingester // stable pipeline adapters (no-flap reconcile identity)

	// pipeline is the ingest pipeline supervisor: it owns the durable write path
	// (ingest→digest→route→segment→distribute). The orchestrator drives its
	// routing table, Origin vault registrations, and ingester reconcile, and
	// starts/stops it alongside itself.
	pipeline *pipeline.Supervisor
	// pipelineGate is the ingest-pipeline pressure signal injected into
	// PressureAware ingesters by the supervisor's ingestion manager.
	pipelineGate *chanwatch.PressureGate
	// segmentsDir is the base directory for per-vault segment roots
	// (OriginRoot = segmentsDir/<vaultID>).
	segmentsDir string
	// homeDir is the gastrolog home directory (stores, segments, raft, …).
	homeDir string
	// pipelineVaults tracks which vaults are currently registered in the pipeline
	// supervisor and whether each is registered as a Home (collection) on this
	// node, so a route/placement reload registers/unregisters/re-registers only
	// the delta. Guarded by o.mu.
	pipelineVaults map[glid.GLID]pipelineVaultReg

	// segmentPuller streams completed segments from peer holders for
	// collection. Set from factories during ApplyConfig (cluster mode only).
	segmentPuller *cluster.SegmentPuller
	// chunkGLCBPuller streams sealed chunk GLCBs from peer homes for
	// replica catch-up — a home that missed builds (wedged, down) whose
	// source segments were already released has no other recovery path.
	chunkGLCBPuller *cluster.ChunkGLCBPuller
	// glcbPullInflight claims per-chunk pulls so concurrent sweeps never
	// pull the same GLCB twice. Guarded by glcbPullMu.
	glcbPullMu       sync.Mutex
	glcbPullInflight map[chunk.ChunkID]bool

	// leaderlessReported is the set of vaults reported leaderless to the
	// alarm collector last sweep tick, so departures diff to a Clear. The
	// delay-on window itself is the collector's (catalog DelayOn).
	// Guarded by leaderlessMu.
	leaderlessMu       sync.Mutex
	leaderlessReported map[glid.GLID]struct{}

	// diskGuard samples free space and drives the disk-space alarm plus
	// protect mode (ingest admission suspended below the floor).
	diskGuard *diskGuard

	// remoteVaultDiskProtected consults peer NodeStats broadcasts for vaults
	// under disk protect on OTHER nodes, so this node's per-vault admission
	// gate is cluster-consistent. Installed via SetRemoteVaultDiskProtected.
	remoteVaultDiskProtected atomic.Pointer[func(glid.GLID) bool]

	// remoteVaultSizeCapped is the same peer lookup for vaults at their
	// max-size budget elsewhere. Installed via SetRemoteVaultSizeCapped.
	remoteVaultSizeCapped atomic.Pointer[func(glid.GLID) bool]

	// Remote transferrer for cross-node chunk migration (nil in single-node mode).
	transferrer RemoteTransferrer

	// Vault replicator: ordered stream per instance per follower (nil in single-node mode).
	chunkReplicator ChunkReplicator

	// groupMgr is the shared multi-group Raft manager (system, vault ctl, …).
	// Set from factories during ApplyConfig; used to tear down vault ctl groups.
	groupMgr *raftgroup.GroupManager

	// peerConns is the shared gRPC pool for cluster peers. Set from factories
	// during ApplyConfig; used by ApplyVaultControlPlane forwarding.
	peerConns *cluster.PeerConnManager

	// vaultCtlPipelineChunkEvents tracks vaults whose vault-ctl FSM already has
	// pipeline manifest → chunk-bus wiring (AddOn* subscribers).
	vaultCtlPipelineChunkEvents sync.Map

	// pipelineChunkBoundsCache memoizes ingest bounds overlaid from GLCB/manifest
	// for sealing chunks. Search lists every FSM entry per query; without this
	// each list re-reads data.glcb for entries whose FSM bounds are still empty.
	pipelineChunkBoundsCache sync.Map // chunk.ChunkID → pipelineChunkBoundsOverlay

	// Pipeline lifecycle. The durable write path lives in the pipeline supervisor
	// (o.pipeline); cancel stops the orchestrator's aux goroutines.
	cancel context.CancelFunc
	// running is an atomic so IsRunning() — read by the /readyz HTTP
	// handler on every probe — never blocks on o.mu. Holding it as a
	// plain bool guarded by o.mu reintroduced the original gastrolog-5n6xz
	// freeze through the back door: kubelet's probe took o.mu.RLock,
	// which starved behind any long-held o.mu.Lock writer regardless of
	// the cached replication-ready flag. Start/Stop use CompareAndSwap
	// to preserve the prior check-then-set mutual exclusion.
	running atomic.Bool
	auxWg   sync.WaitGroup // tracks auxiliary goroutines (rate alerts, progress emitters, etc.)

	// Per-instance import mutex for serializing SetNextChunkID + ImportRecords.
	importMu sync.Map // vaultID → *sync.Mutex

	// Draining vaults (keyed by vault ID, tracks in-progress migrations).
	draining map[glid.GLID]*drainState

	// In-progress instance drains, keyed by vault ID.
	vaultDraining map[string]*vaultDrainState

	// Retention runners (keyed by vaultID:storageID, invoked by the shared scheduler).
	retention map[string]*retentionRunner

	// Shared scheduler for all periodic tasks (retention, placement reconcile, etc.).
	scheduler *Scheduler

	// Per-instance retention rate alerter that surfaces pathological retention
	// configurations as operator-visible alerts. See gastrolog-47qyw.
	// Initialized in New() and evaluated by a periodic goroutine in Start().
	retentionRates *RateAlerter

	// Orchestrator-owned per-vault pipeline stage counters (GLCB catch-up
	// pulls, retention deletes) surfaced as first-class throughput metrics
	// (gastrolog-4r784a).
	stageEvents *stageEventCounters

	// Clock for testing.
	now func() time.Time

	// Config loader for hot-update operations.
	sysLoader SystemLoader

	// Local node identity for multi-node filtering.
	localNodeID string
	// localNodeIDGLID is the parsed GLID form of localNodeID, pre-computed at
	// construction. Supplied to the pipeline supervisor as the node ID used by the
	// ingestion minter to stamp EventID.NodeID.
	// Empty (zero GLID) for memory-config / no-node-id orchestrators.
	localNodeIDGLID glid.GLID

	// Alarm sink for runtime alarms.
	alerts alert.Sink

	// chunkSignal is the legacy bare-wake-up notifier used by the
	// pre-gastrolog-3pf9w WatchChunks shape. New code should emit typed
	// ChunkChangeEvent via chunkBus instead; chunkSignal stays wired so
	// any caller that hasn't been migrated still produces a wake-up tick
	// during the transition.
	chunkSignal *notify.Signal

	// chunkBus broadcasts typed ChunkChangeEvent values to subscribers.
	// Replaces the chunkSignal-then-fan-out-refetch pattern: WatchChunks
	// streams these events directly to clients so they can patch their
	// cache via setQueryData instead of refetching the world. See
	// gastrolog-3pf9w.
	chunkBus *notify.Bus[ChunkChangeEvent]

	// progressTrigger coalesces high-rate active-chunk-progress signals
	// (every record append) into bounded chunkSignal notifications. Hot-
	// path callers do progressTrigger.Signal(); a single throttle
	// goroutine fan-outs to chunkSignal at most once per window with
	// leading-edge fire on the first signal after quiet. See
	// gastrolog-4y03v.
	progressTrigger *progressNotifier

	// Suspect tracker for cloud-backed chunks that returned 404.
	suspects *suspectTracker

	// Per-vault leader loop for vault control-plane Raft (replicated instance
	// chunk metadata when multiraft is enabled). Membership reconciliation
	// runs on the vault ctl Raft leader inside its leader epoch.
	vaultCtlLeaders *vaultCtlLeaderManager

	// ctlRestorePending coalesces deferred after-vault-ctl-restore passes
	// (see scheduleAfterVaultCtlRestore). Keyed by vault ID.
	ctlRestorePending sync.Map

	// pendingPipelineCtlRestore holds vault IDs whose vault-ctl FSM restored
	// before pipeline chunking registered on this home. finishPendingPipelineCtlRestore
	// runs rewire+recover when RegisterVault completes (startup ordering).
	pendingPipelineCtlRestore sync.Map

	// catchupPushInFlight tracks async replica-catchup push batches keyed by
	// (vault, requester). Prevents SweepMissingReplicas from stacking
	// hundreds of overlapping CatchupSelectedChunks goroutines on the same
	// sender→requester stream (gastrolog-2o9e9).
	catchupPushInFlight sync.Map // catchupPushKey → struct{}

	// cachedReplicationReady mirrors liveReplicationReady, updated by the
	// readiness refresher goroutine (~500 ms cadence). LocalVaultsReplicationReady
	// reads this atomic so the /readyz HTTP handler stays responsive even
	// when o.mu is contended by a vault-ctl AddVoter burst or other
	// long-held write lock. See gastrolog-5n6xz. Seeded true in New so
	// orchestrators that have not yet started still report ready when their
	// vault map is empty (matches legacy semantics).
	cachedReplicationReady atomic.Bool

	// Shutdown phase (nil in tests / single-node setups without a
	// Phase wired). When non-nil, hot-path replication helpers like
	// fireAndForgetRemote and sealRemoteFollowers consult
	// phase.ShuttingDown() and skip the remote call during drain, so
	// the orchestrator's pipeline flush does not spam the log with
	// "connection refused" warnings for peers that are going down
	// alongside this node. See gastrolog-1e5ke.
	phase *lifecycle.Phase

	// onIngesterAlive is called when an ingester's alive state changes.
	onIngesterAlive func(ingesterID glid.GLID, alive bool)

	// onIngesterCheckpoint is called when a Checkpointable ingester saves state.
	onIngesterCheckpoint func(ingesterID glid.GLID, data []byte)

	// Logger for this orchestrator instance.
	// Scoped with component="orchestrator" at construction time.
	logger *slog.Logger

	// baseLogger is the unscoped logger from which every subsystem-scoped
	// logger is derived. Storing it separately avoids the
	// double-component-attr problem that would arise if subsystems
	// re-Applied on the orchestrator-scoped logger.
	baseLogger *slog.Logger

	// Per-subsystem scoped loggers (gastrolog-3flfp 3.5). Derived once
	// at construction so log calls don't pay the per-call .With cost,
	// and so each subsystem's emissions carry component=orchestrator.<sub>
	// for fine-grained filter targeting.
	replicationLogger   *slog.Logger
	drainLogger         *slog.Logger
	retentionLogger     *slog.Logger
	rotationLogger      *slog.Logger
	schedulerLogger     *slog.Logger
	vaultOpsLogger      *slog.Logger
	cacheEvictionLogger *slog.Logger
	cloudHealthLogger   *slog.Logger
}

// shuttingDown reports whether the orchestrator has been told to drain.
// Returns false when the phase is nil (tests / harnesses without a
// wired phase), preserving the pre-gastrolog-1e5ke behaviour.
func (o *Orchestrator) shuttingDown() bool {
	return o.phase != nil && o.phase.ShuttingDown()
}

// parseNodeGLID decodes the node identity string into a GLID for use in
// record-level EventID. Empty or unparseable input yields the zero GLID —
// memory-config tests and ad-hoc constructions use this path.
func parseNodeGLID(id string) glid.GLID {
	if id == "" {
		return glid.GLID{}
	}
	g, err := glid.ParseAny(id)
	if err != nil {
		return glid.GLID{}
	}
	return g
}

// ChunkSignal returns the legacy bare-wake-up signal. New code should
// subscribe to ChunkBus for typed ChunkChangeEvent values; ChunkSignal
// stays wired only for callers that haven't been migrated to typed
// emission yet.
func (o *Orchestrator) ChunkSignal() *notify.Signal {
	return o.chunkSignal
}

// ChunkBus returns the typed chunk-event bus. The WatchChunks streaming
// handler subscribes here and translates each ChunkChangeEvent into a
// proto WatchChunksResponse for connected clients. See gastrolog-3pf9w.
func (o *Orchestrator) ChunkBus() *notify.Bus[ChunkChangeEvent] {
	return o.chunkBus
}

// EmitChunkChange broadcasts a typed chunk-state event on the chunk bus
// AND triggers the legacy bare-signal pathway for any pre-3pf9w
// subscribers. Call sites should use the typed Emit* helpers below
// rather than constructing events directly so the Op semantics stay
// consistent.
func (o *Orchestrator) EmitChunkChange(ev ChunkChangeEvent) {
	if o.chunkBus != nil {
		o.chunkBus.Emit(ev)
	}
	// Legacy wake-up — keeps the old WatchChunks shape working during
	// the transition. Safe to remove once all subscribers have migrated
	// to the typed bus.
	o.progressTrigger.Signal()
}

// EmitChunkCreated emits a CREATED event with full post-open metadata.
func (o *Orchestrator) EmitChunkCreated(vault glid.GLID, meta chunk.ChunkMeta) {
	o.logChunkCreated(vault, meta.ID)
	m := meta
	o.EmitChunkChange(ChunkChangeEvent{
		VaultID: vault, ChunkID: meta.ID, Op: ChunkChangeOpCreated, Meta: &m,
	})
}

// EmitChunkProgress emits a PROGRESS event carrying the active chunk's
// current state — recordCount, WriteEnd, IngestEnd, Bytes, etc.
// Frontends use mergeMeta to overlay these onto the cache: subscribers
// see WriteEnd/IngestEnd advance and Bytes grow each tick, in addition
// to the running record count. Producer paths must coalesce
// (runChunkProgressEmitter) so emission stays bounded.
func (o *Orchestrator) EmitChunkProgress(vault glid.GLID, meta chunk.ChunkMeta) {
	m := meta
	o.EmitChunkChange(ChunkChangeEvent{
		VaultID:     vault,
		ChunkID:     meta.ID,
		Op:          ChunkChangeOpProgress,
		Meta:        &m,
		RecordCount: uint64(meta.RecordCount), //nolint:gosec // G115: record count bounded by rotation policy
	})
}

// EmitChunkSealing marks a pipeline chunk entering the sealing phase after
// SealOpenChunkManifest (rotation triggered, GLCB build pending). Logs
// "chunk sealed" for operators and emits PROGRESS with sealing-state meta
// so WatchChunks clients advance active → sealing without a spurious CREATED.
func (o *Orchestrator) EmitChunkSealing(vault glid.GLID, meta chunk.ChunkMeta) {
	o.logChunkSealed(vault, meta.ID)
	m := meta
	o.EmitChunkChange(ChunkChangeEvent{
		VaultID:     vault,
		ChunkID:     meta.ID,
		Op:          ChunkChangeOpProgress,
		Meta:        &m,
		RecordCount: uint64(meta.RecordCount), //nolint:gosec // G115: record count bounded by rotation policy
	})
}

// EmitChunkSealed emits a SEALED event with the final post-seal metadata.
func (o *Orchestrator) EmitChunkSealed(vault glid.GLID, meta chunk.ChunkMeta) {
	m := meta
	o.EmitChunkChange(ChunkChangeEvent{
		VaultID: vault, ChunkID: meta.ID, Op: ChunkChangeOpSealed, Meta: &m,
	})
}

// EmitChunkDeleted emits a DELETED event. Subscribers drop the entry
// from their projection; no Meta is carried.
func (o *Orchestrator) EmitChunkDeleted(vault glid.GLID, chunkID chunk.ChunkID) {
	o.EmitChunkChange(ChunkChangeEvent{
		VaultID: vault, ChunkID: chunkID, Op: ChunkChangeOpDeleted,
	})
}

// EmitChunkUploaded emits an UPLOADED event with the post-upload metadata
// (CloudBacked=true, DiskBytes updated to the local-cache footprint).
func (o *Orchestrator) EmitChunkUploaded(vault glid.GLID, meta chunk.ChunkMeta) {
	m := meta
	o.EmitChunkChange(ChunkChangeEvent{
		VaultID: vault, ChunkID: meta.ID, Op: ChunkChangeOpUploaded, Meta: &m,
	})
}

// NotifyChunkChange is the legacy bare-signal entry point. Retained for
// call sites that haven't been migrated to the typed Emit* helpers yet.
// Prefer EmitChunk{Created,Progress,Sealed,Deleted,Uploaded} for any
// new code — the typed events carry enough information for subscribers
// to patch local state directly without an expensive ListChunks
// refetch. See gastrolog-3pf9w.
func (o *Orchestrator) NotifyChunkChange() {
	o.progressTrigger.Signal()
}

// ChunkResidency returns the cluster-wide node IDs that currently hold
// a chunk's bytes, computed authoritatively from the vault's vault-ctl
// FSM. Works for any vault this node participates in as a vault-ctl
// voter, regardless of whether the local node has a storage placement
// for that vault (per gastrolog-292yi every cluster node is a voter in
// every vault-ctl Raft group).
//
// Used by the WatchChunks event-relay path and the ListChunks overlay
// to stamp authoritative replica info on outbound chunk metas so clients
// see correct counts without reconstructing them from per-node event
// evidence. See gastrolog-66vmg; single-semantic (holder receipts only)
// per gastrolog-68wsli.
//
// Returns nil if the local node has no FSM for the vault (single-node /
// memory mode), or if the chunk is unknown to the FSM. Callers in that
// case should leave replica info absent from the outbound event so the
// client preserves its existing cache value via mergeMeta. Empty means
// the chunk is known with zero verified copies (no receipts yet).
func (o *Orchestrator) ChunkResidency(vaultID glid.GLID, chunkID chunk.ChunkID) []string {
	if o.groupMgr == nil {
		return nil
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil {
		return nil
	}
	var fsm *vaultctlfsm.FSM
	switch raw := g.FSM.(type) {
	case *vaultctlfsm.FSM:
		fsm = raw
	case *vaultraft.FSM:
		fsm = raw.EnsureVaultFSM(vaultID)
	}
	if fsm == nil {
		return nil
	}
	return fsm.ChunkResidency(chunkID)
}

// vaultLabel returns the operator-friendly name for an instance as configured,
// or "" if the instance or config is unknown. Used by RateAlerter to build
// alert messages that say "instance ssd-hot" instead of just a UUID. Safe to
// call from any goroutine — it acquires the orchestrator read lock.
func (o *Orchestrator) vaultLabel(vaultID glid.GLID) string {
	if o.sysLoader == nil {
		return ""
	}
	sys, err := o.sysLoader.Load(context.Background())
	if err != nil || sys == nil {
		return ""
	}
	for _, v := range sys.Config.Vaults {
		if v.ID == vaultID {
			return v.Name
		}
	}
	return ""
}

// nodeLabel returns the operator-friendly name for a cluster node ID, or ""
// when unknown. Safe to call from any goroutine.
func (o *Orchestrator) nodeLabel(nodeID string) string {
	if nodeID == "" || o.sysLoader == nil {
		return ""
	}
	sys, err := o.sysLoader.Load(context.Background())
	if err != nil || sys == nil {
		return ""
	}
	for _, n := range sys.Runtime.Nodes {
		if n.ID.String() == nodeID {
			return n.Name
		}
	}
	return ""
}

// SystemLoader provides read access to the full system state.
// The orchestrator uses this during hot-update operations (ReloadFilters,
// ReloadRotationPolicies, etc.) to resolve references like filter IDs
// and policy IDs without the server having to mediate.
//
// system.Store satisfies this interface.
type SystemLoader interface {
	Load(ctx context.Context) (*system.System, error)
}

// Config configures an Orchestrator.
type Config struct {
	// IngestChannelSize is the buffer size for the ingestion→digestion queue
	// (minted messages awaiting digestion). Defaults to 1000 if not set.
	IngestChannelSize int

	// MaxConcurrentJobs limits how many scheduler jobs (index builds,
	// cron rotation, retention) can run in parallel. Defaults to 4.
	MaxConcurrentJobs int

	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time

	// Logger for structured logging. If nil, logging is disabled.
	// The orchestrator scopes this logger with component="orchestrator".
	Logger *slog.Logger

	// SystemLoader provides read access to the full configuration.
	// If set, the orchestrator can reload config internally during
	// hot-update operations (ReloadFilters, AddVault, etc.).
	// If nil, hot-update methods that require config will return an error.
	SystemLoader SystemLoader

	// LocalNodeID is the raft server ID of this node. Used to filter
	// vaults and ingesters during ApplyConfig — only entities assigned
	// to this node (or with empty NodeID) are instantiated.
	LocalNodeID string

	// Alerts is an optional sink for runtime alarms. Components call
	// Raise when they detect a cataloged condition and Clear when it
	// resolves; priority comes from the alarm catalog, never the caller.
	Alerts alert.Sink

	// OnIngesterAlive is called when an ingester's alive state changes.
	// The app layer wires this to Raft to replicate the state cluster-wide.
	OnIngesterAlive func(ingesterID glid.GLID, alive bool)

	// OnIngesterCheckpoint is called when a Checkpointable ingester saves state.
	// The app layer wires this to Raft to replicate checkpoints cluster-wide.
	OnIngesterCheckpoint func(ingesterID glid.GLID, data []byte)

	// IngesterRetryDelay overrides the pause before an ingester run is
	// retried (any passive listener exit, or a non-passive run that returned
	// an error). consecutiveFailures counts error exits since the last clean
	// run. Nil uses the ingestion manager's default jittered exponential
	// backoff (3–5s first retry, doubling to a 5m cap); tests inject a short
	// delay to observe retries without wall-clock waits.
	IngesterRetryDelay func(consecutiveFailures int) time.Duration

	// Digesters run in order on each ingestion message before the record is
	// built. The app supplies the level/timestamp enrichers here.
	Digesters []digestion.Digester

	// DiskGuardPaths are filesystem paths whose volumes the disk-space
	// guard samples (node home for the Raft WAL, segments dir for the
	// pipeline). Empty disables the guard (tests, memory-only setups).
	DiskGuardPaths []string

	// SegmentsDir is the base directory under which each origin vault's
	// segmentation working/ and completed/ areas live: a vault's segment
	// OriginRoot is SegmentsDir/<vaultID>. Set from home.Dir.SegmentsDir() at
	// process startup; tests may point it at t.TempDir(). There is no default
	// under $TMPDIR or the working directory.
	SegmentsDir string

	// Phase is the shared shutdown signal. When non-nil, the orchestrator
	// consults phase.ShuttingDown() in hot-path replication helpers so that
	// during the drain window (after BeginShutdown) remote forwards no-op
	// instead of spamming "connection refused" against peers that are
	// shutting down alongside this node. See gastrolog-1e5ke.
	Phase *lifecycle.Phase

	// SegmentCompletePolicy controls when a vault's working segment is completed —
	// finalized, renamed to completed/, and published for collection/chunking. A zero
	// value selects the production defaults (defaultSegmentCompleteMaxBytes /
	// defaultSegmentCompleteMaxAge). Without a complete policy the pipeline never
	// completes a segment, so records would stay in working/ forever and
	// never reach a sealed GLCB (gastrolog-18f9r, Rubicon E3).
	SegmentCompletePolicy segmentation.CompletePolicy

	// SegmentDisableFsync skips fsync on segmentation group-commit flushes
	// (dev/load testing only). Segment completion still syncs before rename.
	// Wired from --segment-hot-path-fsync / GLOG_SEGMENT_HOT_PATH_FSYNC.
	SegmentDisableFsync bool
}

// Production segment complete defaults: a segment completes once it reaches
// 8 MiB or 10 seconds of age, whichever comes first. MaxAge bounds the
// ingest→queryable latency on low-throughput vaults (age is evaluated on
// each commit, so a trickle still completes its segment on the next record
// after the window); MaxBytes bounds segment size under load.
const (
	defaultSegmentCompleteMaxBytes = uint64(8 << 20)
	defaultSegmentCompleteMaxAge   = 10 * time.Second
)

// New creates an Orchestrator with empty registries.
func New(cfg Config) (*Orchestrator, error) {
	if cfg.IngestChannelSize <= 0 {
		cfg.IngestChannelSize = 1000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SegmentCompletePolicy == (segmentation.CompletePolicy{}) {
		cfg.SegmentCompletePolicy = segmentation.CompletePolicy{
			MaxBytes: defaultSegmentCompleteMaxBytes,
			MaxAge:   defaultSegmentCompleteMaxAge,
		}
	}

	// Scope logger with component identity. Each subsystem-scoped
	// logger is derived from the unscoped base, NOT from the orchestrator
	// root — chaining .Apply twice would emit two "component" attributes,
	// and while the filter handler picks the last one, the text output
	// would carry both.
	baseLogger := logging.Default(cfg.Logger)
	logger := compOrchestrator.Apply(baseLogger)

	sched, err := newScheduler(logger, cfg.MaxConcurrentJobs, cfg.Now)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	o := &Orchestrator{
		backfillLogThrottle:    logging.Throttle{Interval: 30 * time.Second},
		retentionLeaderlessLog: logging.Throttle{Interval: 10 * time.Minute},
		vaults:                 make(map[glid.GLID]*Vault),
		ingesters:              make(map[glid.GLID]Ingester),
		ingesterStats:          make(map[glid.GLID]*IngesterStats),
		ingesterMeta:           make(map[glid.GLID]ingesterInfo),
		ingesterAdapters:       make(map[glid.GLID]ingestion.Ingester),
		draining:               make(map[glid.GLID]*drainState),
		vaultDraining:          make(map[string]*vaultDrainState),
		retention:              make(map[string]*retentionRunner),
		scheduler:              sched,
		sysLoader:              cfg.SystemLoader,
		localNodeID:            cfg.LocalNodeID,
		localNodeIDGLID:        parseNodeGLID(cfg.LocalNodeID),
		alerts:                 cfg.Alerts,
		suspects:               newSuspectTracker(),
		chunkSignal:            notify.NewSignal(),
		chunkBus:               notify.NewBus[ChunkChangeEvent](256),
		progressTrigger:        newProgressNotifier(),
		vaultCtlLeaders:        newVaultCtlLeaderManager(baseLogger),
		phase:                  cfg.Phase,
		onIngesterAlive:        cfg.OnIngesterAlive,
		onIngesterCheckpoint:   cfg.OnIngesterCheckpoint,
		segmentsDir:            cfg.SegmentsDir,
		diskGuard:              newDiskGuardWithLogger(cfg.DiskGuardPaths, cfg.Logger),
		homeDir:                homeDirFromSegments(cfg.SegmentsDir),
		pipelineVaults:         make(map[glid.GLID]pipelineVaultReg),
		now:                    cfg.Now,
		logger:                 logger,
		baseLogger:             baseLogger,
		replicationLogger:      compReplication.Apply(baseLogger),
		drainLogger:            compDrain.Apply(baseLogger),
		retentionLogger:        compRetention.Apply(baseLogger),
		rotationLogger:         compRotation.Apply(baseLogger),
		schedulerLogger:        compScheduler.Apply(baseLogger),
		vaultOpsLogger:         compVaultOps.Apply(baseLogger),
		cacheEvictionLogger:    compCacheEviction.Apply(baseLogger),
		cloudHealthLogger:      compCloudHealth.Apply(baseLogger),
	}

	// The max-size budget measures the vault's whole local disk claim.
	o.diskGuard.vaultFootprint = o.localVaultFootprintBytes
	// The backlog budget measures the vault-ctl registry (cluster-wide truth).
	o.diskGuard.vaultBacklogBytes = o.vaultRegistryBacklogBytes

	// ingest pipeline. The supervisor owns the durable write path; the
	// orchestrator publishes its routing table, registers Origin vaults, and
	// reconciles ingesters into it (see pipeline.go). The pressure gate is injected
	// into PressureAware ingesters; the orchestrator runs it and attaches
	// queue-depth probes in Start.
	o.pipelineGate = chanwatch.NewPressureGate(chanwatch.DefaultThresholds())
	o.pipeline = pipeline.New(pipeline.Config{
		AdmissionGate:         o.diskAdmissionGate,
		VaultAdmissionGate:    o.vaultAdmissionGate,
		DeferWritesGate:       o.diskDeferWrites,
		NodeID:                o.localNodeIDGLID,
		Logger:                baseLogger,
		Alerts:                o.alerts,
		Digesters:             cfg.Digesters,
		OnCheckpoint:          cfg.OnIngesterCheckpoint,
		PressureGate:          o.pipelineGate,
		IngestionOutCapacity:  cfg.IngestChannelSize,
		IngestionRetryDelay:   cfg.IngesterRetryDelay,
		SegmentCompletePolicy: cfg.SegmentCompletePolicy,
		SegmentDisableFsync:   cfg.SegmentDisableFsync,
	})
	if cfg.SegmentDisableFsync {
		logger.Warn("segmentation hot-path fsync disabled — group-commit flushes are not durable until segment close; dev/load testing only")
	}

	// Seed the cached readiness flag so /readyz reports true while the
	// vault map is still empty (matches the legacy live-check semantics)
	// even before the readiness refresher goroutine has run a tick. The
	// refresher starts in Start() and overwrites this on its first pass.
	o.cachedReplicationReady.Store(true)

	// gastrolog-51gme step 10: when the vault-ctl Raft leader removes a
	// node from the voter set, propose CmdPruneNode on every instance
	// sub-FSM in that vault so pendingDeletes ExpectedFrom obligations
	// from the decommissioned node don't block finalization. The
	// reconciler's onPruneNode handler will then propose
	// CmdFinalizeDelete for any chunk whose ExpectedFrom became empty.
	o.vaultCtlLeaders.SetOnMemberRemoved(o.proposePruneNodeForVault)
	o.vaultCtlLeaders.SetOnLeadGained(o.onVaultCtlLeadGained)

	// Per-instance retention rate alerter (gastrolog-47qyw): the condition
	// is >10 deletes/sec sustained over a 30s window. These constants ARE
	// the retention-rate catalog row's documented threshold — the row's
	// Cause text quotes them, so a change here changes both. Priority comes
	// from the catalog like every other alarm. The vaultName closure looks
	// up the human label from the current vault registry; "" if unknown.
	o.retentionRates = newRateAlerter(rateAlerterConfig{
		Window:    30 * time.Second,
		Kind:      "retention",
		Threshold: 10.0,
		Alerts:    o.alerts,
		VaultName: o.vaultLabel,
	})

	o.stageEvents = newStageEventCounters()

	// Register the single retention sweep that discovers all vault instances
	// each tick. No per-vault lifecycle management needed.
	if err := o.startRetentionSweep(); err != nil {
		return nil, fmt.Errorf("retention sweep: %w", err)
	}

	if err := o.startArchivalSweep(); err != nil {
		return nil, fmt.Errorf("archival sweep: %w", err)
	}

	if err := o.startReconcileSweep(); err != nil {
		return nil, fmt.Errorf("reconcile sweep: %w", err)
	}

	// Vault catchup sweep: every node consults its OWN replicated FSM
	// every 20s and runs three independent reconciliation passes per
	// instance (pending obligations, local orphans, missing replicas).
	// Phase-offset from retention's :00 tick to avoid spikiness. See
	// gastrolog-51gme (delete-side) and gastrolog-2dgvj (create-side).
	if err := o.startDiskGuard(); err != nil {
		return nil, err
	}
	if err := o.startInstanceCatchupSweep(); err != nil {
		return nil, fmt.Errorf("vault catchup sweep: %w", err)
	}

	// Warm-cache eviction sweep: every minute (second 23, phase-offset
	// from the other sweeps) walk every leader instance and apply whatever
	// LRU + TTL policies its chunk manager was configured with. No-op for
	// managers without an eviction policy. See gastrolog-2idw8.
	if err := o.startCacheEvictionSweep(); err != nil {
		return nil, fmt.Errorf("cache eviction sweep: %w", err)
	}

	// Vault-ctl membership reconcile safety net (gastrolog-11bla):
	// wakes every active leader-epoch goroutine via desiredChanged
	// every 30 s as a fallback for primary triggers (leadership
	// gain, SetDesiredMembers) that may have missed firing.
	if err := o.startVaultCtlMembershipReconcile(); err != nil {
		return nil, fmt.Errorf("vault-ctl membership reconcile: %w", err)
	}

	// Lock diagnostics default ON (gastrolog-1ug3rq): per-acquisition cost
	// is a few microseconds on per-batch/per-RPC paths, and the payoff is a
	// leak report naming the exact acquisition site instead of a node-wide
	// silent wedge. GLOG_LOCK_TRACKING=off|false|0 disables.
	switch strings.ToLower(strings.TrimSpace(os.Getenv("GLOG_LOCK_TRACKING"))) {
	case "off", "false", "0":
	default:
		o.mu.EnableTracking()
	}

	return o, nil
}

// SetRemoteTransferrer injects the cross-node chunk transferrer.
// Must be called before Start(). Safe to leave nil for single-node mode.
func (o *Orchestrator) SetRemoteTransferrer(t RemoteTransferrer) {
	o.transferrer = t
}

// SetChunkReplicator injects the ordered instance replication client.
func (o *Orchestrator) SetChunkReplicator(tr ChunkReplicator) {
	o.chunkReplicator = tr
}

// Logger returns a child logger scoped for a subcomponent.
// Use this when passing loggers to components created by the orchestrator.
func (o *Orchestrator) Logger() *slog.Logger {
	return o.logger
}

// Scheduler returns the shared scheduler for job submission and listing.
func (o *Orchestrator) Scheduler() *Scheduler {
	return o.scheduler
}

// GetIngesterStats returns the stats for a specific ingester.
// Returns nil if the ingester is not found.
func (o *Orchestrator) GetIngesterStats(id glid.GLID) *IngesterStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ingesterStats[id]
}

// IngesterName returns the registered display name for the given ingester.
func (o *Orchestrator) IngesterName(id glid.GLID) string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ingesterMeta[id].Name
}

// IsIngesterRunning reports whether the given ingester's run is currently
// executing, backed by the Alive flag the pipeline adapter toggles around each
// run. A crashed or retry-waiting ingester reports false — this is the signal
// the ingester convergence sweep alerts on (gastrolog-3mnjlo).
func (o *Orchestrator) IsIngesterRunning(id glid.GLID) bool {
	o.mu.RLock()
	stats := o.ingesterStats[id]
	o.mu.RUnlock()
	return stats != nil && stats.Alive.Load()
}

// GetRouteStats returns a snapshot of the global routing counters, sourced from
// the pipeline routing manager (records that entered routing, went unmatched,
// or matched a route and were fanned out).
func (o *Orchestrator) GetRouteStats() *RouteStats {
	snap := o.pipeline.RouteStats()
	return &RouteStats{
		Routed:    int64(snap.Routed),    //nolint:gosec // G115: counter bounded in practice
		Unmatched: int64(snap.Unmatched), //nolint:gosec // G115
		Matched:   int64(snap.Matched),   //nolint:gosec // G115
	}
}

// IsRouteTableActive reports whether a route table is currently published to
// the pipeline. When false, every ingested record goes unmatched (a counted
// drop).
func (o *Orchestrator) IsRouteTableActive() bool {
	return o.pipeline.RoutingActive()
}

// VaultRouteStatsList returns per-vault matched-record counts from the pipeline
// routing manager (one entry per destination vault that has matched a route).
func (o *Orchestrator) VaultRouteStatsList() map[glid.GLID]*VaultRouteStats {
	snap := o.pipeline.RouteStats()
	result := make(map[glid.GLID]*VaultRouteStats, len(snap.PerVault))
	for vaultID, matched := range snap.PerVault {
		result[vaultID] = &VaultRouteStats{Matched: int64(matched)} //nolint:gosec // G115
	}
	return result
}

// PerRouteStatsList returns per-route matched-record counts from the pipeline
// routing manager (one entry per route that has matched at least one record).
func (o *Orchestrator) PerRouteStatsList() map[glid.GLID]*PerRouteStats {
	snap := o.pipeline.RouteStats()
	result := make(map[glid.GLID]*PerRouteStats, len(snap.PerRoute))
	for routeID, matched := range snap.PerRoute {
		result[routeID] = &PerRouteStats{Matched: int64(matched)} //nolint:gosec // G115
	}
	return result
}

// IngestQueueDepth returns the current depth of the ingestion→digestion
// queue (minted-but-not-yet-digested messages).
func (o *Orchestrator) IngestQueueDepth() int {
	return o.pipeline.IngestQueueDepth()
}

// IngestQueueCapacity returns the capacity of the ingestion→digestion queue.
func (o *Orchestrator) IngestQueueCapacity() int {
	return o.pipeline.IngestQueueCapacity()
}

// IngestQueueNearFull returns true if the ingest queue is at or above 90% capacity.
func (o *Orchestrator) IngestQueueNearFull() bool {
	c := o.pipeline.IngestQueueCapacity()
	if c == 0 {
		return false
	}
	return o.pipeline.IngestQueueDepth() >= c*9/10
}

// PressureGate exposes the ingest-pipeline pressure signal for ingesters to
// consult before emitting records. It is non-nil for the orchestrator's whole
// lifetime; it only begins ticking (and elevating) once Start runs.
func (o *Orchestrator) PressureGate() *chanwatch.PressureGate {
	return o.pipelineGate
}

// VaultSnapshot is a point-in-time summary of a vault's state.
type VaultSnapshot struct {
	ID glid.GLID
	// Name is the operator-facing vault name from system config. Broadcast
	// in NodeStats so every stats consumer (inspector throughput rows, CLI)
	// can label vaults by name instead of falling back to GLID prefixes.
	Name         string
	RecordCount  int64
	ChunkCount   int
	SealedChunks int
	DataBytes    int64
	Enabled      bool
	// RaftAppliedIndex is the local node's vault-ctl Raft applied
	// index for this vault. Zero if this node has no vault-ctl group
	// (or its Raft instance hasn't initialized). Broadcast in
	// NodeStats so the per-vault-ctl learner promoter
	// (gastrolog-gcbx7) can observe each follower's catchup progress.
	RaftAppliedIndex uint64
}

// VaultAppendStats returns per-vault cumulative segmentation throughput
// counters from the pipeline supervisor. Empty when the pipeline is not
// running (gastrolog-4eh5ns).
func (o *Orchestrator) VaultAppendStats() []segmentation.AppendStats {
	if o.pipeline == nil {
		return nil
	}
	return o.pipeline.AppendStats()
}

// VaultCollectStats returns per-vault home-side collection counters from the
// pipeline supervisor (gastrolog-10n6k8).
func (o *Orchestrator) VaultCollectStats() []collection.VaultCollectStats {
	if o.pipeline == nil {
		return nil
	}
	return o.pipeline.CollectStats()
}

// VaultSealStats returns per-vault GLCB seal counters from the pipeline
// supervisor (gastrolog-10n6k8).
func (o *Orchestrator) VaultSealStats() []chunking.VaultSealStats {
	if o.pipeline == nil {
		return nil
	}
	return o.pipeline.SealStats()
}

// VaultPublishStats returns per-vault segment-publish counters from the
// pipeline supervisor's distribution manager (gastrolog-4r784a).
func (o *Orchestrator) VaultPublishStats() []distribution.VaultPublishStats {
	if o.pipeline == nil {
		return nil
	}
	return o.pipeline.PublishStats()
}

// VaultChunkStageStats returns per-vault chunk-lifecycle stage counters
// (planned/built/sealed/released/head-purges) from the pipeline supervisor's
// chunking manager (gastrolog-4r784a).
func (o *Orchestrator) VaultChunkStageStats() []chunking.VaultStageStats {
	if o.pipeline == nil {
		return nil
	}
	return o.pipeline.ChunkStageStats()
}

// VaultStageEventStats returns per-vault orchestrator-owned stage-event
// counters (GLCB catch-up pulls, retention deletes) — gastrolog-4r784a.
func (o *Orchestrator) VaultStageEventStats() []VaultStageEventSnapshot {
	if o.stageEvents == nil {
		return nil
	}
	return o.stageEvents.snapshot()
}

// VaultSnapshots returns a snapshot of stats for all registered vaults.
// Vaults without a local chunk instance (e.g. placement excludes this
// node) are still reported with zero chunk/byte counts so consumers
// that key off (vaultID → some field) — notably the per-vault-ctl
// learner promoter, which reads RaftAppliedIndex — get a row for
// every vault this node knows about, not only the ones it stores
// data for.
func (o *Orchestrator) VaultSnapshots() []VaultSnapshot {
	vaultIDs := o.ListVaults()
	snapshots := make([]VaultSnapshot, 0, len(vaultIDs))
	for _, id := range vaultIDs {
		snap := VaultSnapshot{
			ID:               id,
			Name:             o.vaultLabel(id),
			Enabled:          o.IsVaultEnabled(id),
			RaftAppliedIndex: o.vaultCtlAppliedIndex(id),
		}
		// Chunk-derived fields are best-effort. The local view fails
		// for vaults without an active local chunk manager (no
		// placement on this node); that's a legitimate state, not an
		// error — leave the fields at zero. The broadcast purpose is
		// per-node stats (THIS node's disk view), so local is the
		// correct API choice here, not cluster.
		if metas, err := o.ListLocalChunkMetas(id); err == nil {
			snap.ChunkCount = len(metas)
			for _, m := range metas {
				if m.Sealed {
					snap.SealedChunks++
				}
				snap.RecordCount += m.RecordCount
				if m.DiskBytes > 0 {
					snap.DataBytes += m.DiskBytes
				} else {
					snap.DataBytes += m.Bytes
				}
			}
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots
}

// vaultCtlAppliedIndex returns this node's local vault-ctl Raft
// applied index for the given vault. Zero if the vault has no
// vault-ctl group on this node (e.g. placement excludes it) or the
// GroupManager isn't wired (single-node test). Read at snapshot time
// so the value reflects the latest applied entry on
// this node.
func (o *Orchestrator) vaultCtlAppliedIndex(vaultID glid.GLID) uint64 {
	if o.groupMgr == nil {
		return 0
	}
	g := o.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil || g.Raft == nil {
		return 0
	}
	return g.Raft.AppliedIndex()
}
