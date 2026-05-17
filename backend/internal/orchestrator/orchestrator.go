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
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/logging"
	"gastrolog/internal/notify"
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

// RouteStats tracks routing metrics using atomic counters.
// Safe for concurrent reads (from API handlers) and writes (from ingest loop).
type RouteStats struct {
	Ingested atomic.Int64 // total records entering ingest()
	Dropped  atomic.Int64 // records matching no filter
	Routed   atomic.Int64 // records delivered to at least one vault
}

// VaultRouteStats tracks per-vault routing metrics.
type VaultRouteStats struct {
	Matched   atomic.Int64 // records routed to this vault
	Forwarded atomic.Int64 // records sent to remote node for this vault
}

// PerRouteStats tracks per-route routing metrics.
type PerRouteStats struct {
	Matched   atomic.Int64 // records matched by this route
	Forwarded atomic.Int64 // records forwarded to remote node by this route
}

// ingesterInfo holds metadata about an ingester for logging purposes.
// The Ingester interface is a bare Run() — metadata lives alongside it.
type ingesterInfo struct {
	Name    string
	Type    string
	Passive bool // true for listener ingesters that should retry on failure
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

// RecordForwarder ships records to remote cluster nodes for vault routes
// that target a vault on another node.
//
// Forward is a best-effort enqueue (drop on full) — used only for ancillary
// paths such as placement redirect replays, not the ingestion hot path.
//
// ForwardSync blocks until each record is accepted by the per-node buffer
// or ctx / forwarder shutdown fires. Ingestion uses this (outside o.mu) so
// a full forward buffer applies backpressure through digestedCh instead of
// dropping records. Ack-gated ingestion also uses ForwardSync from
// ackAfterReplication.
//
// RegisterPressureGate wires the per-node forward channels as probes on
// the orchestrator's shared pressure gate so ingesters throttle upstream
// when cross-node forwarding is backed up (gastrolog-27zvt).
type RecordForwarder interface {
	Forward(ctx context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error
	ForwardSync(ctx context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error
	RegisterPressureGate(gate *chanwatch.PressureGate)
	RedirectNode(fromNodeID, toNodeID string)
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
	AppendRecords(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID, records []chunk.Record) error
	SealVault(ctx context.Context, nodeID string, vaultID glid.GLID, chunkID chunk.ChunkID) error
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
// migration. Unlike RecordForwarder (fire-and-forget for ingestion), this
// is synchronous and reliable — the caller blocks until the remote node
// confirms delivery.
type RemoteTransferrer interface {
	// TransferRecords streams records to a remote node, which imports them
	// as a new sealed chunk. Used by MoveChunk and DrainVault where
	// preserving chunk boundaries is desired.
	TransferRecords(ctx context.Context, nodeID string, vaultID glid.GLID, next chunk.RecordIterator) error

	// ForwardAppend sends records to a remote node, which appends them to
	// the destination vault's active chunk (same as live ingestion).
	// Synchronous — blocks until the remote node confirms the append.
	// Used by retention eject where records should flow through the
	// destination's normal rotation lifecycle.
	ForwardAppend(ctx context.Context, nodeID string, vaultID glid.GLID, records []chunk.Record) error

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
	mu sync.RWMutex

	// Vault registry. Each vault bundles Chunks, Indexes, and Query.
	vaults map[glid.GLID]*Vault

	// Ingester management.
	ingesters       map[glid.GLID]Ingester
	ingesterCancels map[glid.GLID]context.CancelFunc // per-ingester cancel functions
	ingesterStats   map[glid.GLID]*IngesterStats     // per-ingester metrics
	ingesterMeta    map[glid.GLID]ingesterInfo       // per-ingester name/type for logging

	// Digesters (message enrichment pipeline).
	digesters []Digester

	// Routing table — gastrolog-4kkoo (Phase 5): per-route, priority-ordered,
	// first-match-wins. Replaces the Phase-4 per-vault FilterSet.
	routeSet *RouteSet

	// Route stats (atomic, no lock needed for reads/writes).
	routeStats      RouteStats
	vaultRouteStats sync.Map // glid.GLID → *VaultRouteStats
	perRouteStats   sync.Map // glid.GLID → *PerRouteStats

	// Record forwarder for cross-node delivery (nil in single-node mode).
	forwarder RecordForwarder

	// Remote transferrer for cross-node chunk migration (nil in single-node mode).
	transferrer RemoteTransferrer

	// Vault replicator: ordered stream per instance per follower (nil in single-node mode).
	chunkReplicator ChunkReplicator

	// replicaCircuit tracks per-node backoff for follower replication.
	// After consecutive failures, the node is skipped until the backoff
	// expires. Prevents log spam when a follower is down.
	replicaCircuit sync.Map // nodeID (string) → *replicaBackoff

	// groupMgr is the shared multi-group Raft manager (system, vault ctl, …).
	// Set from factories during ApplyConfig; used to tear down vault ctl groups.
	groupMgr *raftgroup.GroupManager

	// peerConns is the shared gRPC pool for cluster peers. Set from factories
	// during ApplyConfig; used by ApplyVaultControlPlane forwarding.
	peerConns *cluster.PeerConns

	// Ingest channel and lifecycle.
	ingestCh     chan IngestMessage
	digestedCh   chan digestedRecord
	ingestSize   int
	pressureGate *chanwatch.PressureGate // shared signal for ingester throttling
	cancel       context.CancelFunc
	done         chan struct{}
	// running is an atomic so IsRunning() — read by the /readyz HTTP
	// handler on every probe — never blocks on o.mu. Holding it as a
	// plain bool guarded by o.mu reintroduced the original gastrolog-5n6xz
	// freeze through the back door: kubelet's probe took o.mu.RLock,
	// which starved behind any long-held o.mu.Lock writer regardless of
	// the cached replication-ready flag. Start/Stop use CompareAndSwap
	// to preserve the prior check-then-set mutual exclusion.
	running      atomic.Bool
	ingesterWg   sync.WaitGroup // tracks ingester goroutines
	digestWg     sync.WaitGroup // tracks digest goroutine
	writeWg      sync.WaitGroup // tracks write goroutine
	ackWg        sync.WaitGroup // tracks in-flight ack-gated replication goroutines
	auxWg        sync.WaitGroup // tracks auxiliary goroutines (watchdog, etc.)

	// Per-instance import mutex for serializing SetNextChunkID + ImportRecords.
	importMu sync.Map // vaultID → *sync.Mutex

	// Draining vaults (keyed by vault ID, tracks in-progress migrations).
	draining map[glid.GLID]*drainState

	// In-progress instance drains, keyed by vault ID.
	vaultDraining map[string]*vaultDrainState

	// Retention runners (keyed by vaultID:storageID, invoked by the shared scheduler).
	retention map[string]*retentionRunner

	// Shared scheduler for all periodic tasks (cron rotation, retention, etc.).
	scheduler *Scheduler

	// Cron rotation lifecycle.
	cronRotation *cronRotationManager

	// Per-instance rate alerters that surface pathological rotation or
	// retention configurations as operator-visible alerts. See
	// gastrolog-47qyw. Both are initialized in New() and evaluated by
	// a periodic goroutine in Start().
	rotationRates  *RateAlerter
	retentionRates *RateAlerter

	// Clock for testing.
	now func() time.Time

	// Config loader for hot-update operations.
	sysLoader SystemLoader

	// Local node identity for multi-node filtering.
	localNodeID string
	// localNodeIDGLID is the parsed GLID form of localNodeID, pre-computed
	// at construction for the hot EventID-stamping path in digestAndForward.
	// Empty (zero GLID) for memory-config / no-node-id orchestrators.
	localNodeIDGLID glid.GLID

	// Per-ingester rolling sequence counter for EventID assignment.
	// Only accessed from digestLoop (single goroutine), no lock needed.
	ingestSeqs map[string]uint32

	// Alert collector for runtime system alerts.
	alerts AlertCollector

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

	// Suspect tracker for cloud chunks that returned 404.
	suspects *suspectTracker

	// Per-vault leader loop for vault control-plane Raft (replicated instance
	// chunk metadata when multiraft is enabled). Membership reconciliation
	// runs on the vault ctl Raft leader inside its leader epoch.
	vaultCtlLeaders *vaultCtlLeaderManager

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
// Used by the WatchChunks event-relay path to stamp authoritative
// replica info on outbound events so clients see correct counts
// without reconstructing them from per-node event evidence. See
// gastrolog-66vmg.
//
// Returns nil if the local node has no FSM for the vault (single-node /
// memory mode), or if the chunk is unknown to the FSM. Callers in that
// case should leave replica info absent from the outbound event so the
// client preserves its existing cache value via mergeMeta.
func (o *Orchestrator) ChunkResidency(vaultID glid.GLID, chunkID chunk.ChunkID, placementNodeIDs []string) []string {
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
	return fsm.ChunkResidency(chunkID, placementNodeIDs)
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
	// IngestChannelSize is the buffer size for the ingest channel.
	// Defaults to 1000 if not set.
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

	// Alerts is an optional collector for runtime system alerts.
	// Components call Set to raise alerts and Clear to resolve them.
	Alerts AlertCollector

	// OnIngesterAlive is called when an ingester's alive state changes.
	// The app layer wires this to Raft to replicate the state cluster-wide.
	OnIngesterAlive func(ingesterID glid.GLID, alive bool)

	// OnIngesterCheckpoint is called when a Checkpointable ingester saves state.
	// The app layer wires this to Raft to replicate checkpoints cluster-wide.
	OnIngesterCheckpoint func(ingesterID glid.GLID, data []byte)

	// Phase is the shared shutdown signal. When non-nil, the orchestrator
	// consults phase.ShuttingDown() in hot-path replication helpers so that
	// during the drain window (after BeginShutdown) remote forwards no-op
	// instead of spamming "connection refused" against peers that are
	// shutting down alongside this node. See gastrolog-1e5ke.
	Phase *lifecycle.Phase
}

// AlertCollector is the interface for raising and clearing system alerts.
// Satisfied by *alert.Collector.
type AlertCollector interface {
	Set(id string, severity alert.Severity, source, message string)
	Clear(id string)
}

// New creates an Orchestrator with empty registries.
func New(cfg Config) (*Orchestrator, error) {
	if cfg.IngestChannelSize <= 0 {
		cfg.IngestChannelSize = 1000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
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
		vaults:               make(map[glid.GLID]*Vault),
		ingesters:            make(map[glid.GLID]Ingester),
		ingesterCancels:      make(map[glid.GLID]context.CancelFunc),
		ingesterStats:        make(map[glid.GLID]*IngesterStats),
		ingesterMeta:         make(map[glid.GLID]ingesterInfo),
		draining:             make(map[glid.GLID]*drainState),
		vaultDraining:         make(map[string]*vaultDrainState),
		retention:            make(map[string]*retentionRunner),
		scheduler:            sched,
		cronRotation:         newCronRotationManager(sched, logger),
		ingestSize:           cfg.IngestChannelSize,
		sysLoader:            cfg.SystemLoader,
		localNodeID:          cfg.LocalNodeID,
		localNodeIDGLID:      parseNodeGLID(cfg.LocalNodeID),
		ingestSeqs:           make(map[string]uint32),
		alerts:               cfg.Alerts,
		suspects:             newSuspectTracker(),
		chunkSignal:          notify.NewSignal(),
		chunkBus:             notify.NewBus[ChunkChangeEvent](256),
		progressTrigger:      newProgressNotifier(),
		vaultCtlLeaders:      newVaultCtlLeaderManager(baseLogger),
		phase:                cfg.Phase,
		onIngesterAlive:      cfg.OnIngesterAlive,
		onIngesterCheckpoint: cfg.OnIngesterCheckpoint,
		now:                  cfg.Now,
		logger:               logger,
		baseLogger:          baseLogger,
		replicationLogger:   compReplication.Apply(baseLogger),
		drainLogger:         compDrain.Apply(baseLogger),
		retentionLogger:     compRetention.Apply(baseLogger),
		rotationLogger:      compRotation.Apply(baseLogger),
		schedulerLogger:     compScheduler.Apply(baseLogger),
		vaultOpsLogger:      compVaultOps.Apply(baseLogger),
		cacheEvictionLogger: compCacheEviction.Apply(baseLogger),
		cloudHealthLogger:   compCloudHealth.Apply(baseLogger),
	}

	// Seed the cached readiness flag so /readyz reports true while the
	// vault map is still empty (matches the legacy live-check semantics)
	// even before the readiness refresher goroutine has run a tick. The
	// refresher starts in Start() and overwrites this on its first pass.
	o.cachedReplicationReady.Store(true)

	// Wire up post-seal callback for cron rotation so sealed chunks
	// get compressed and indexed (same pipeline as ingest-triggered seals).
	o.cronRotation.onSeal = o.postSealWork

	// gastrolog-51gme step 10: when the vault-ctl Raft leader removes a
	// node from the voter set, propose CmdPruneNode on every instance
	// sub-FSM in that vault so pendingDeletes ExpectedFrom obligations
	// from the decommissioned node don't block finalization. The
	// reconciler's onPruneNode handler will then propose
	// CmdFinalizeDelete for any chunk whose ExpectedFrom became empty.
	o.vaultCtlLeaders.SetOnMemberRemoved(o.proposePruneNodeForVault)

	// Per-instance rate alerters. Thresholds are taken from gastrolog-47qyw:
	//   rotation: warn at >1/sec, error at >5/sec, sustained over 30s
	//   retention: warn at >10/sec sustained over 30s
	// The orchestrator's vaultName closure looks up the human label from
	// the current vault registry; "" is returned if the instance is unknown.
	o.rotationRates = newRateAlerter(rateAlerterConfig{
		Window:    30 * time.Second,
		Kind:      "rotation",
		Source:    "rotation",
		WarningAt: 1.0,
		ErrorAt:   5.0,
		Alerts:    o.alerts,
		VaultName:  o.vaultLabel,
	})
	o.retentionRates = newRateAlerter(rateAlerterConfig{
		Window:    30 * time.Second,
		Kind:      "retention",
		Source:    "retention",
		WarningAt: 10.0,
		ErrorAt:   0, // no error escalation per issue scope
		Alerts:    o.alerts,
		VaultName:  o.vaultLabel,
	})

	// Cron rotation completes its work outside the post-seal pipeline,
	// so the rotation rate counter must be hooked from the cron manager
	// directly. The age-based rotationsweep path increments the counter
	// inline at its seal-trigger site.
	o.cronRotation.onRotation = func(vaultID glid.GLID) {
		o.rotationRates.Record(vaultID, o.now())
	}

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

	return o, nil
}

// SetRecordForwarder injects the cross-node record forwarder.
// Must be called before Start(). Safe to leave nil for single-node mode.
func (o *Orchestrator) SetRecordForwarder(f RecordForwarder) {
	o.forwarder = f
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

// IsIngesterRunning reports whether the given ingester has an active cancel function,
// meaning its goroutine was launched and hasn't been stopped.
func (o *Orchestrator) IsIngesterRunning(id glid.GLID) bool {
	o.mu.RLock()
	stats := o.ingesterStats[id]
	o.mu.RUnlock()
	return stats != nil && stats.Alive.Load()
}

// GetRouteStats returns the global route stats.
func (o *Orchestrator) GetRouteStats() *RouteStats {
	return &o.routeStats
}

// IsFilterSetActive reports whether a routing table is currently
// loaded. When false, all ingested records are silently dropped.
// gastrolog-4kkoo (Phase 5): name kept for proto/RPC stability — the
// underlying state is now a RouteSet, not a per-vault FilterSet.
func (o *Orchestrator) IsFilterSetActive() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.routeSet != nil
}

// VaultRouteStatsList returns per-vault routing stats for all vaults
// that have received at least one record.
func (o *Orchestrator) VaultRouteStatsList() map[glid.GLID]*VaultRouteStats {
	result := make(map[glid.GLID]*VaultRouteStats)
	o.vaultRouteStats.Range(func(key, value any) bool {
		result[key.(glid.GLID)] = value.(*VaultRouteStats)
		return true
	})
	return result
}

// PerRouteStatsList returns per-route routing stats for all routes
// that have matched at least one record.
func (o *Orchestrator) PerRouteStatsList() map[glid.GLID]*PerRouteStats {
	result := make(map[glid.GLID]*PerRouteStats)
	o.perRouteStats.Range(func(key, value any) bool {
		result[key.(glid.GLID)] = value.(*PerRouteStats)
		return true
	})
	return result
}

// IngestQueueDepth returns the current number of messages in the ingest channel.
func (o *Orchestrator) IngestQueueDepth() int {
	return len(o.ingestCh)
}

// IngestQueueCapacity returns the capacity of the ingest channel.
func (o *Orchestrator) IngestQueueCapacity() int {
	return cap(o.ingestCh)
}

// IngestQueueNearFull returns true if the ingest queue is at or above 90% capacity.
func (o *Orchestrator) IngestQueueNearFull() bool {
	c := cap(o.ingestCh)
	if c == 0 {
		return false
	}
	return len(o.ingestCh) >= c*9/10
}

// PressureGate exposes the ingest pipeline pressure signal for ingesters to
// consult before emitting records. Returns nil if the orchestrator has not
// been Started yet; ingesters should treat nil as "no throttling".
func (o *Orchestrator) PressureGate() *chanwatch.PressureGate {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.pressureGate
}

// VaultSnapshot is a point-in-time summary of a vault's state.
type VaultSnapshot struct {
	ID           glid.GLID
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
			Enabled:          o.IsVaultEnabled(id),
			RaftAppliedIndex: o.vaultCtlAppliedIndex(id),
		}
		// Chunk-derived fields are best-effort. ListChunkMetas fails
		// for vaults without an active local chunk manager (no
		// placement on this node); that's a legitimate state, not an
		// error — leave the fields at zero.
		if metas, err := o.ListChunkMetas(id); err == nil {
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
// so the value reflects the latest committed-and-applied entry on
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
