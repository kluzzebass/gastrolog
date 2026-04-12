// Package orchestrator coordinates ingestion, indexing, and querying
// without owning business logic. It filters records to chunk managers,
// schedules index builds on seal events, and delegates queries.
package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chanwatch"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/lifecycle"
	"gastrolog/internal/logging"
	"gastrolog/internal/notify"

	"github.com/google/uuid"
)

// IngesterStats tracks per-ingester metrics using atomic counters.
// Safe for concurrent reads (from API handlers) and writes (from ingest loop).
type IngesterStats struct {
	MessagesIngested atomic.Int64
	BytesIngested    atomic.Int64
	Errors           atomic.Int64
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
	Name string
	Type string
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

// RecordForwarder ships records to remote cluster nodes. The orchestrator
// calls Forward() during ingestion for records that match routes targeting
// vaults on other nodes.
//
// Forward is the fire-and-forget path — it must be non-blocking (channel
// enqueue, drop on full) so it is safe to call under the orchestrator
// mutex.
//
// ForwardSync is the ack-gated path — it blocks until each record is
// accepted by the per-node channel or ctx expires. Callers invoke it
// OUTSIDE any orchestrator lock, from the ack-after-replication goroutine,
// so the block is scoped to the ack-gated caller only.
//
// RegisterPressureGate wires the per-node forward channels as probes on
// the orchestrator's shared pressure gate so ingesters throttle upstream
// when cross-node forwarding is backed up (gastrolog-27zvt).
type RecordForwarder interface {
	Forward(ctx context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error
	ForwardSync(ctx context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error
	RegisterPressureGate(gate *chanwatch.PressureGate)
}

// TierReplicator sequences all replication commands for a tier on a single
// ordered stream per follower. Nil in single-node mode.
type TierReplicator interface {
	AppendRecords(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, records []chunk.Record) error
	SealTier(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error
	ImportSealedChunk(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID, records []chunk.Record) error
	DeleteChunk(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, chunkID chunk.ChunkID) error
}

// RemoteTransferrer sends records to a remote node for cross-node chunk
// migration. Unlike RecordForwarder (fire-and-forget for ingestion), this
// is synchronous and reliable — the caller blocks until the remote node
// confirms delivery.
type RemoteTransferrer interface {
	// TransferRecords streams records to a remote node, which imports them
	// as a new sealed chunk. Used by MoveChunk and DrainVault where
	// preserving chunk boundaries is desired.
	TransferRecords(ctx context.Context, nodeID string, vaultID uuid.UUID, next chunk.RecordIterator) error

	// ForwardAppend sends records to a remote node, which appends them to
	// the destination vault's active chunk (same as live ingestion).
	// Synchronous — blocks until the remote node confirms the append.
	// Used by retention eject where records should flow through the
	// destination's normal rotation lifecycle.
	ForwardAppend(ctx context.Context, nodeID string, vaultID uuid.UUID, records []chunk.Record) error

	// StreamToTier opens a single streaming connection and pipes all records
	// to a remote tier's active chunk. Used for tier transitions — one stream,
	// no per-batch round trips.
	StreamToTier(ctx context.Context, nodeID string, vaultID, tierID uuid.UUID, next chunk.RecordIterator) error

	// WaitVaultReady blocks until the vault is registered and accepting
	// records on the given node, or ctx expires. Used by DrainVault to
	// synchronize with the target node's AddVault before unregistering
	// the vault locally.
	WaitVaultReady(ctx context.Context, nodeID string, vaultID uuid.UUID) error
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
	vaults map[uuid.UUID]*Vault

	// Ingester management.
	ingesters       map[uuid.UUID]Ingester
	ingesterCancels map[uuid.UUID]context.CancelFunc // per-ingester cancel functions
	ingesterStats   map[uuid.UUID]*IngesterStats     // per-ingester metrics
	ingesterMeta    map[uuid.UUID]ingesterInfo        // per-ingester name/type for logging

	// Digesters (message enrichment pipeline).
	digesters []Digester

	// Vault filters.
	filterSet *FilterSet

	// Route stats (atomic, no lock needed for reads/writes).
	routeStats      RouteStats
	vaultRouteStats sync.Map // uuid.UUID → *VaultRouteStats
	perRouteStats   sync.Map // uuid.UUID → *PerRouteStats

	// Record forwarder for cross-node delivery (nil in single-node mode).
	forwarder RecordForwarder

	// Remote transferrer for cross-node chunk migration (nil in single-node mode).
	transferrer RemoteTransferrer

	// Tier replicator: ordered stream per tier per follower (nil in single-node mode).
	tierReplicator TierReplicator

	// Ingest channel and lifecycle.
	ingestCh       chan IngestMessage
	digestedCh     chan digestedRecord
	ingestSize     int
	pressureGate   *chanwatch.PressureGate // shared signal for ingester throttling
	cancel         context.CancelFunc
	done           chan struct{}
	running        bool
	ingesterWg     sync.WaitGroup // tracks ingester goroutines
	digestWg       sync.WaitGroup // tracks digest goroutine
	writeWg    sync.WaitGroup // tracks write goroutine
	ackWg      sync.WaitGroup // tracks in-flight ack-gated replication goroutines
	auxWg      sync.WaitGroup // tracks auxiliary goroutines (watchdog, etc.)

	// Per-tier import mutex for serializing SetNextChunkID + ImportRecords.
	importMu sync.Map // tierID → *sync.Mutex

	// Draining vaults (keyed by vault ID, tracks in-progress migrations).
	draining map[uuid.UUID]*drainState

	// Draining tiers (keyed by "vaultID:tierID", tracks in-progress tier drains).
	tierDraining map[string]*tierDrainState

	// OnTierDrainComplete is called after a tier drain finishes. The dispatch
	// layer uses this to remove the tier from vault tier lists in the config
	// store (which fires a subsequent vault-put notification to rebuild).
	OnTierDrainComplete func(ctx context.Context, vaultID, tierID uuid.UUID)

	// Retention runners (keyed by tierID:storageID, invoked by the shared scheduler).
	retention map[string]*retentionRunner

	// Shared scheduler for all periodic tasks (cron rotation, retention, etc.).
	scheduler *Scheduler

	// Cron rotation lifecycle.
	cronRotation *cronRotationManager

	// Per-tier rate alerters that surface pathological rotation or
	// retention configurations as operator-visible alerts. See
	// gastrolog-47qyw. Both are initialized in New() and evaluated by
	// a periodic goroutine in Start().
	rotationRates  *RateAlerter
	retentionRates *RateAlerter

	// Clock for testing.
	now func() time.Time

	// Config loader for hot-update operations.
	cfgLoader ConfigLoader

	// Local node identity for multi-node filtering.
	localNodeID string

	// Per-ingester rolling sequence counter for EventID assignment.
	// Only accessed from digestLoop (single goroutine), no lock needed.
	ingestSeqs map[string]uint32

	// Alert collector for runtime system alerts.
	alerts AlertCollector

	// chunkSignal fires every time chunk metadata changes on this node
	// (seal, delete, create, compress, cloud upload). The WatchChunks
	// streaming RPC watches this signal to push notifications to
	// connected clients. See gastrolog-1jijm.
	chunkSignal *notify.Signal

	// Suspect tracker for cloud chunks that returned 404.
	suspects *suspectTracker

	// Per-tier leader loop manager. Each tier Raft group gets a leader
	// loop whose OnLead callback reconciles membership against the
	// orchestrator's view of the desired member list. Membership
	// reconciliation runs ONLY on the tier Raft leader, from inside the
	// leader epoch (after raft.Barrier returns).
	tierLeaders *tierLeaderManager

	// Shutdown phase (nil in tests / single-node setups without a
	// Phase wired). When non-nil, hot-path replication helpers like
	// fireAndForgetRemote and sealRemoteFollowers consult
	// phase.ShuttingDown() and skip the remote call during drain, so
	// the orchestrator's pipeline flush does not spam the log with
	// "connection refused" warnings for peers that are going down
	// alongside this node. See gastrolog-1e5ke.
	phase *lifecycle.Phase

	// Logger for this orchestrator instance.
	// Scoped with component="orchestrator" at construction time.
	logger *slog.Logger
}

// shuttingDown reports whether the orchestrator has been told to drain.
// Returns false when the phase is nil (tests / harnesses without a
// wired phase), preserving the pre-gastrolog-1e5ke behaviour.
func (o *Orchestrator) shuttingDown() bool {
	return o.phase != nil && o.phase.ShuttingDown()
}

// ChunkSignal returns the signal that fires on every chunk metadata change.
// The WatchChunks streaming handler uses this to push notifications.
func (o *Orchestrator) ChunkSignal() *notify.Signal {
	return o.chunkSignal
}

// NotifyChunkChange fires the chunk-change signal so that WatchChunks
// subscribers know to refetch. Called from seal, delete, create,
// compress, and cloud upload code paths. Safe for concurrent use.
func (o *Orchestrator) NotifyChunkChange() {
	o.chunkSignal.Notify()
}

// tierLabel returns the operator-friendly name for a tier as configured,
// or "" if the tier or config is unknown. Used by RateAlerter to build
// alert messages that say "tier ssd-hot" instead of just a UUID. Safe to
// call from any goroutine — it acquires the orchestrator read lock.
func (o *Orchestrator) tierLabel(tierID uuid.UUID) string {
	if o.cfgLoader == nil {
		return ""
	}
	cfg, err := o.cfgLoader.Load(context.Background())
	if err != nil || cfg == nil {
		return ""
	}
	if tierCfg := findTierConfig(cfg.Tiers, tierID); tierCfg != nil {
		return tierCfg.Name
	}
	return ""
}

// ConfigLoader provides read access to the full configuration.
// The orchestrator uses this during hot-update operations (ReloadFilters,
// ReloadRotationPolicies, etc.) to resolve references like filter IDs
// and policy IDs without the server having to mediate.
//
// config.Store satisfies this interface.
type ConfigLoader interface {
	Load(ctx context.Context) (*config.Config, error)
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

	// ConfigLoader provides read access to the full configuration.
	// If set, the orchestrator can reload config internally during
	// hot-update operations (ReloadFilters, AddVault, etc.).
	// If nil, hot-update methods that require config will return an error.
	ConfigLoader ConfigLoader

	// LocalNodeID is the raft server ID of this node. Used to filter
	// vaults and ingesters during ApplyConfig — only entities assigned
	// to this node (or with empty NodeID) are instantiated.
	LocalNodeID string

	// Alerts is an optional collector for runtime system alerts.
	// Components call Set to raise alerts and Clear to resolve them.
	Alerts AlertCollector

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

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "orchestrator")

	sched, err := newScheduler(logger, cfg.MaxConcurrentJobs, cfg.Now)
	if err != nil {
		return nil, fmt.Errorf("create scheduler: %w", err)
	}

	o := &Orchestrator{
		vaults:          make(map[uuid.UUID]*Vault),
		ingesters:       make(map[uuid.UUID]Ingester),
		ingesterCancels: make(map[uuid.UUID]context.CancelFunc),
		ingesterStats:   make(map[uuid.UUID]*IngesterStats),
		ingesterMeta:    make(map[uuid.UUID]ingesterInfo),
		draining:     make(map[uuid.UUID]*drainState),
		tierDraining: make(map[string]*tierDrainState),
		retention:    make(map[string]*retentionRunner),
		scheduler:       sched,
		cronRotation:    newCronRotationManager(sched, logger),
		ingestSize:      cfg.IngestChannelSize,
		cfgLoader:       cfg.ConfigLoader,
		localNodeID:     cfg.LocalNodeID,
		ingestSeqs:      make(map[string]uint32),
		alerts:          cfg.Alerts,
		suspects:        newSuspectTracker(),
		chunkSignal:     notify.NewSignal(),
		tierLeaders:     newTierLeaderManager(logger),
		phase:           cfg.Phase,
		now:             cfg.Now,
		logger:          logger,
	}

	// Wire up post-seal callback for cron rotation so sealed chunks
	// get compressed and indexed (same pipeline as ingest-triggered seals).
	o.cronRotation.onSeal = o.postSealWork

	// Per-tier rate alerters. Thresholds are taken from gastrolog-47qyw:
	//   rotation: warn at >1/sec, error at >5/sec, sustained over 30s
	//   retention: warn at >10/sec sustained over 30s
	// The orchestrator's tierName closure looks up the human label from
	// the current vault registry; "" is returned if the tier is unknown.
	o.rotationRates = newRateAlerter(rateAlerterConfig{
		Window:    30 * time.Second,
		Kind:      "rotation",
		Source:    "rotation",
		WarningAt: 1.0,
		ErrorAt:   5.0,
		Alerts:    o.alerts,
		TierName:  o.tierLabel,
	})
	o.retentionRates = newRateAlerter(rateAlerterConfig{
		Window:    30 * time.Second,
		Kind:      "retention",
		Source:    "retention",
		WarningAt: 10.0,
		ErrorAt:   0, // no error escalation per issue scope
		Alerts:    o.alerts,
		TierName:  o.tierLabel,
	})

	// Cron rotation completes its work outside the post-seal pipeline,
	// so the rotation rate counter must be hooked from the cron manager
	// directly. The age-based rotationsweep path increments the counter
	// inline at its seal-trigger site.
	o.cronRotation.onRotation = func(_, tierID uuid.UUID) {
		o.rotationRates.Record(tierID, o.now())
	}

	// Register the single retention sweep that discovers all tier instances
	// each tick. No per-tier lifecycle management needed.
	if err := o.startRetentionSweep(); err != nil {
		return nil, fmt.Errorf("retention sweep: %w", err)
	}

	if err := o.startArchivalSweep(); err != nil {
		return nil, fmt.Errorf("archival sweep: %w", err)
	}

	if err := o.startReconcileSweep(); err != nil {
		return nil, fmt.Errorf("reconcile sweep: %w", err)
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

// SetTierReplicator injects the ordered tier replication client.
func (o *Orchestrator) SetTierReplicator(tr TierReplicator) {
	o.tierReplicator = tr
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
func (o *Orchestrator) GetIngesterStats(id uuid.UUID) *IngesterStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ingesterStats[id]
}

// IngesterName returns the registered display name for the given ingester.
func (o *Orchestrator) IngesterName(id uuid.UUID) string {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ingesterMeta[id].Name
}

// IsIngesterRunning reports whether the given ingester has an active cancel function,
// meaning its goroutine was launched and hasn't been stopped.
func (o *Orchestrator) IsIngesterRunning(id uuid.UUID) bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	_, ok := o.ingesterCancels[id]
	return ok
}

// GetRouteStats returns the global route stats.
func (o *Orchestrator) GetRouteStats() *RouteStats {
	return &o.routeStats
}

// IsFilterSetActive reports whether a compiled filter set exists.
// When false, all ingested records are silently dropped.
func (o *Orchestrator) IsFilterSetActive() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.filterSet != nil
}

// VaultRouteStatsList returns per-vault routing stats for all vaults
// that have received at least one record.
func (o *Orchestrator) VaultRouteStatsList() map[uuid.UUID]*VaultRouteStats {
	result := make(map[uuid.UUID]*VaultRouteStats)
	o.vaultRouteStats.Range(func(key, value any) bool {
		result[key.(uuid.UUID)] = value.(*VaultRouteStats)
		return true
	})
	return result
}

// PerRouteStatsList returns per-route routing stats for all routes
// that have matched at least one record.
func (o *Orchestrator) PerRouteStatsList() map[uuid.UUID]*PerRouteStats {
	result := make(map[uuid.UUID]*PerRouteStats)
	o.perRouteStats.Range(func(key, value any) bool {
		result[key.(uuid.UUID)] = value.(*PerRouteStats)
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
	ID           uuid.UUID
	RecordCount  int64
	ChunkCount   int
	SealedChunks int
	DataBytes    int64
	Enabled      bool
}

// VaultSnapshots returns a snapshot of stats for all registered vaults.
func (o *Orchestrator) VaultSnapshots() []VaultSnapshot {
	vaultIDs := o.ListVaults()
	snapshots := make([]VaultSnapshot, 0, len(vaultIDs))
	for _, id := range vaultIDs {
		metas, err := o.ListChunkMetas(id)
		if err != nil {
			continue
		}
		snap := VaultSnapshot{
			ID:         id,
			ChunkCount: len(metas),
			Enabled:    o.IsVaultEnabled(id),
		}
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
		snapshots = append(snapshots, snap)
	}
	return snapshots
}
