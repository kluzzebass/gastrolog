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

	"gastrolog/internal/config"
	"gastrolog/internal/logging"

	"github.com/google/uuid"
)

// IngesterStats tracks per-ingester metrics using atomic counters.
// Safe for concurrent reads (from API handlers) and writes (from ingest loop).
type IngesterStats struct {
	MessagesIngested atomic.Int64
	BytesIngested    atomic.Int64
	Errors           atomic.Int64
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

	// Digesters (message enrichment pipeline).
	digesters []Digester

	// Vault filters.
	filterSet *FilterSet


	// Ingest channel and lifecycle.
	ingestCh   chan IngestMessage
	digestedCh chan digestedRecord
	ingestSize int
	cancel     context.CancelFunc
	done       chan struct{}
	running    bool
	ingesterWg sync.WaitGroup // tracks ingester goroutines
	digestWg   sync.WaitGroup // tracks digest goroutine
	writeWg    sync.WaitGroup // tracks write goroutine

	// Retention runners (keyed by vault ID, invoked by the shared scheduler).
	retention map[uuid.UUID]*retentionRunner

	// Shared scheduler for all periodic tasks (cron rotation, retention, etc.).
	scheduler *Scheduler

	// Cron rotation lifecycle.
	cronRotation *cronRotationManager

	// Clock for testing.
	now func() time.Time

	// Config loader for hot-update operations.
	cfgLoader ConfigLoader

	// Local node identity for multi-node filtering.
	localNodeID string

	// Logger for this orchestrator instance.
	// Scoped with component="orchestrator" at construction time.
	logger *slog.Logger
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
}

// New creates an Orchestrator with empty registries.
func New(cfg Config) *Orchestrator {
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
		// This should never fail in practice (just creates a scheduler).
		panic(fmt.Sprintf("create scheduler: %v", err))
	}

	o := &Orchestrator{
		vaults:          make(map[uuid.UUID]*Vault),
		ingesters:       make(map[uuid.UUID]Ingester),
		ingesterCancels: make(map[uuid.UUID]context.CancelFunc),
		ingesterStats:   make(map[uuid.UUID]*IngesterStats),
		retention:       make(map[uuid.UUID]*retentionRunner),
		scheduler:       sched,
		cronRotation:    newCronRotationManager(sched, logger),
		ingestSize:      cfg.IngestChannelSize,
		cfgLoader:       cfg.ConfigLoader,
		localNodeID:     cfg.LocalNodeID,
		now:             cfg.Now,
		logger:          logger,
	}

	// Wire up post-seal callback for cron rotation so sealed chunks
	// get compressed and indexed (same pipeline as ingest-triggered seals).
	o.cronRotation.onSeal = o.postSealWork

	return o
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
