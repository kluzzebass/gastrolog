// Package orchestrator coordinates ingestion, indexing, and querying
// without owning business logic. It routes records to chunk managers,
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

	"gastrolog/internal/logging"
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
// It routes records to chunk managers, observes seal events to trigger
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
//   - Ingesters emit IngestMessages; orchestrator resolves identity and routes.
//
// Routing:
//   - Each store has a route expression that determines which messages it receives.
//   - Routes are compiled at registration time and evaluated against message attrs.
//   - Special routes: "*" (catch-all), "+" (catch-the-rest), "" (receives nothing).
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Orchestrator owns its scoped logger (component="orchestrator")
//   - Subcomponents receive child loggers with additional context
//   - Logging is intentionally sparse; only lifecycle events are logged
type Orchestrator struct {
	mu sync.RWMutex

	// Store registry. Each store bundles Chunks, Indexes, and Query.
	stores map[string]*Store

	// Ingester management.
	ingesters       map[string]Ingester
	ingesterCancels map[string]context.CancelFunc // per-ingester cancel functions
	ingesterStats   map[string]*IngesterStats     // per-ingester metrics

	// Digesters (message enrichment pipeline).
	digesters []Digester

	// Store filters.
	filterSet *FilterSet


	// Ingest channel and lifecycle.
	ingestCh     chan IngestMessage
	ingestSize   int
	cancel       context.CancelFunc
	done         chan struct{}
	running      bool
	ingesterWg   sync.WaitGroup // tracks ingester goroutines
	ingestLoopWg sync.WaitGroup // tracks ingest loop goroutine

	// Retention runners (keyed by store ID, invoked by the shared scheduler).
	retention map[string]*retentionRunner

	// Shared scheduler for all periodic tasks (cron rotation, retention, etc.).
	scheduler *Scheduler

	// Cron rotation lifecycle.
	cronRotation *cronRotationManager

	// Clock for testing.
	now func() time.Time

	// Logger for this orchestrator instance.
	// Scoped with component="orchestrator" at construction time.
	logger *slog.Logger
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

	return &Orchestrator{
		stores:          make(map[string]*Store),
		ingesters:       make(map[string]Ingester),
		ingesterCancels: make(map[string]context.CancelFunc),
		ingesterStats:   make(map[string]*IngesterStats),
		retention:       make(map[string]*retentionRunner),
		scheduler:       sched,
		cronRotation:    newCronRotationManager(sched, logger),
		ingestSize:      cfg.IngestChannelSize,
		now:             cfg.Now,
		logger:          logger,
	}
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
func (o *Orchestrator) GetIngesterStats(id string) *IngesterStats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.ingesterStats[id]
}
