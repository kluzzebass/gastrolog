// Package orchestrator coordinates ingestion, indexing, and querying
// without owning business logic. It routes records to chunk managers,
// schedules index builds on seal events, and delegates queries.
package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
	"gastrolog/internal/query"
)

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
// Receiver lifecycle:
//   - Receivers are registered before Start() is called.
//   - Start() launches one goroutine per receiver plus an ingest loop.
//   - Stop() cancels all receivers and the ingest loop via context.
//   - Receivers emit IngestMessages; orchestrator resolves identity and routes.
//
// Logging:
//   - Logger is dependency-injected via Config.Logger
//   - Orchestrator owns its scoped logger (component="orchestrator")
//   - Subcomponents receive child loggers with additional context
//   - Logging is intentionally sparse; only lifecycle events are logged
type Orchestrator struct {
	mu sync.RWMutex

	// Component registries.
	chunks  map[string]chunk.ChunkManager
	indexes map[string]index.IndexManager
	queries map[string]*query.Engine

	// Receiver management.
	receivers map[string]Receiver

	// Ingest channel and lifecycle.
	ingestCh     chan IngestMessage
	ingestSize   int
	cancel       context.CancelFunc
	done         chan struct{}
	running      bool
	receiverWg   sync.WaitGroup // tracks receiver goroutines
	ingestLoopWg sync.WaitGroup // tracks ingest loop goroutine

	// Index build lifecycle.
	indexCtx    context.Context
	indexCancel context.CancelFunc
	indexWg     sync.WaitGroup

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

	// Initialize index build context for standalone Ingest() calls.
	// This is replaced with a fresh context if Start() is called.
	indexCtx, indexCancel := context.WithCancel(context.Background())

	// Scope logger with component identity.
	logger := logging.Default(cfg.Logger).With("component", "orchestrator")

	return &Orchestrator{
		chunks:      make(map[string]chunk.ChunkManager),
		indexes:     make(map[string]index.IndexManager),
		queries:     make(map[string]*query.Engine),
		receivers:   make(map[string]Receiver),
		ingestSize:  cfg.IngestChannelSize,
		now:         cfg.Now,
		indexCtx:    indexCtx,
		indexCancel: indexCancel,
		logger:      logger,
	}
}

// Logger returns a child logger scoped for a subcomponent.
// Use this when passing loggers to components created by the orchestrator.
func (o *Orchestrator) Logger() *slog.Logger {
	return o.logger
}
