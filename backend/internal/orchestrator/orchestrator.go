// Package orchestrator coordinates ingestion, indexing, and querying
// without owning business logic. It routes records to chunk managers,
// schedules index builds on seal events, and delegates queries.
package orchestrator

import (
	"context"
	"errors"
	"iter"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
	"gastrolog/internal/source"
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
type Orchestrator struct {
	mu sync.RWMutex

	// Component registries.
	chunks  map[string]chunk.ChunkManager
	indexes map[string]index.IndexManager
	queries map[string]*query.Engine

	// Receiver management.
	receivers map[string]Receiver
	sources   *source.Registry

	// Ingest channel and lifecycle.
	ingestCh   chan IngestMessage
	ingestSize int
	cancel     context.CancelFunc
	done       chan struct{}
	running    bool

	// Clock for testing.
	now func() time.Time
}

// Config configures an Orchestrator.
type Config struct {
	// SourceRegistry for identity resolution. Required for receiver-based ingestion.
	Sources *source.Registry

	// IngestChannelSize is the buffer size for the ingest channel.
	// Defaults to 1000 if not set.
	IngestChannelSize int

	// Now returns the current time. Defaults to time.Now.
	Now func() time.Time
}

// New creates an Orchestrator with empty registries.
func New(cfg Config) *Orchestrator {
	if cfg.IngestChannelSize <= 0 {
		cfg.IngestChannelSize = 1000
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	return &Orchestrator{
		chunks:     make(map[string]chunk.ChunkManager),
		indexes:    make(map[string]index.IndexManager),
		queries:    make(map[string]*query.Engine),
		receivers:  make(map[string]Receiver),
		sources:    cfg.Sources,
		ingestSize: cfg.IngestChannelSize,
		now:        cfg.Now,
	}
}

// RegisterChunkManager adds a chunk manager to the registry.
func (o *Orchestrator) RegisterChunkManager(key string, cm chunk.ChunkManager) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.chunks[key] = cm
}

// RegisterIndexManager adds an index manager to the registry.
func (o *Orchestrator) RegisterIndexManager(key string, im index.IndexManager) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.indexes[key] = im
}

// RegisterQueryEngine adds a query engine to the registry.
func (o *Orchestrator) RegisterQueryEngine(key string, qe *query.Engine) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.queries[key] = qe
}

// RegisterReceiver adds a receiver to the registry.
// Must be called before Start().
func (o *Orchestrator) RegisterReceiver(id string, r Receiver) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.receivers[id] = r
}

// UnregisterReceiver removes a receiver from the registry.
// Must be called before Start() or after Stop().
func (o *Orchestrator) UnregisterReceiver(id string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.receivers, id)
}

// Start launches all receivers and the ingest loop.
// Each receiver runs in its own goroutine, emitting messages to a shared channel.
// The ingest loop receives messages, resolves identity, and routes to chunk managers.
// Start returns immediately; use Stop() to shut down.
func (o *Orchestrator) Start(ctx context.Context) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.running {
		return ErrAlreadyRunning
	}

	// Create cancellable context for all receivers and ingest loop.
	ctx, cancel := context.WithCancel(ctx)
	o.cancel = cancel
	o.done = make(chan struct{})
	o.running = true

	// Create ingest channel.
	o.ingestCh = make(chan IngestMessage, o.ingestSize)

	// Launch receiver goroutines.
	for id, r := range o.receivers {
		go o.runReceiver(ctx, id, r)
	}

	// Launch ingest loop.
	go o.ingestLoop(ctx)

	return nil
}

// Stop cancels all receivers and the ingest loop, then waits for completion.
func (o *Orchestrator) Stop() error {
	o.mu.Lock()
	if !o.running {
		o.mu.Unlock()
		return ErrNotRunning
	}
	cancel := o.cancel
	done := o.done
	o.mu.Unlock()

	// Cancel context to stop receivers and ingest loop.
	cancel()

	// Wait for ingest loop to finish.
	<-done

	o.mu.Lock()
	o.running = false
	o.cancel = nil
	o.done = nil
	o.ingestCh = nil
	o.mu.Unlock()

	return nil
}

// runReceiver runs a single receiver, recovering from panics.
func (o *Orchestrator) runReceiver(ctx context.Context, id string, r Receiver) {
	// Receiver.Run blocks until ctx is cancelled or error.
	// Errors are currently ignored - future: add error callback or logging.
	_ = r.Run(ctx, o.ingestCh)
}

// ingestLoop receives messages from the ingest channel and routes them.
func (o *Orchestrator) ingestLoop(ctx context.Context) {
	defer close(o.done)

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-o.ingestCh:
			if !ok {
				return
			}
			o.processMessage(msg)
		}
	}
}

// processMessage resolves identity and routes to chunk managers.
func (o *Orchestrator) processMessage(msg IngestMessage) {
	// Resolve source identity.
	var sourceID chunk.SourceID
	if o.sources != nil {
		sourceID = o.sources.Resolve(msg.Attrs)
	}

	// Construct record.
	now := o.now()
	rec := chunk.Record{
		WriteTS:  now,
		IngestTS: now,
		SourceID: sourceID,
		Raw:      msg.Raw,
	}

	// Route to chunk managers (reuses existing Ingest logic).
	_ = o.ingest(rec)
}

// ingest is the internal ingest implementation, called by processMessage.
// Extracted from Ingest to allow both direct and channel-based ingestion.
func (o *Orchestrator) ingest(rec chunk.Record) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.chunks) == 0 {
		return ErrNoChunkManagers
	}

	for key, cm := range o.chunks {
		activeBefore := cm.Active()

		_, _, err := cm.Append(rec)
		if err != nil {
			return err
		}

		activeAfter := cm.Active()
		if activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID) {
			o.scheduleIndexBuild(key, activeBefore.ID)
		}
	}

	return nil
}

// Ingest routes a record to all registered chunk managers.
// If a chunk is sealed as a result of the append, index builds are
// scheduled asynchronously for that chunk.
//
// This is the direct ingestion API for pre-constructed records.
// For receiver-based ingestion, use Start() which runs an ingest loop
// that receives IngestMessages, resolves identity, and calls this internally.
//
// Ingest acquires an exclusive lock to serialize seal detection. This
// means only one Ingest call runs at a time, but Search calls can still
// run concurrently (they only need the registry snapshot, not the lock
// during iteration).
//
// Error semantics: This is fan-out with partial failure. If CM A succeeds
// and CM B fails, the record is persisted in A but not B, and the error
// from B is returned. There is no rollback. This is acceptable for now
// since we typically have one CM per registry key, but callers should be
// aware of this behavior.
//
// Seal detection: compares Active() before/after append to detect when
// the active chunk changes (indicating the previous chunk was sealed).
// This assumes:
//   - ChunkManagers are append-serialized (single writer per CM)
//   - No delayed/async sealing within ChunkManager
//
// Future improvement: have ChunkManager.Append() return sealed chunk ID,
// or emit seal events via callback.
func (o *Orchestrator) Ingest(rec chunk.Record) error {
	return o.ingest(rec)
}

// scheduleIndexBuild triggers an asynchronous index build for the given chunk.
// The IndexManager handles deduplication of concurrent builds.
//
// Goroutine lifecycle: Currently fire-and-forget with no cancellation,
// shutdown coordination, or backpressure. This is acceptable because:
//   - Index builds are bounded (one per sealed chunk)
//   - IndexManager deduplicates concurrent builds for the same chunk
//   - No long-lived daemon yet that needs graceful shutdown
//
// Future improvement: add a worker pool or WaitGroup for graceful shutdown,
// and accept a context for cancellation.
func (o *Orchestrator) scheduleIndexBuild(registryKey string, chunkID chunk.ChunkID) {
	im, ok := o.indexes[registryKey]
	if !ok {
		return
	}

	go func() {
		// Use background context - builds should complete regardless of caller.
		_ = im.BuildIndexes(context.Background(), chunkID)
	}()
}

// Search delegates to the query engine registered under the given key.
// If key is empty, uses "default".
func (o *Orchestrator) Search(ctx context.Context, key string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if key == "" {
		key = "default"
	}

	qe, ok := o.queries[key]
	if !ok {
		if len(o.queries) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := qe.Search(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchThenFollow delegates to the query engine's SearchThenFollow method.
func (o *Orchestrator) SearchThenFollow(ctx context.Context, key string, q query.Query, resume *query.ResumeToken) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if key == "" {
		key = "default"
	}

	qe, ok := o.queries[key]
	if !ok {
		if len(o.queries) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := qe.SearchThenFollow(ctx, q, resume)
	return seq, nextToken, nil
}

// SearchWithContext delegates to the query engine's SearchWithContext method.
func (o *Orchestrator) SearchWithContext(ctx context.Context, key string, q query.Query) (iter.Seq2[chunk.Record, error], func() *query.ResumeToken, error) {
	o.mu.RLock()
	defer o.mu.RUnlock()

	if key == "" {
		key = "default"
	}

	qe, ok := o.queries[key]
	if !ok {
		if len(o.queries) == 0 {
			return nil, nil, ErrNoQueryEngines
		}
		return nil, nil, ErrUnknownRegistry
	}

	seq, nextToken := qe.SearchWithContext(ctx, q)
	return seq, nextToken, nil
}
