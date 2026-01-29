// Package orchestrator coordinates ingestion, indexing, and querying
// without owning business logic. It routes records to chunk managers,
// schedules index builds on seal events, and delegates queries.
package orchestrator

import (
	"context"
	"errors"
	"iter"
	"sync"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/query"
)

var (
	// ErrNoChunkManagers is returned when no chunk managers are registered.
	ErrNoChunkManagers = errors.New("no chunk managers registered")
	// ErrNoQueryEngines is returned when no query engines are registered.
	ErrNoQueryEngines = errors.New("no query engines registered")
	// ErrUnknownRegistry is returned when a registry key is not found.
	ErrUnknownRegistry = errors.New("unknown registry key")
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
type Orchestrator struct {
	mu sync.RWMutex

	chunks  map[string]chunk.ChunkManager
	indexes map[string]index.IndexManager
	queries map[string]*query.Engine
}

// New creates an Orchestrator with empty registries.
func New() *Orchestrator {
	return &Orchestrator{
		chunks:  make(map[string]chunk.ChunkManager),
		indexes: make(map[string]index.IndexManager),
		queries: make(map[string]*query.Engine),
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

// Ingest routes a record to all registered chunk managers.
// If a chunk is sealed as a result of the append, index builds are
// scheduled asynchronously for that chunk.
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
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(o.chunks) == 0 {
		return ErrNoChunkManagers
	}

	for key, cm := range o.chunks {
		// Capture state before append to detect sealing.
		activeBefore := cm.Active()

		_, _, err := cm.Append(rec)
		if err != nil {
			return err
		}

		// Check if a chunk was sealed (active chunk changed).
		activeAfter := cm.Active()
		if activeBefore != nil && (activeAfter == nil || activeAfter.ID != activeBefore.ID) {
			// The previous active chunk was sealed.
			o.scheduleIndexBuild(key, activeBefore.ID)
		}
	}

	return nil
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
