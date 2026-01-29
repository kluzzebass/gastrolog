package orchestrator

import (
	"context"

	"gastrolog/internal/chunk"
)

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
