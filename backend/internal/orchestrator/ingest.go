package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// Ingest routes a record to all registered chunk managers.
// If a chunk is sealed as a result of the append, index builds are
// scheduled asynchronously for that chunk.
//
// This is the direct ingestion API for pre-constructed records.
// For ingester-based ingestion, use Start() which runs an ingest loop
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

	if len(o.stores) == 0 {
		return ErrNoChunkManagers
	}

	// Determine which stores should receive this message.
	var targetStores []uuid.UUID
	if o.filterSet != nil {
		targetStores = o.filterSet.Match(rec.Attrs)
	} else {
		// Legacy behavior: fan-out to all stores.
		targetStores = make([]uuid.UUID, 0, len(o.stores))
		for key := range o.stores {
			targetStores = append(targetStores, key)
		}
	}

	// Dispatch to target stores only.
	for _, key := range targetStores {
		store := o.stores[key]
		if store == nil || !store.Enabled {
			continue
		}
		onSeal := func(cid chunk.ChunkID) {
			o.scheduleCompression(key, cid)
			o.scheduleIndexBuild(key, cid)
		}
		if err := store.Append(rec, onSeal); err != nil {
			return err
		}
	}

	return nil
}

// scheduleCompression triggers an asynchronous compression job for the given chunk
// via the shared scheduler. Only dispatched if the ChunkManager implements ChunkCompressor.
func (o *Orchestrator) scheduleCompression(registryKey uuid.UUID, chunkID chunk.ChunkID) {
	store := o.stores[registryKey]
	if store == nil {
		return
	}

	compressor, ok := store.Chunks.(chunk.ChunkCompressor)
	if !ok {
		return
	}

	name := fmt.Sprintf("compress:%s:%s", registryKey, chunkID)
	if err := o.scheduler.RunOnce(name, compressor.CompressChunk, chunkID); err != nil {
		o.logger.Warn("failed to schedule compression", "name", name, "error", err)
	}
}

// scheduleIndexBuild triggers an asynchronous index build for the given chunk
// via the shared scheduler. The build is visible in ScheduledJobs() while running
// and subject to the scheduler's concurrency limit.
// The IndexManager handles deduplication of concurrent builds for the same chunk.
func (o *Orchestrator) scheduleIndexBuild(registryKey uuid.UUID, chunkID chunk.ChunkID) {
	store := o.stores[registryKey]
	if store == nil {
		return
	}

	name := fmt.Sprintf("index-build:%s:%s", registryKey, chunkID)
	if err := o.scheduler.RunOnce(name, store.Indexes.BuildIndexes, context.Background(), chunkID); err != nil {
		o.logger.Warn("failed to schedule index build", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Build indexes for chunk %s", chunkID))
}
