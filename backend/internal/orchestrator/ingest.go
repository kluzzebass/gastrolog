package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/chunk"
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

	if len(o.chunks) == 0 {
		return ErrNoChunkManagers
	}

	// Determine which stores should receive this message.
	var targetStores []string
	if o.filterSet != nil {
		targetStores = o.filterSet.Match(rec.Attrs)
	} else {
		// Legacy behavior: fan-out to all stores.
		targetStores = make([]string, 0, len(o.chunks))
		for key := range o.chunks {
			targetStores = append(targetStores, key)
		}
	}

	// Dispatch to target stores only.
	for _, key := range targetStores {
		cm, ok := o.chunks[key]
		if !ok {
			continue // store not registered (shouldn't happen)
		}

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

// scheduleIndexBuild triggers an asynchronous index build for the given chunk
// via the shared scheduler. The build is visible in ScheduledJobs() while running
// and subject to the scheduler's concurrency limit.
// The IndexManager handles deduplication of concurrent builds for the same chunk.
func (o *Orchestrator) scheduleIndexBuild(registryKey string, chunkID chunk.ChunkID) {
	im, ok := o.indexes[registryKey]
	if !ok {
		return
	}

	name := fmt.Sprintf("index-build:%s:%s", registryKey, chunkID)
	if err := o.scheduler.RunOnce(name, im.BuildIndexes, context.Background(), chunkID); err != nil {
		o.logger.Warn("failed to schedule index build", "name", name, "error", err)
	}
}
