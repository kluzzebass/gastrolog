package orchestrator

import (
	"context"
	"fmt"
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// Ingest filters a record to matching chunk managers.
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

	if len(o.vaults) == 0 && o.forwarder == nil {
		return ErrNoChunkManagers
	}

	if o.filterSet == nil {
		return o.fanoutAll(rec)
	}

	for _, t := range o.filterSet.MatchWithNode(rec.Attrs) {
		if t.NodeID != "" {
			o.forwardRemote(t, rec)
			continue
		}
		if err := o.appendLocal(t.VaultID, rec); err != nil {
			return err
		}
	}
	return nil
}

// fanoutAll is the legacy path: no filter set, send to all local vaults.
func (o *Orchestrator) fanoutAll(rec chunk.Record) error {
	for key, vault := range o.vaults {
		if vault == nil || !vault.Enabled {
			continue
		}
		k := key
		onSeal := func(cid chunk.ChunkID) {
			o.scheduleCompression(k, cid)
			o.scheduleIndexBuild(k, cid)
		}
		if err := vault.Append(rec, onSeal); err != nil {
			return err
		}
	}
	return nil
}

// appendLocal appends a record to a local vault.
func (o *Orchestrator) appendLocal(vaultID uuid.UUID, rec chunk.Record) error {
	vault := o.vaults[vaultID]
	if vault == nil || !vault.Enabled {
		return nil
	}
	onSeal := func(cid chunk.ChunkID) {
		o.scheduleCompression(vaultID, cid)
		o.scheduleIndexBuild(vaultID, cid)
	}
	return vault.Append(rec, onSeal)
}

// forwardRemote ships a record to the node that owns the target vault.
func (o *Orchestrator) forwardRemote(t MatchResult, rec chunk.Record) {
	if err := o.forwarder.Forward(context.Background(), t.NodeID, t.VaultID, []chunk.Record{rec}); err != nil {
		o.logger.Warn("forward record failed", "node", t.NodeID, "vault", t.VaultID, "error", err)
	}
}

// postSealWork schedules compression and index builds for a newly sealed chunk.
// Safe to call from any context (cron rotation, background sweep, etc.) —
// acquires the orchestrator lock internally.
func (o *Orchestrator) postSealWork(storeID uuid.UUID, chunkID chunk.ChunkID) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	o.scheduleCompression(storeID, chunkID)
	o.scheduleIndexBuild(storeID, chunkID)
}

// scheduleCompression triggers an asynchronous compression job for the given chunk
// via the shared scheduler. Only dispatched if the ChunkManager implements ChunkCompressor.
func (o *Orchestrator) scheduleCompression(registryKey uuid.UUID, chunkID chunk.ChunkID) {
	vault := o.vaults[registryKey]
	if vault == nil {
		return
	}

	compressor, ok := vault.Chunks.(chunk.ChunkCompressor)
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
	vault := o.vaults[registryKey]
	if vault == nil {
		return
	}

	// Wrap the index build to refresh chunk disk sizes afterward,
	// since index files are written into the chunk directory.
	buildFn := func(ctx context.Context, id chunk.ChunkID) error {
		if err := vault.Indexes.BuildIndexes(ctx, id); err != nil {
			return err
		}
		if compressor, ok := vault.Chunks.(chunk.ChunkCompressor); ok {
			compressor.RefreshDiskSizes(id)
		}
		return nil
	}

	name := fmt.Sprintf("index-build:%s:%s", registryKey, chunkID)
	if err := o.scheduler.RunOnce(name, buildFn, context.Background(), chunkID); err != nil {
		o.logger.Warn("failed to schedule index build", "name", name, "error", err)
	}
	o.scheduler.Describe(name, fmt.Sprintf("Build indexes for chunk %s", chunkID))
}
