package orchestrator

import (
	"context"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// runChunkProgressEmitter walks every vault homed on this node on a fixed
// cadence and emits a PROGRESS event when the open chunk's record count has
// advanced since the last tick. One event per open chunk per window, regardless
// of ingest rate — bounds the typed-event volume so busy clusters don't drown
// the WatchChunks bus.
//
// The record count is read from the vault-ctl FSM's open-chunk manifest
// (OpenChunkSummary().TotalRecords), the pipeline's source of truth for the
// chunk currently accumulating segments. Only the vault-ctl leader emits, so a
// given open chunk produces one PROGRESS stream cluster-wide; followers apply
// the same manifest but stay quiet to avoid duplicate events. Sealed manifests
// carry their final count via the SEALED event and are not re-emitted here.
func (o *Orchestrator) runChunkProgressEmitter(ctx context.Context, interval time.Duration) {
	last := make(map[glid.GLID]lastSeen)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.emitOpenChunkProgress(last)
		}
	}
}

// emitOpenChunkProgress is the per-tick body of runChunkProgressEmitter,
// extracted so the iteration order is easy to reason about and the goroutine
// body stays compact.
func (o *Orchestrator) emitOpenChunkProgress(last map[glid.GLID]lastSeen) {
	o.mu.RLock()
	vaultIDs := make([]glid.GLID, 0, len(o.vaults))
	for vaultID := range o.vaults {
		vaultIDs = append(vaultIDs, vaultID)
	}
	o.mu.RUnlock()

	type snapshot struct {
		VaultID glid.GLID
		Meta    chunk.ChunkMeta
	}
	var snapshots []snapshot

	for _, vaultID := range vaultIDs {
		fsm, _, isLeader, ok := o.vaultCtlHandle(vaultID)
		if !ok || fsm == nil || isLeader == nil || !isLeader() {
			continue
		}
		summary, open := fsm.OpenChunkSummary()
		if !open || summary.TotalRecords == 0 {
			continue
		}
		snapshots = append(snapshots, snapshot{
			VaultID: vaultID,
			Meta: chunk.ChunkMeta{
				ID:          summary.ChunkID,
				RecordCount: int64(summary.TotalRecords), //nolint:gosec // G115: bounded by rotation policy
				Bytes:       int64(summary.TotalBytes),   //nolint:gosec // G115
			},
		})
	}

	for _, s := range snapshots {
		count := uint64(s.Meta.RecordCount) //nolint:gosec // G115: non-negative record count
		prev, hadPrev := last[s.VaultID]
		// First sighting of a new open chunk for this vault, or the chunk ID
		// changed since the last tick (rotation happened): reset the
		// high-watermark and emit a fresh PROGRESS so the inspector sees the
		// post-rotation count immediately.
		if !hadPrev || prev.ChunkID != s.Meta.ID {
			last[s.VaultID] = lastSeen{ChunkID: s.Meta.ID, Count: count}
			o.EmitChunkProgress(s.VaultID, s.Meta)
			continue
		}
		// Same open chunk: only emit when the count advanced. No emit for
		// unchanged counts — idle vaults stay quiet.
		if count > prev.Count {
			last[s.VaultID] = lastSeen{ChunkID: s.Meta.ID, Count: count}
			o.EmitChunkProgress(s.VaultID, s.Meta)
		}
	}
}

// lastSeen tracks the most recent (chunk, count) we emitted PROGRESS for
// on a given vault. Top-level so the test file can reference the type.
type lastSeen struct {
	ChunkID chunk.ChunkID
	Count   uint64
}
