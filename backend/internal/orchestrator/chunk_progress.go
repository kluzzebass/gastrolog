package orchestrator

import (
	"context"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// runChunkProgressEmitter walks every leader vault's active chunk on a
// fixed cadence and emits a PROGRESS event when the chunk's record count
// has advanced since the last tick. One event per active chunk per
// window, regardless of append rate — bounds the typed-event volume so
// busy clusters don't drown the WatchChunks bus.
//
// Coalescing is implicit: each tick reads the current count and compares
// to the last-emitted value per (vault, chunk). Inflight records
// between ticks are reflected in the next tick's count; no events are
// emitted for chunks whose count hasn't changed. Sealed chunks are
// skipped — their final count is carried by the SEALED event.
//
// Followers are skipped: only the leader has appending records; follower
// chunk managers' counts grow via sealed-chunk replication which fires
// its own CREATED / SEALED events. See gastrolog-3pf9w.
func (o *Orchestrator) runChunkProgressEmitter(ctx context.Context, interval time.Duration) {
	last := make(map[glid.GLID]lastSeen)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.emitActiveChunkProgress(last)
		}
	}
}

// emitActiveChunkProgress is the per-tick body of runChunkProgressEmitter,
// extracted so the iteration order (read lock + per-vault Active()
// lookups) is easy to reason about and the goroutine body stays compact.
func (o *Orchestrator) emitActiveChunkProgress(last map[glid.GLID]lastSeen) {
	type snapshot struct {
		VaultID glid.GLID
		Meta    chunk.ChunkMeta
	}
	var snapshots []snapshot

	o.mu.RLock()
	for vaultID, vault := range o.vaults {
		vaultInst := vault.Instance
		if vaultInst == nil || vaultInst.Chunks == nil {
			continue
		}
		// Only the vault-ctl Raft leader emits chunk-progress events
		// so the inspector doesn't double-count active-chunk record
		// counts across every Receiver under fan-out.
		if vaultInst.IsRaftLeader != nil && !vaultInst.IsRaftLeader() {
			continue
		}
		active := vaultInst.Chunks.Active()
		if active == nil || active.Sealed {
			continue
		}
		snapshots = append(snapshots, snapshot{
			VaultID: vaultID,
			Meta:    *active,
		})
	}
	o.mu.RUnlock()

	for _, s := range snapshots {
		count := uint64(s.Meta.RecordCount) //nolint:gosec // G115: bounded by rotation policy
		prev, hadPrev := last[s.VaultID]
		// First sighting of a new active chunk for this vault, or the
		// chunk ID changed since the last tick (rotation happened): reset
		// the high-watermark and emit a fresh PROGRESS so the inspector
		// sees the post-rotation count immediately.
		if !hadPrev || prev.ChunkID != s.Meta.ID {
			last[s.VaultID] = lastSeen{ChunkID: s.Meta.ID, Count: count}
			if count > 0 {
				o.EmitChunkProgress(s.VaultID, s.Meta)
			}
			continue
		}
		// Same active chunk: only emit when the count advanced. No
		// emit for unchanged counts — idle vaults stay quiet.
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
