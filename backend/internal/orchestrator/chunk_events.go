package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func (o *Orchestrator) logChunkCreated(vaultID glid.GLID, chunkID chunk.ChunkID) {
	if o == nil || o.vaultOpsLogger == nil {
		return
	}
	o.vaultOpsLogger.Info("chunk created", "vault", vaultID, "chunk", chunkID)
}

func (o *Orchestrator) logChunkDeleted(vaultID glid.GLID, chunkID chunk.ChunkID) {
	if o == nil || o.vaultOpsLogger == nil {
		return
	}
	o.vaultOpsLogger.Info("chunk deleted", "vault", vaultID, "chunk", chunkID)
}

func (o *Orchestrator) logChunkExpunged(vaultID glid.GLID, chunkID chunk.ChunkID, reason string) {
	if o == nil || o.vaultOpsLogger == nil {
		return
	}
	o.vaultOpsLogger.Info("chunk expunged", "vault", vaultID, "chunk", chunkID, "reason", reason)
}

func (o *Orchestrator) logChunkSealed(vaultID glid.GLID, chunkID chunk.ChunkID) {
	if o == nil || o.vaultOpsLogger == nil {
		return
	}
	o.vaultOpsLogger.Info("chunk sealed", "vault", vaultID, "chunk", chunkID)
}

// manifestEntryToChunkMeta builds a chunk.ChunkMeta from the FSM's
// authoritative ManifestEntry. Used by the vault-ctl FSM callbacks
// (OnCreate / OnSeal / OnUpload) to emit ChunkChangeEvents that carry
// the same cluster-wide state on every node — every cluster node's FSM
// applies the same Cmd payload, so events derived from the entry are
// node-independent. Using local Manager.Meta instead would surface
// per-node variance (replication lag) as inspector flicker.
//
// sealed controls the Sealed bool on the result (the FSM tracks State
// separately from the ChunkMeta-side flag). Callers pass true for
// OnSeal/OnUpload and false for OnCreate.
func manifestEntryToChunkMeta(e vaultctlfsm.ManifestEntry, sealed bool) chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          e.ID,
		WriteStart:  e.WriteStart,
		WriteEnd:    e.WriteEnd,
		IngestStart: e.IngestStart,
		IngestEnd:   e.IngestEnd,
		SourceStart: e.SourceStart,
		SourceEnd:   e.SourceEnd,
		RecordCount: e.RecordCount,
		Bytes:       e.Bytes,
		CloudBytes:  e.CloudBytes,
		SealedAt:    e.SealedAt,
		Sealed:      sealed,
		State:       e.State,
		CloudBacked: e.CloudBacked,
		Archived:    e.Archived,
	}
}

// openChunkManifestToChunkMeta projects a pipeline open/sealed manifest into
// chunk metadata for WatchChunks events. Every vault-ctl voter applies the
// same manifest commands, so events derived here are cluster-wide.
func openChunkManifestToChunkMeta(m *vaultctlfsm.OpenChunkManifest, state chunk.ChunkState) chunk.ChunkMeta {
	if m == nil {
		return chunk.ChunkMeta{}
	}
	meta := chunk.ChunkMeta{
		ID:          m.ChunkID,
		WriteStart:  m.OpenedAt,
		IngestStart: m.OpenedAt,
		SourceStart: m.OpenedAt,
		State:       state,
		RecordCount: int64(m.TotalRecords), //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
		Bytes:       int64(m.TotalBytes),   //nolint:gosec // G115: manifest totals fit in int64 for chunk metadata
		Sealed:      state == chunk.ChunkStateSealed,
	}
	vaultctlfsm.ApplyManifestBoundsToChunkMeta(&meta, m.Bounds)
	return meta
}

// ChunkChangeOp identifies what changed about a chunk in a ChunkChangeEvent.
// Subscribers (WatchChunks RPC handler, downstream cluster fan-out) use the
// op to decide how to mutate their projection of vault state: CREATED and
// SEALED carry a full ChunkMeta and replace the cache entry; PROGRESS carries
// only a new record count and patches the active chunk in place; DELETED
// removes the entry; UPLOADED transitions the cloud-backed flag.
type ChunkChangeOp uint8

const (
	// ChunkChangeOpUnspecified is the zero value; never emitted intentionally.
	ChunkChangeOpUnspecified ChunkChangeOp = iota
	// ChunkChangeOpCreated marks a fresh active chunk opening on this node.
	ChunkChangeOpCreated
	// ChunkChangeOpProgress marks the active chunk's record count advancing.
	// Coalesced inside the orchestrator's progress notifier so subscribers
	// see at most one event per active chunk per throttle window even under
	// per-record append rates.
	ChunkChangeOpProgress
	// ChunkChangeOpSealed marks a chunk transitioning active → sealed.
	// Final RecordCount is carried in the Meta snapshot.
	ChunkChangeOpSealed
	// ChunkChangeOpDeleted marks a chunk's removal (retention sweep or
	// operator-triggered). Subscribers should drop the entry from their
	// projection; no Meta is carried.
	ChunkChangeOpDeleted
	// ChunkChangeOpUploaded marks a sealed chunk transitioning local-only →
	// cloud-backed. Meta carries the post-upload snapshot (CloudBacked=true).
	ChunkChangeOpUploaded
)

// ChunkChangeEvent is the typed event broadcast on the orchestrator's chunk
// event bus. The WatchChunks RPC handler subscribes and translates each event
// into a proto WatchChunksResponse.
//
// Field presence by Op:
//
//   - CREATED, SEALED, UPLOADED: Meta is set; RecordCount unused (read from Meta).
//   - PROGRESS: RecordCount is the live count; Meta is nil.
//   - DELETED: both Meta and RecordCount are zero.
type ChunkChangeEvent struct {
	VaultID     glid.GLID
	ChunkID     chunk.ChunkID
	Op          ChunkChangeOp
	Meta        *chunk.ChunkMeta
	RecordCount uint64
}
