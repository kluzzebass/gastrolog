package orchestrator

import (
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

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
// into a proto WatchChunksResponse. See gastrolog-3pf9w.
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
