package chunk

import "time"

// MetadataAnnouncer is called by chunk managers after each metadata state
// change. The implementation typically applies the change to a Raft group
// for cluster-wide visibility. All methods are best-effort — failures are
// logged but don't block the local operation.
//
// When nil, no announcements are made (single-node mode, tests).
type MetadataAnnouncer interface {
	AnnounceCreate(id ChunkID, writeStart, ingestStart, sourceStart time.Time)
	AnnounceSeal(id ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestEnd, sourceEnd time.Time)
	AnnounceCompress(id ChunkID, diskBytes int64)
	AnnounceUpload(id ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, numFrames int32)
	AnnounceDelete(id ChunkID)
}

// AnnouncerSetter is an optional interface for chunk managers that support
// metadata announcements. Callers should type-assert to check availability.
type AnnouncerSetter interface {
	SetAnnouncer(MetadataAnnouncer)
}

// AnnouncerGetter retrieves the current announcer from a chunk manager.
type AnnouncerGetter interface {
	GetAnnouncer() MetadataAnnouncer
}

// SilentDeleter is an optional interface for chunk managers that can delete
// a chunk WITHOUT firing the metadata announcer. This is used by tier Raft
// FSM apply paths: when CmdDeleteChunk is applied via Raft on this node,
// we need to delete the local files but must NOT re-announce the delete —
// the announce already happened (it's what put us into this code path).
//
// The contract: DeleteSilent does the same local cleanup as Delete (chunk
// directory + in-memory metadata) but skips the AnnounceDelete call. It
// returns the same errors as Delete (ErrChunkNotFound, ErrActiveChunk, etc).
type SilentDeleter interface {
	DeleteSilent(id ChunkID) error
}

// DeleteNoAnnounce deletes a chunk from the local store without firing the
// metadata announcer. Used by LOCAL cleanup paths (e.g. replacing a
// forwarded-but-not-yet-canonical chunk, cleaning up orphaned follower
// chunks) that must not propagate the delete via tier Raft.
//
// If the manager implements SilentDeleter, this calls DeleteSilent (the
// common case — file.Manager supports it). Otherwise it falls back to
// the regular Delete, which is safe for manager types that do not have
// an announcer wired (e.g. memory, jsonl).
func DeleteNoAnnounce(cm ChunkManager, id ChunkID) error {
	if silent, ok := cm.(SilentDeleter); ok {
		return silent.DeleteSilent(id)
	}
	return cm.Delete(id)
}
