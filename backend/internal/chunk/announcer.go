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
	// AnnounceSeal carries the chunk manager's running min IngestTS
	// (ingestStart) and IngestTSMonotonic flag in addition to the seal
	// finalization fields. Both must reach the FSM at seal time: createdAt
	// (which CmdCreateChunk seeded into IngestStart) is wall-clock and
	// can lag the actual record TSs by a tier-transition delay; the
	// monotonic flag is the chunk manager's running observation that's
	// not preserved in the FSM otherwise.
	AnnounceSeal(id ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestStart, ingestEnd, sourceEnd time.Time, ingestTSMonotonic bool)
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
// a chunk WITHOUT firing the metadata announcer. This is used by vault-ctl Raft
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

// SealEnsurer is an optional interface for chunk managers that can project
// the FSM's sealed state onto local files without firing the announcer.
//
// Contract: when EnsureSealed is called for a chunk ID, the local Manager
// MUST end up with that chunk sealed if it exists locally — including the
// case where the chunk is the local active pointer (force-demote: close
// files, mark sealed=true, clear m.active). The FSM is authoritative; if
// it says sealed, the local Manager's stale active pointer must yield.
//
// Why force-demote always (not just on recovery): a previous design split
// this into "steady-state skip-active" + "recovery force-demote" on the
// theory that the leader's record-stream would swap the follower's active
// pointer in steady state. That assumption is topology-dependent — true for
// ingest tiers fed by continuous appends, false for downstream tiers fed
// only by transitions. The skip-active variant left receipt-protocol delete
// obligations bouncing off ErrActiveChunk forever on transition-fed tiers
// (gastrolog-2yeht), and SweepLocalOrphans transitively blocked because no
// tombstone gets created when finalize never fires. The single-method
// always-demote contract is correct for every topology.
//
// Idempotent: a chunk that doesn't exist locally is a silent no-op (this
// node never had it). A chunk that's already locally sealed is a no-op.
type SealEnsurer interface {
	EnsureSealed(id ChunkID) error
}

// DeleteNoAnnounce deletes a chunk from the local store without firing the
// metadata announcer. Used by LOCAL cleanup paths (e.g. replacing a
// forwarded-but-not-yet-canonical chunk, cleaning up orphaned follower
// chunks) that must not propagate the delete via vault-ctl Raft.
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
