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
// Two methods cover the two distinct contexts in which the FSM's sealed
// state needs to be projected; each has different correctness invariants:
//
//   - EnsureSealed (steady state): called from the FSM apply path on
//     every node when CmdSealChunk commits. On followers, the local
//     active pointer typically still points at the just-sealed chunk
//     because the leader's per-(tier, follower) record-stream lags the
//     vault-ctl Raft broadcast by a few ms. The record-stream's next
//     TierReplicationAppend (for the new active chunk) is what
//     authoritatively swaps the active pointer; EnsureSealed just sets
//     the sealed flag on disk if the chunk exists and is not the local
//     active. Skipping the active case avoids double-sealing the
//     in-flight active across the natural rotation boundary.
//
//   - EnsureSealedAndDemote (recovery): called from
//     ReconcileFromSnapshot's projectAllSealedFromFSM walk after a
//     follower restores from a Raft snapshot. The record-stream that
//     would have swapped the active pointer in steady state is *gone*
//     for any chunk sealed in this node's absence — there's no
//     forthcoming TierReplicationAppend to do the swap. So if the
//     local active matches a now-sealed chunk, we MUST force-demote
//     it: close files, mark sealed=true, clear m.active. Otherwise
//     subsequent appends keep landing on a chunk the cluster considers
//     immutable (gastrolog-uccg6's 53K-records-on-a-10K-cap incident).
//
// In both methods, a chunk that doesn't exist locally is a silent no-op
// (this node never had it), and a chunk that's already locally sealed
// is also a no-op.
type SealEnsurer interface {
	EnsureSealed(id ChunkID) error
	EnsureSealedAndDemote(id ChunkID) error
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
