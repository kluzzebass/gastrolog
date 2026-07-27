package chunk

import (
	"time"

	"gastrolog/internal/glid"
)

// MetadataAnnouncer is called by chunk managers after each metadata state
// change. The implementation typically applies the change to a Raft group
// for cluster-wide visibility. All methods are best-effort — failures are
// logged but don't block the local operation.
//
// When nil, no announcements are made (single-node mode, tests).
type MetadataAnnouncer interface {
	AnnounceCreate(id ChunkID, writeStart, ingestStart, sourceStart time.Time)
	// AnnounceBeginSeal fires the Active → Sealing transition before
	// the chunk manager's sealToGLCB runs. Lets followers and
	// retention/upload code observe the in-flight assembly window.
	// gastrolog-1huz5.
	AnnounceBeginSeal(id ChunkID)
	// AnnounceSeal carries the chunk manager's running min IngestTS
	// (ingestStart) and IngestTSMonotonic flag in addition to the seal
	// finalization fields. Both must reach the FSM at seal time: createdAt
	// (which CmdCreateChunk seeded into IngestStart) is wall-clock and
	// can lag the actual record TSs by a retention-routing delay; the
	// monotonic flag is the chunk manager's running observation that's
	// not preserved in the FSM otherwise.
	AnnounceSeal(id ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestStart, ingestEnd, sourceEnd time.Time, ingestTSMonotonic bool)
	// AnnounceAttachOffsets propagates the GLCB blob's section offsets
	// (IngestTS index, SourceTS index) and frame count into the FSM
	// after sealToGLCB has produced data.glcb. Without this, FSM
	// section-offset fields stayed zero until AnnounceUpload — leaving
	// sealed-but-not-yet-uploaded chunks invisible to the histogram's
	// GLCB section-reader path.
	AnnounceAttachOffsets(id ChunkID, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64)
	// AnnounceUpload publishes a successful cloud upload. cloudBytes is the
	// compressed cloud object's transport size (was misleadingly passed
	// through as "diskBytes"; local warm-cache footprint is per-node state
	// that never belonged on this cluster-replicated announce — see
	// gastrolog-33ul6h). hash is the GLCB whole-blob digest (32 bytes) read
	// from the TOC footer; cloudServiceID is the cloud service the chunk
	// was actually uploaded to (snapshot, survives later vault
	// reconfiguration); keyScheme selects the blobKey() derivation
	// function (only scheme 0 today). See gastrolog-grnc3.
	AnnounceUpload(id ChunkID, cloudBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, hash [32]byte, cloudServiceID glid.GLID, keyScheme uint8)

	// AnnounceArchived records the cloud storage class a chunk's blob now
	// sits in; an empty class means restored to standard storage. Only the
	// node that called the cloud API knows this, so without replicating it
	// every other node — and this node after a restart — cannot tell what
	// class a chunk is in. That is what stalled multi-step transition
	// chains: the archival sweep compares current class against the chain's
	// target, and an unreplicated class reads as empty forever
	// (gastrolog-35ygqv).
	AnnounceArchived(id ChunkID, storageClass string)
}

// AnnouncerSetter is an optional interface for chunk managers that support
// metadata announcements. Callers should type-assert to check availability.
type AnnouncerSetter interface {
	SetAnnouncer(MetadataAnnouncer)
}

// IntegrityVerifier reports the expected GLCB whole-blob digest for a chunk
// (the value the FSM stamped onto CmdUploadChunk via gastrolog-grnc3). The
// chunk manager calls ExpectedDigest after every cold-cache cloud download
// and rejects any blob whose actual digest doesn't match. The (zero, false)
// return path means "no expectation on file" — used during the migration
// from pre-grnc3 entries that have no recorded hash; a nil verifier on the
// Manager Config disables verification entirely (single-node tests).
type IntegrityVerifier interface {
	ExpectedDigest(id ChunkID) ([32]byte, bool)
}

// IntegrityVerifierSetter is an optional interface for chunk managers that
// support post-construction injection of an IntegrityVerifier. Mirrors
// AnnouncerSetter — the orchestrator wires the manifest-backed verifier
// after the chunk Manager is built.
type IntegrityVerifierSetter interface {
	SetIntegrityVerifier(IntegrityVerifier)
}

// AnnouncerGetter retrieves the current announcer from a chunk manager.
type AnnouncerGetter interface {
	GetAnnouncer() MetadataAnnouncer
}

// SilentDeleter is an optional interface for chunk managers that can delete
// a chunk WITHOUT firing the metadata announcer. Used by the receipt-protocol
// delete path (VaultLifecycleReconciler): when a node fulfills a delete
// obligation, it removes the local files but must NOT re-announce — the
// vault-ctl FSM already drove the delete cluster-wide.
//
// The contract: DeleteSilent does the same local cleanup as Delete (chunk
// directory + in-memory metadata). It returns the same errors as Delete
// (ErrChunkNotFound, ErrActiveChunk, etc).
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
// ingest vaults fed by continuous appends, false for downstream vaults fed
// only by transitions. The skip-active variant left receipt-protocol delete
// obligations bouncing off ErrActiveChunk forever on retention-fed vaults
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

// SealNoAnnounce seals the given chunk locally without firing the metadata
// announcer. Used by LOCAL teardown paths (placement loss, vault instance
// removal) that need the active chunk demoted so it can be deleted, but must
// not propagate a lifecycle transition via vault-ctl Raft.
//
// Seal() is the wrong primitive there: it fires AnnounceBeginSeal (Active →
// Sealing) and leaves the matching AnnounceSeal to the post-seal pipeline,
// which a teardown never runs — so the cluster-wide manifest entry parks in
// Sealing while this node throws its bytes away.
//
// If the manager implements SealEnsurer, this calls EnsureSealed (the common
// case — file.Manager supports it, and its contract is force-demote the local
// active without announcing). Otherwise it falls back to Seal(), which is safe
// for manager types that have no announcer wired (memory, jsonl).
func SealNoAnnounce(cm ChunkManager, id ChunkID) error {
	if ensurer, ok := cm.(SealEnsurer); ok {
		return ensurer.EnsureSealed(id)
	}
	return cm.Seal()
}
