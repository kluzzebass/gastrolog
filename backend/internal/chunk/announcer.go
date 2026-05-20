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
	AnnounceCompress(id ChunkID, diskBytes int64)
	// AnnounceAttachOffsets propagates the GLCB blob's section offsets
	// (IngestTS index, SourceTS index) and frame count into the FSM
	// after sealToGLCB has produced data.glcb. Without this, FSM
	// section-offset fields stayed zero until AnnounceUpload — leaving
	// sealed-but-not-yet-uploaded chunks invisible to the histogram's
	// GLCB section-reader path.
	AnnounceAttachOffsets(id ChunkID, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64)
	// AnnounceUpload publishes a successful cloud upload. hash is the GLCB
	// whole-blob digest (32 bytes) read from the TOC footer; cloudServiceID
	// is the cloud service the chunk was actually uploaded to (snapshot,
	// survives later vault reconfiguration); keyScheme selects the
	// blobKey() derivation function (only scheme 0 today). See gastrolog-grnc3.
	AnnounceUpload(id ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, hash [32]byte, cloudServiceID glid.GLID, keyScheme uint8)
	AnnounceDelete(id ChunkID)
}

// AnnouncerSetter is an optional interface for chunk managers that support
// metadata announcements. Callers should type-assert to check availability.
type AnnouncerSetter interface {
	SetAnnouncer(MetadataAnnouncer)
}

// RotationCoordinatorSetter is an optional interface for chunk
// managers that support post-construction injection of a
// RotationCoordinator. Mirrors AnnouncerSetter — the orchestrator
// wires the coordinator after the chunk Manager is built so the
// coordinator can close over per-vault state (FSM, applier, current
// Receiving) without the chunk package depending on the orchestrator.
type RotationCoordinatorSetter interface {
	SetRotationCoordinator(RotationCoordinator)
}

// UploadGate decides whether THIS replica should upload a sealed
// chunk to the shared cloud blob store. Under the fan-out data plane
// every Receiver runs PostSealProcess locally (compress + index),
// but only one replica per vault should actually push to S3 —
// otherwise N replicas race on the same vaultID/chunkID.glcb path,
// multiplying cluster egress bandwidth and risking last-writer-wins
// corruption if any GLCB digest differs.
//
// The orchestrator wires a gate that returns IsRaftLeader() for the
// vault-ctl Raft group: only the elected Raft leader uploads.
// Subsequent CmdUploadChunk applies on every replica, OnUpload fires,
// and the non-uploading replicas mark their local chunks CloudBacked
// without re-uploading.
//
// When nil on the chunk manager Config, all replicas upload — the
// legacy / single-node default.
type UploadGate func() bool

// UploadGateSetter is the optional interface for chunk managers that
// support post-construction injection of an UploadGate. Mirrors
// AnnouncerSetter — the orchestrator wires the gate after build so
// it can close over the vault-ctl Raft leadership callback.
type UploadGateSetter interface {
	SetUploadGate(UploadGate)
}

// ReceivingAnnouncer is the optional extension of MetadataAnnouncer
// that carries the initial Receiving set when announcing a new chunk
// under the fan-out data-plane (gastrolog-2ujjh / gastrolog-nd6sz /
// gastrolog-hshgl). Announcers that do not implement this interface
// produce CmdCreateChunk without an initial Receiving snapshot —
// suitable for single-node / memory / JSONL chunk managers that have
// no cross-node replication path.
//
// receiving is the initial Receiving set: the full placement member
// list for the vault.
type ReceivingAnnouncer interface {
	AnnounceCreateWithReceiving(id ChunkID, writeStart, ingestStart, sourceStart time.Time, receiving []string)
}

// FanOutConfigSetter is the optional interface for chunk managers
// that can be told which Receiving snapshot to stamp on new chunks.
// The orchestrator wires this from VaultConfig at instance build
// time + on every placement change.
type FanOutConfigSetter interface {
	SetFanOutConfig(receiving []string)
}

// RotationCoordinator drives FSM-mediated rotation under the fan-out
// data plane (gastrolog-3yre7). When the chunk manager's rotation
// policy fires, it hands the rotation moment to the coordinator,
// which round-trips through vault-ctl Raft and returns the canonical
// new chunk ID. The chunk manager then seals its current local chunk
// and opens a new one with the returned ID — same ID across every
// replica because Raft serializes proposals and the FSM single-Active
// invariant (vaultctlfsm.ErrActiveChunkExists) discriminates winners
// from losers.
//
// When this interface is nil on the chunk manager Config, the manager
// falls back to the legacy local-mint flow: each rotation picks a
// locally-fresh chunk ID and the announcer proposes via Raft after
// the seal+open completes. Used by single-node, memory, and JSONL
// chunk managers that have no cross-replica synchronization
// requirement.
type RotationCoordinator interface {
	// BeginRotation transitions the FSM from "oldID is Active" to a
	// new Active. Returns the canonical new chunk ID after the
	// underlying Raft entries commit. The caller's candidate ID
	// proposal may or may not win; either way the returned ID is the
	// canonical Active and the caller must use it for the local seal+
	// open.
	//
	// oldID may be the zero value when there's no current Active to
	// transition (e.g., the chunk manager is opening its very first
	// chunk for a vault).
	BeginRotation(oldID ChunkID) (ChunkID, error)
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
