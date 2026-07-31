package vaultctlfsm

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// Command identifies the type of chunk metadata mutation. Commands are
// encoded as gastrologv1.VaultCtlCommand (a oneof) rather than an opcode
// byte, so these constants are not part of any wire encoding; they are
// retained as the canonical record of the opcode→proto-field tag mapping
// (the oneof field numbers in vaultctlfsm.proto deliberately match these
// values) and as the anchor for the domain documentation below.
type Command byte

const (
	CmdCreateChunk Command = 1
	CmdSealChunk   Command = 2
	CmdUploadChunk Command = 3

	CmdRetentionPending Command = 4

	// Receipt-based deletion protocol: an N-way receipt exchange that
	// survives snapshot install and gives every node a first-class "delete
	// locally and ack" obligation. This is the sole chunk-delete path. See
	// docs in fsm_receipts.go.
	CmdRequestDelete  Command = 5 // vault leader proposes a delete; replicates the expected-acks set
	CmdAckDelete      Command = 6 // each expected node acks after handling its local side
	CmdFinalizeDelete Command = 7 // leader removes the entry once expectedFrom is empty

	// Membership-change cleanup. After a node is removed from this
	// vault-ctl Raft group's voter set (decommissioned or rebalanced
	// away), the leader proposes CmdPruneNode to drop that node's slot
	// from every pendingDeletes entry's ExpectedFrom. Without this,
	// deletes proposed before the node left would never finalize: the
	// leader would hold the entry forever waiting for an ack from a node
	// that no longer participates.
	CmdPruneNode Command = 8

	// CmdAttachOffsets carries the GLCB blob's section-offset/size pairs
	// (IngestTS index, SourceTS index) once sealToGLCB has produced
	// data.glcb on the leader. Without it the offsets would only land in
	// the FSM via CmdUploadChunk, leaving sealed-but-not-yet-uploaded
	// chunks with zero offsets in the manifest and breaking the
	// histogram's GLCB section-reader path. Fires from chunk/file.Manager
	// after sealToGLCB and from the import path
	// (orchestrator/vault_ops.finalizeImportedChunk) for replicated chunks.
	CmdAttachOffsets Command = 9

	// CmdBeginSeal carries the Active → Sealing transition: the leader
	// has stopped accepting appends on the chunk and is starting
	// sealed-form assembly. Distinct from CmdSealChunk so the cluster
	// observes the in-flight assembly window explicitly — uploads,
	// retention triggers, and replication-completeness checks gate on
	// State == Sealed; the Sealing entry is not yet a finished chunk
	// even though it's no longer accepting writes.
	CmdBeginSeal Command = 10

	// CmdRepatriateChunk re-introduces a sealed chunk's manifest entry
	// when the FSM has lost it but a local replica still exists on
	// disk (operator-driven recovery from FSM glitches, restore-from-
	// backup desync, etc.). Payload is the full ManifestEntry
	// reconstructed from the local chunk's idx.log headers. Apply
	// inserts the entry in Sealed state, refusing if the entry
	// already exists or is tombstoned.
	CmdRepatriateChunk Command = 11

	// CmdPublishCompletedSegment registers completed segment metadata in the
	// vault-ctl FSM when Segmentation closes a segment.
	CmdPublishCompletedSegment Command = 12

	// Open-chunk manifest (direction D).
	CmdOpenChunkManifest      Command = 13
	CmdAddOpenChunkSegmentRef Command = 14
	CmdSealOpenChunkManifest  Command = 15
	CmdReleaseSegments        Command = 16

	// CmdAckSegmentHolder records that a node now holds a completed segment
	// (Rubicon C). Appends the node to the segment registry entry's holder
	// set; idempotent.
	CmdAckSegmentHolder Command = 17

	// CmdPublishCompletedSegments registers many completed segments in one
	// vault-ctl apply (burst ingest on origin nodes).
	CmdPublishCompletedSegments Command = 19

	// CmdAddOpenChunkSegmentRefs appends many open-chunk segment refs in one
	// vault-ctl apply (chunking planner batching).
	CmdAddOpenChunkSegmentRefs Command = 20

	// CmdAckChunkHolder records that a node holds a sealed chunk's verified
	// GLCB bytes (built locally or pulled via replica catch-up). Idempotent.
	CmdAckChunkHolder Command = 21

	// CmdRevokeChunkHolder withdraws a holder claim after the node
	// stat-missed the bytes it was recorded as holding. Idempotent.
	CmdRevokeChunkHolder Command = 22

	// CmdClearTransferSource clears a manifest entry's
	// TransferSourceVaultID once the destination has confirmed enough
	// holder receipts that the source vault is about to expire its own
	// copies. Proposed by the SOURCE vault's retention runner against
	// the DESTINATION vault's FSM, right before the source's local
	// expire. Idempotent; a no-op if the entry is missing or the field is
	// already clear. See pullMissingGLCB / runGLCBPull (glcb_catchup.go)
	// for the defense-in-depth other half: a holder-set fallback when this
	// clear was missed (crash, apply failure) and a pull still gets
	// addressed at an already-expired source.
	CmdClearTransferSource Command = 24

	// CmdArchiveChunk records the cloud storage class a chunk's blob now
	// sits in. Replicating it is what lets the archival sweep's "already at
	// the target class?" test read authoritative state on any voter, so a
	// multi-step transition chain (cold -> deep-freeze) advances past its
	// first step. Idempotent; an empty class means restored to standard
	// storage.
	CmdArchiveChunk Command = 25
)

// ManifestEntry holds the full metadata for one chunk in this vault's
// manifest (the FSM's set of chunks for one vault — see Manifest in
// docs/ubiquitous_language.md). Every chunk in the cluster is described by
// exactly one ManifestEntry, mutated only by the Cmd* applies. This is the
// Raft-replicated equivalent of file.Manager.chunkMeta + cloudIdx entries
// — and the source of truth they project from.
type ManifestEntry struct {
	ID          chunk.ChunkID
	WriteStart  time.Time
	WriteEnd    time.Time
	RecordCount int64
	Bytes       int64
	// State is the chunk's lifecycle state (Active|Sealing|Sealed).
	// Consumers that just want "is the cluster done sealing this chunk?"
	// check `State == chunk.ChunkStateSealed`.
	// ChunkMeta still carries a separate `Sealed` bool because its
	// semantics differ — ChunkMeta.Sealed is the LOCAL active-form-closed
	// signal, distinct from this cluster-wide lifecycle state.
	State chunk.ChunkState
	// CloudBytes is the compressed cloud object's transport size, set only
	// by CmdUploadChunk (0 until uploaded). This entry never carried a real
	// per-node local-disk-bytes fact — ManifestEntry is Raft-replicated
	// cluster state, and local warm-cache footprint is node-local and
	// lives in file.Manager's own chunkMeta/cloudIdx instead.
	CloudBytes int64

	IngestStart time.Time
	IngestEnd   time.Time
	SourceStart time.Time
	SourceEnd   time.Time

	SealedAt time.Time // wall-clock sealing completion (FSM / retention anchor)

	// Holders are node IDs holding verified GLCB bytes for this chunk —
	// earned via AckChunkHolder (local build or replica catch-up pull),
	// withdrawn via RevokeChunkHolder (stat-missed bytes). Residency truth;
	// placement says who SHOULD hold. Treated as immutable once stored:
	// apply functions copy-on-write so the shallow entry copies handed out
	// by Get/List never race with later applies.
	Holders []string

	// TransferSourceVaultID is non-zero only for a chunk introduced via
	// retention transfer disposition: the vault ID this chunk's bytes
	// still need to be pulled FROM. Zero value for every normally-chunked
	// or same-vault repatriated entry. Consulted by the
	// GLCB replica catch-up sweep (pullMissingGLCB) to address its pull
	// at the SOURCE vault's chunk root instead of this vault's own
	// placement peers. Cleared (CmdClearTransferSource) by the source's
	// retention runner once destination receipts meet RF, right before
	// the source expires its own copies — left set past that point,
	// every FUTURE replica-repair pull would keep addressing a vault
	// that has nothing left to give. pullMissingGLCB / runGLCBPull carry
	// a holder-set fallback as defense in depth for a missed clear (a
	// crash between receipts-met and the clear). See
	// docs/retention-transfer-disposition-design.md "Replica repair
	// after completion".
	TransferSourceVaultID glid.GLID

	// IngestTSMonotonic is true when records were appended in IngestTS-
	// ascending order; the histogram fast path uses position-as-rank only
	// when this is true. Set by CmdSealChunk from the chunk manager's
	// running flag (which only flips one direction, true → false).
	IngestTSMonotonic bool

	CloudBacked bool
	// CloudStorageClass is the cloud archival storage class this chunk
	// currently sits in ("GLACIER", "cold"), as last announced by
	// CmdArchiveChunk. Empty
	// means standard storage. Archived is derived from it rather than tracked
	// separately, so the two can never disagree.
	//
	// NOT "StorageClass": that name belongs to the uint32 local class on
	// FileStorage / CloudService / VaultConfig, which selects which disk a
	// vault may live on. Different concept, same words — see
	// docs/ubiquitous_language.md.
	CloudStorageClass string
	Archived          bool
	RetentionPending  bool

	// Cloud-specific TOC offsets (GLCB format).
	IngestIdxOffset int64
	IngestIdxSize   int64
	SourceIdxOffset int64
	SourceIdxSize   int64

	// Integrity fields populated at upload time, verified on every cache
	// re-fetch.
	//
	// Hash is the GLCB whole-blob digest from the TOC footer:
	// sha256(header ‖ section_hashes_in_TOC_order ‖ TOC_bytes). Re-fetch
	// verification re-derives this from the on-disk file's footer + TOC
	// section hashes — O(1) work regardless of blob size — and rejects on
	// mismatch.
	Hash [32]byte
	// CloudServiceID pins the chunk to the cloud store it was actually
	// uploaded to, surviving any future vault reconfiguration that points
	// the vault at a different cloud service.
	CloudServiceID glid.GLID
	// KeyScheme selects from the table of blobKey() derivation functions.
	// Today only scheme 0 exists ("vault-<vault>/<chunk>.glcb"); future
	// schemes (date-prefixed, hash-sharded, multi-bucket) won't render
	// existing entries ambiguous.
	KeyScheme uint8
}

// IsSealed reports whether the chunk has reached the cluster-wide
// Sealed state (GLCB committed). For ChunkMeta produced from this entry,
// ChunkMeta.Sealed carries the same answer for downstream consumers — but
// those consumers must remember that ChunkMeta from the local chunk
// Manager (un-overlaid) has DIFFERENT semantics: it's the local
// active-form-closed signal, which flips earlier.
func (e *ManifestEntry) IsSealed() bool {
	return e.State == chunk.ChunkStateSealed
}

// ToChunkMeta converts to the public chunk.ChunkMeta type. DiskBytes is
// deliberately left zero: it's per-node live warm-cache state that only
// file.Manager's own chunkMeta/cloudIdx track, and this ManifestEntry-
// sourced meta has no local Manager behind it. CloudBytes (the cloud
// object size) is the one size fact the FSM actually carries for an
// uploaded chunk.
func (e *ManifestEntry) ToChunkMeta() chunk.ChunkMeta {
	state := e.State
	return chunk.ChunkMeta{
		ID:                e.ID,
		WriteStart:        e.WriteStart,
		WriteEnd:          e.WriteEnd,
		SealedAt:          e.SealedAt,
		RecordCount:       e.RecordCount,
		Bytes:             e.Bytes,
		Sealed:            state == chunk.ChunkStateSealed,
		State:             state,
		CloudBytes:        e.CloudBytes,
		IngestStart:       e.IngestStart,
		IngestEnd:         e.IngestEnd,
		SourceStart:       e.SourceStart,
		SourceEnd:         e.SourceEnd,
		IngestTSMonotonic: e.IngestTSMonotonic,
		CloudBacked:       e.CloudBacked,
		Archived:          e.Archived,
		CloudStorageClass: e.CloudStorageClass,
	}
}

// FSM is a Raft FSM that maintains chunk metadata for a single vault.
// All reads are local (no Raft round-trip). Writes go through Raft.Apply().
type FSM struct {
	mu     sync.RWMutex
	chunks map[chunk.ChunkID]*ManifestEntry

	// completedListScans counts ListCompletedSegments calls — a full O(N)
	// registry walk. The chunking-leader plan pass regressed to O(N^2) by
	// re-scanning per step; this makes scan frequency observable so that
	// regression cannot return unnoticed.
	completedListScans atomic.Uint64

	ready    bool                // true after first Apply or Restore
	onUpload func(ManifestEntry) // called after CmdUploadChunk applies (outside lock)

	// Reconciler-wiring hooks. Each fires outside the FSM mutex after the
	// corresponding Cmd applies, so the reconciler can project FSM state
	// changes into local Manager state without polling.
	onCreate func(ManifestEntry) // CmdCreateChunk applied; passes the freshly-created entry
	// onSeal fan-out: slot 0 is SetOnSeal (reconciler); AddOnSeal uses ids ≥ 1.
	onSeal             map[int]func(ManifestEntry)
	onSealSeq          int
	onRetentionPending func(chunk.ChunkID) // CmdRetentionPending applied
	// onHoldersChanged fires (outside the FSM lock) once per chunk whose
	// Holders set actually changed under CmdAckChunkHolder /
	// CmdRevokeChunkHolder, with the post-apply entry. Residency is
	// receipt-only, so subscribers — the WatchChunks bus — need an edge
	// when receipts land or revoke; without it the inspector's honest
	// pre-receipt amber never turns green until a full snapshot refetch.
	// Idempotent re-acks don't fire.
	onHoldersChanged func(ManifestEntry)

	// Receipt-protocol state and hooks. pendingDeletes is the queue of
	// chunk deletes awaiting per-node acknowledgement. See PendingDelete
	// and the apply* functions in fsm_receipts.go.
	pendingDeletes map[chunk.ChunkID]*PendingDelete

	onRequestDelete  func(PendingDelete)           // CmdRequestDelete applied; passes a copy of the new entry
	onAckDelete      func(chunk.ChunkID, string)   // CmdAckDelete applied; (chunkID, ackingNodeID)
	onFinalizeDelete func(chunk.ChunkID)           // CmdFinalizeDelete applied; expectedFrom was empty
	onPruneNode      func(string, []chunk.ChunkID) // CmdPruneNode applied; (prunedNodeID, finalizableChunks)

	// tombstones records chunk IDs that have been deleted, with the apply
	// timestamp of the delete. Consulted by the receive side of vault
	// replication to reject stale ImportSealed / Append commands that
	// arrive after a chunk has been deleted — closes the race between
	// retention and post-seal replication where a late replication RPC
	// could otherwise recreate a "ghost" chunk on a follower.
	//
	// Periodically pruned by the orchestrator (entries older than the
	// replication-job deadline, typically a few minutes, cannot still be
	// in flight and are safe to drop).
	tombstones map[chunk.ChunkID]time.Time

	// completedSegments is the pipeline registry of closed segments awaiting
	// chunking. Populated by CmdPublishCompletedSegment; see segments.go.
	completedSegments map[glid.GLID]*CompletedSegmentEntry
	// completedSegmentOrder is FirstIngestTS-then-ID sort order; kept in sync
	// with completedSegments so ListCompletedSegments avoids re-sorting.
	completedSegmentOrder []glid.GLID

	// openChunk is the in-progress manifest-backed active chunk. See open_chunk.go.
	openChunk *OpenChunkManifest
	// sealedManifests is the FIFO queue of sealed open-chunk manifests awaiting
	// local GLCB build. SealedManifest() returns the queue head.
	sealedManifests []*OpenChunkManifest
	// segmentResume maps segment ID → next EventID-order record number after
	// a partial manifest ref.
	segmentResume map[glid.GLID]uint32
	// segmentChunks maps a completed segment ID → the chunk IDs whose manifests
	// referenced its records. Unlike openChunk/sealedManifests it survives the
	// manifest pop at build time, so SegmentSuperseded can decide release from
	// chunk replication (records live in an RF-replicated chunk) after the build.
	// Cleared when the segment is released.
	segmentChunks map[glid.GLID][]chunk.ChunkID
	// releasedSegments records segment IDs dropped by ReleaseSegments. Stale
	// PublishCompletedSegment replays after release must not re-add registry
	// entries without on-disk bytes (distribution publish race).
	releasedSegments map[glid.GLID]struct{}

	// onSealedManifest fires after SealOpenChunkManifest transitions open →
	// sealed manifest awaiting local GLCB build (outside the FSM lock).
	// Slot 0 is reserved for SetOnSealedManifest; AddOnSealedManifest uses
	// monotonic ids ≥ 1 so orchestrator chunk-bus wiring can coexist with
	// the chunking manager's primary callback.
	onSealedManifest    map[int]func(*OpenChunkManifest)
	onSealedManifestSeq int
	// onSealedManifestCleared fires after CmdSealChunk clears the pending
	// sealed manifest, signalling the build cycle finished (outside the FSM
	// lock). The chunking planner uses it to start the next manifest.
	onSealedManifestCleared func(chunk.ChunkID)
	// onPublishCompletedSegment fan-out: each registered callback fires after a
	// new completed segment is registered (outside the FSM lock). Idempotent
	// replays do not fire. Keyed by a monotonic id so Collection and Chunking
	// can subscribe and unsubscribe independently.
	onPublishCompletedSegment map[int]func(CompletedSegmentEntry)
	onPublishSeq              int
	// onOpenChunkManifest fires after a new open-chunk manifest is created.
	// Slot 0 is reserved for SetOnOpenChunkManifest; AddOnOpenChunkManifest
	// uses monotonic ids ≥ 1.
	onOpenChunkManifest    map[int]func(*OpenChunkManifest)
	onOpenChunkManifestSeq int
	// onOpenChunkRefAdded fires after a new segment ref is appended to the open manifest.
	onOpenChunkRefAdded    map[int]func(*OpenChunkManifest)
	onOpenChunkRefAddedSeq int
	// onReleaseSegments fan-out: each subscriber fires after ReleaseSegments drops
	// registry entries (outside the FSM lock).
	onReleaseSegments map[int]func([]glid.GLID)
	onReleaseSeq      int
	// onAckSegmentHolder fan-out: fires after a new holder is recorded for a segment.
	onAckSegmentHolder map[int]func(glid.GLID)
	onAckHolderSeq     int
}

// New creates an empty chunk metadata FSM.
func New() *FSM {
	return &FSM{
		chunks:            make(map[chunk.ChunkID]*ManifestEntry),
		tombstones:        make(map[chunk.ChunkID]time.Time),
		pendingDeletes:    make(map[chunk.ChunkID]*PendingDelete),
		completedSegments: make(map[glid.GLID]*CompletedSegmentEntry),
		segmentResume:     make(map[glid.GLID]uint32),
		segmentChunks:     make(map[glid.GLID][]chunk.ChunkID),
		releasedSegments:  make(map[glid.GLID]struct{}),
	}
}

// IsTombstoned reports whether a chunk has been deleted and is still
// within the tombstone retention window. Used by the replication receiver
// to reject late commands that would otherwise recreate a deleted chunk.
func (f *FSM) IsTombstoned(id chunk.ChunkID) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	_, ok := f.tombstones[id]
	return ok
}

// PruneTombstones removes tombstone entries whose delete time is older
// than the given cutoff. Returns the number of entries pruned. Intended
// to be called periodically from a non-Raft path on the leader — this
// mutation is local only (not raft-replicated) because every node
// independently applies identical tombstones via the delete command
// and can safely prune them independently once the replication window
// has elapsed.
func (f *FSM) PruneTombstones(before time.Time) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for id, ts := range f.tombstones {
		if ts.Before(before) {
			delete(f.tombstones, id)
			n++
		}
	}
	return n
}

var _ hraft.FSM = (*FSM)(nil)

// Ready returns true after the FSM has applied at least one log entry or
// restored from a snapshot. Before that, the FSM state may be incomplete
// and should not be used for authoritative decisions (e.g. follower
// reconciliation should not delete chunks based on a not-yet-ready manifest).
func (f *FSM) Ready() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.ready
}

// SetOnCreate registers a callback invoked (outside the FSM lock) after
// CmdCreateChunk applies. The callback receives the freshly-created
// manifest entry. Used by the WatchChunks event bus to emit CREATED
// events as soon as a new active chunk is announced across the cluster.
func (f *FSM) SetOnCreate(fn func(ManifestEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onCreate = fn
}

// SetOnUpload registers a callback invoked (outside the FSM lock) after
// CmdUploadChunk applies. The callback receives a copy of the uploaded
// entry. Follower nodes use this to register cloud-backed chunks in their local
// chunk manager without streaming any records.
func (f *FSM) SetOnUpload(fn func(ManifestEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onUpload = fn
}

// SetOnHoldersChanged registers a callback invoked (outside the FSM lock)
// once per chunk whose holder set actually changed under an
// AckChunkHolder / RevokeChunkHolder apply, with a copy of the
// post-apply entry. The orchestrator wires this to the chunk event bus
// so WatchChunks subscribers see residency grow as copy-seal receipts
// land (and shrink on revoke) instead of waiting for a snapshot refetch.
func (f *FSM) SetOnHoldersChanged(fn func(ManifestEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onHoldersChanged = fn
}

// SetOnSeal registers a callback invoked (outside the FSM lock) after
// CmdSealChunk applies. The callback receives a copy of the now-sealed
// entry. The reconciler uses this to project the FSM-side seal into the
// local Manager's chunk meta, so the local manager cannot go on treating
// a cluster-sealed chunk as active.
//
// Fires once per actual seal apply. A re-apply (log replay over an
// already-sealed entry) still fires the callback because the FSM
// idempotently re-writes the seal fields; the reconciler is expected
// to be idempotent in turn.
func (f *FSM) SetOnSeal(fn func(ManifestEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onSeal == nil {
		f.onSeal = make(map[int]func(ManifestEntry))
	}
	if fn == nil {
		delete(f.onSeal, 0)
		return
	}
	f.onSeal[0] = fn
}

// AddOnSeal registers an additional callback invoked (outside the FSM lock)
// after CmdSealChunk applies. The returned closure removes this subscriber
// without disturbing SetOnSeal's slot-0 callback (reconciler).
func (f *FSM) AddOnSeal(fn func(ManifestEntry)) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onSeal == nil {
		f.onSeal = make(map[int]func(ManifestEntry))
	}
	id := f.onSealSeq
	if id == 0 {
		id = 1
	}
	f.onSealSeq++
	if f.onSealSeq == 0 {
		f.onSealSeq = 1
	}
	f.onSeal[id] = fn
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.onSeal, id)
	}
}

// SetOnSealedManifest registers a callback invoked (outside the FSM lock)
// after SealOpenChunkManifest transitions the open manifest into
// sealedManifest awaiting local GLCB build. Idempotent replays of an
// already-sealed manifest do not fire the callback.
func (f *FSM) SetOnSealedManifest(fn func(*OpenChunkManifest)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onSealedManifest == nil {
		f.onSealedManifest = make(map[int]func(*OpenChunkManifest))
	}
	if fn == nil {
		delete(f.onSealedManifest, 0)
		return
	}
	f.onSealedManifest[0] = fn
}

// AddOnSealedManifest registers an additional callback invoked (outside the
// FSM lock) after SealOpenChunkManifest transitions the open manifest into
// sealedManifest. Idempotent replays do not fire. The returned closure
// removes this subscriber without disturbing SetOnSealedManifest's slot-0
// callback (chunking manager).
func (f *FSM) AddOnSealedManifest(fn func(*OpenChunkManifest)) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onSealedManifest == nil {
		f.onSealedManifest = make(map[int]func(*OpenChunkManifest))
	}
	id := f.onSealedManifestSeq
	if id == 0 {
		id = 1
	}
	f.onSealedManifestSeq++
	if f.onSealedManifestSeq == 0 {
		f.onSealedManifestSeq = 1
	}
	f.onSealedManifest[id] = fn
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.onSealedManifest, id)
	}
}

// SetOnSealedManifestCleared registers a callback invoked (outside the FSM
// lock) after CmdSealChunk clears the pending sealed manifest (GLCB build
// completed cluster-wide). The chunking planner uses this to wake and open
// the next manifest; without it, remaining published segments are only
// picked up when a future segment publish happens to arrive.
func (f *FSM) SetOnSealedManifestCleared(fn func(chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onSealedManifestCleared = fn
}

// AddOnAckSegmentHolder registers a callback invoked (outside the FSM lock)
// after AckSegmentHolder appends a new node to a segment's holder set.
// Idempotent replays for an already-recorded holder do not fire.
func (f *FSM) AddOnAckSegmentHolder(fn func(glid.GLID)) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onAckSegmentHolder == nil {
		f.onAckSegmentHolder = make(map[int]func(glid.GLID))
	}
	id := f.onAckHolderSeq
	f.onAckHolderSeq++
	f.onAckSegmentHolder[id] = fn
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.onAckSegmentHolder, id)
	}
}

// AddOnReleaseSegments registers a callback invoked (outside the FSM lock)
// after ReleaseSegments drops completed-segment registry entries. The
// callback receives the segment IDs that were present and removed.
// Idempotent replays for already-released segments do not fire.
func (f *FSM) AddOnReleaseSegments(fn func([]glid.GLID)) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onReleaseSegments == nil {
		f.onReleaseSegments = make(map[int]func([]glid.GLID))
	}
	id := f.onReleaseSeq
	f.onReleaseSeq++
	f.onReleaseSegments[id] = fn
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.onReleaseSegments, id)
	}
}

// AddOnPublishCompletedSegment registers a callback invoked (outside the FSM
// lock) after PublishCompletedSegment registers new segment metadata. Idempotent
// replays of an already-present entry do not fire the callback. Multiple
// subscribers (Collection, Chunking) may register independently; the returned
// closure removes this one.
func (f *FSM) AddOnPublishCompletedSegment(fn func(CompletedSegmentEntry)) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onPublishCompletedSegment == nil {
		f.onPublishCompletedSegment = make(map[int]func(CompletedSegmentEntry))
	}
	id := f.onPublishSeq
	f.onPublishSeq++
	f.onPublishCompletedSegment[id] = fn
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.onPublishCompletedSegment, id)
	}
}

// SetOnOpenChunkManifest registers a callback invoked (outside the FSM lock)
// after OpenChunkManifest creates a new open manifest. Idempotent replays do
// not fire the callback.
func (f *FSM) SetOnOpenChunkManifest(fn func(*OpenChunkManifest)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onOpenChunkManifest == nil {
		f.onOpenChunkManifest = make(map[int]func(*OpenChunkManifest))
	}
	if fn == nil {
		delete(f.onOpenChunkManifest, 0)
		return
	}
	f.onOpenChunkManifest[0] = fn
}

// AddOnOpenChunkManifest registers an additional callback invoked (outside the
// FSM lock) after OpenChunkManifest creates a new open manifest. Idempotent
// replays do not fire. The returned closure removes this subscriber without
// disturbing SetOnOpenChunkManifest's slot-0 callback (chunking manager).
func (f *FSM) AddOnOpenChunkManifest(fn func(*OpenChunkManifest)) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onOpenChunkManifest == nil {
		f.onOpenChunkManifest = make(map[int]func(*OpenChunkManifest))
	}
	id := f.onOpenChunkManifestSeq
	if id == 0 {
		id = 1
	}
	f.onOpenChunkManifestSeq++
	if f.onOpenChunkManifestSeq == 0 {
		f.onOpenChunkManifestSeq = 1
	}
	f.onOpenChunkManifest[id] = fn
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.onOpenChunkManifest, id)
	}
}

// SetOnOpenChunkRefAdded registers a callback invoked (outside the FSM lock)
// after AddOpenChunkSegmentRef appends a new ref. Idempotent replays of the
// same ref do not fire the callback.
func (f *FSM) SetOnOpenChunkRefAdded(fn func(*OpenChunkManifest)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onOpenChunkRefAdded == nil {
		f.onOpenChunkRefAdded = make(map[int]func(*OpenChunkManifest))
	}
	if fn == nil {
		delete(f.onOpenChunkRefAdded, 0)
		return
	}
	f.onOpenChunkRefAdded[0] = fn
}

// AddOnOpenChunkRefAdded registers an additional ref-added callback. The
// returned closure removes this subscriber without disturbing
// SetOnOpenChunkRefAdded's slot-0 callback.
func (f *FSM) AddOnOpenChunkRefAdded(fn func(*OpenChunkManifest)) (remove func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onOpenChunkRefAdded == nil {
		f.onOpenChunkRefAdded = make(map[int]func(*OpenChunkManifest))
	}
	id := f.onOpenChunkRefAddedSeq
	if id == 0 {
		id = 1
	}
	f.onOpenChunkRefAddedSeq++
	if f.onOpenChunkRefAddedSeq == 0 {
		f.onOpenChunkRefAddedSeq = 1
	}
	f.onOpenChunkRefAdded[id] = fn
	return func() {
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.onOpenChunkRefAdded, id)
	}
}

// SetOnRetentionPending registers a callback invoked (outside the FSM
// lock) after CmdRetentionPending applies. The callback receives the
// chunk ID. Used by the reconciler to learn that the cluster has
// promoted a chunk into the retention-pending state without polling
// the manifest.
func (f *FSM) SetOnRetentionPending(fn func(chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onRetentionPending = fn
}

// SetOnRequestDelete registers a callback invoked (outside the FSM lock)
// after CmdRequestDelete applies. The callback receives a copy of the
// new pending entry — chunk ID, reason, expectedFrom set. Every node in
// the placement uses this to learn that a delete was requested and
// (where appropriate) propose CmdAckDelete. Part of the receipt-based
// deletion protocol.
func (f *FSM) SetOnRequestDelete(fn func(PendingDelete)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onRequestDelete = fn
}

// SetOnAckDelete registers a callback invoked (outside the FSM lock)
// after CmdAckDelete applies. Receives the chunk ID and the node ID
// that just acked. The leader watches this to decide when to propose
// CmdFinalizeDelete (when the entry's expectedFrom set is empty).
func (f *FSM) SetOnAckDelete(fn func(chunkID chunk.ChunkID, ackingNodeID string)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onAckDelete = fn
}

// SetOnFinalizeDelete registers a callback invoked (outside the FSM
// lock) after CmdFinalizeDelete applies. Receives the chunk ID. Final
// signal that the receipt-based delete completed and the entry has
// been removed from pendingDeletes. Reconcilers can use this for audit
// logging and any post-delete bookkeeping.
func (f *FSM) SetOnFinalizeDelete(fn func(chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onFinalizeDelete = fn
}

// SetOnPruneNode registers a callback invoked (outside the FSM lock)
// after CmdPruneNode applies. Receives the prunedNodeID and the slice
// of chunkIDs whose pendingDeletes ExpectedFrom became empty as a
// result. Leader-side reconcilers should propose CmdFinalizeDelete for
// each finalizable chunk so the receipt protocol can complete deletes
// that the decommissioned node would otherwise have blocked.
func (f *FSM) SetOnPruneNode(fn func(prunedNodeID string, finalizable []chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onPruneNode = fn
}

// ---------- Reads (local, no Raft) ----------

// Get returns a copy of a chunk's metadata, or nil if not found.
func (f *FSM) Get(id chunk.ChunkID) *ManifestEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e := f.chunks[id]
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

// ListIncludingPipelineManifest returns manifest entries plus synthetic
// Active/Sealing entries from open or pending sealed manifests when the chunk
// map is missing them (inspector / cross-node reads).
func (f *FSM) ListIncludingPipelineManifest() []ManifestEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]ManifestEntry, 0, len(f.chunks)+2)
	indexByID := make(map[chunk.ChunkID]int, len(f.chunks))
	for _, e := range f.chunks {
		cp := *e
		out = append(out, cp)
		indexByID[e.ID] = len(out) - 1
	}
	overlayManifest := func(m *OpenChunkManifest, state chunk.ChunkState) {
		if m == nil {
			return
		}
		entry := manifestEntryFromOpenChunk(m, state)
		if i, ok := indexByID[m.ChunkID]; ok {
			out[i] = entry
			return
		}
		out = append(out, entry)
		indexByID[m.ChunkID] = len(out) - 1
	}
	if oc := f.openChunk; oc != nil {
		overlayManifest(oc, chunk.ChunkStateActive)
	}
	for _, sm := range f.sealedManifests {
		overlayManifest(sm, chunk.ChunkStateSealing)
	}
	return out
}

// List returns all chunk metadata, sorted by WriteStart ascending.
func (f *FSM) List() []ManifestEntry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]ManifestEntry, 0, len(f.chunks))
	for _, e := range f.chunks {
		out = append(out, *e)
	}
	return out
}

// Count returns the number of chunks.
func (f *FSM) Count() int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.chunks)
}

// ---------- Raft FSM interface ----------

// Apply handles a Raft log entry. The log data is a marshaled
// gastrologv1.VaultCtlCommand.
//
// Post-apply subscriber callbacks (onCreate, onUpload, onSeal, the receipt-
// protocol delete hooks, …) are invoked OUTSIDE the FSM mutex so that
// potentially-slow filesystem operations don't block other Apply calls.
func (f *FSM) Apply(log *hraft.Log) any {
	if len(log.Data) == 0 {
		return errors.New("empty chunk FSM command")
	}
	var cmd gastrologv1.VaultCtlCommand
	if err := proto.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("vaultctlfsm: decode command: %w", err)
	}
	return f.ApplyCommand(&cmd)
}

// ApplyCommand applies an already-decoded VaultCtlCommand. The outer
// vaultraft FSM calls this directly with the nested command from a
// VaultScopedCommand, avoiding a redundant re-marshal/unmarshal round-trip.
func (f *FSM) ApplyCommand(cmd *gastrologv1.VaultCtlCommand) any {
	result, fx := f.applyLocked(cmd)
	fx.fire()
	return result
}

// applyEffects collects the post-apply callbacks to fire outside the
// FSM mutex. Each non-nil ID/entry signals that the corresponding
// command applied successfully and the callback should run with the
// captured value. Callbacks are captured under the same lock that
// produced the IDs/entries, so a concurrent SetOn... after Apply
// returns can never observe a stale binding.
type applyEffects struct {
	createdEntry            *ManifestEntry
	uploadedEntry           *ManifestEntry
	sealedEntry             *ManifestEntry
	retentionPendingID      *chunk.ChunkID
	requestedDelete         *PendingDelete
	ackedDeleteID           *chunk.ChunkID
	ackedDeleteNodeID       string
	finalizedDeleteID       *chunk.ChunkID
	prunedNode              string
	prunedFinalizable       []chunk.ChunkID
	sealedManifest          *OpenChunkManifest
	sealedManifestClearedID *chunk.ChunkID
	publishedSegment        *CompletedSegmentEntry
	publishedSegments       []CompletedSegmentEntry
	openChunkOpened         *OpenChunkManifest
	openChunkRefAdded       *OpenChunkManifest
	releasedSegmentIDs      []glid.GLID
	ackedSegmentHolderIDs   []glid.GLID
	discardedManifestIDs    []chunk.ChunkID
	holdersChangedEntries   []ManifestEntry

	onCreate                  func(ManifestEntry)
	onUpload                  func(ManifestEntry)
	onSeal                    []func(ManifestEntry)
	onSealedManifest          []func(*OpenChunkManifest)
	onSealedManifestCleared   func(chunk.ChunkID)
	onPublishCompletedSegment []func(CompletedSegmentEntry)
	onOpenChunkManifest       []func(*OpenChunkManifest)
	onOpenChunkRefAdded       []func(*OpenChunkManifest)
	onReleaseSegments         map[int]func([]glid.GLID)
	onAckSegmentHolder        map[int]func(glid.GLID)
	onRetentionPending        func(chunk.ChunkID)
	onRequestDelete           func(PendingDelete)
	onAckDelete               func(chunk.ChunkID, string)
	onFinalizeDelete          func(chunk.ChunkID)
	onPruneNode               func(string, []chunk.ChunkID)
	onHoldersChanged          func(ManifestEntry)
}

func (e applyEffects) fire() {
	if e.createdEntry != nil && e.onCreate != nil {
		e.onCreate(*e.createdEntry)
	}
	if e.uploadedEntry != nil && e.onUpload != nil {
		e.onUpload(*e.uploadedEntry)
	}
	if e.sealedEntry != nil {
		for _, fn := range e.onSeal {
			fn(*e.sealedEntry)
		}
	}
	e.firePipelineCallbacks()
	if e.retentionPendingID != nil && e.onRetentionPending != nil {
		e.onRetentionPending(*e.retentionPendingID)
	}
	if e.requestedDelete != nil && e.onRequestDelete != nil {
		e.onRequestDelete(*e.requestedDelete)
	}
	if e.ackedDeleteID != nil && e.onAckDelete != nil {
		e.onAckDelete(*e.ackedDeleteID, e.ackedDeleteNodeID)
	}
	if e.finalizedDeleteID != nil && e.onFinalizeDelete != nil {
		e.onFinalizeDelete(*e.finalizedDeleteID)
	}
	if e.prunedNode != "" && e.onPruneNode != nil {
		e.onPruneNode(e.prunedNode, e.prunedFinalizable)
	}
	e.firePruneFinalizeCallbacks()
}

func (e applyEffects) firePipelineCallbacks() {
	if e.sealedManifest != nil {
		for _, fn := range e.onSealedManifest {
			fn(e.sealedManifest)
		}
	}
	if e.sealedManifestClearedID != nil && e.onSealedManifestCleared != nil {
		e.onSealedManifestCleared(*e.sealedManifestClearedID)
	}
	if e.publishedSegment != nil {
		for _, fn := range e.onPublishCompletedSegment {
			fn(*e.publishedSegment)
		}
	}
	for _, seg := range e.publishedSegments {
		for _, fn := range e.onPublishCompletedSegment {
			fn(seg)
		}
	}
	if e.openChunkOpened != nil {
		for _, fn := range e.onOpenChunkManifest {
			fn(e.openChunkOpened)
		}
	}
	if e.openChunkRefAdded != nil {
		for _, fn := range e.onOpenChunkRefAdded {
			fn(e.openChunkRefAdded)
		}
	}
	if len(e.releasedSegmentIDs) > 0 {
		for _, fn := range e.onReleaseSegments {
			ids := append([]glid.GLID(nil), e.releasedSegmentIDs...)
			fn(ids)
		}
	}
	// Discarded (unbuildable) manifests fire the same cleared signal a normal
	// build completion does: every home's chunking manager drops its pending
	// build/progress state for the chunk and the planner wakes to re-plan the
	// rewound records into a fresh manifest.
	for _, id := range e.discardedManifestIDs {
		if e.onSealedManifestCleared != nil {
			e.onSealedManifestCleared(id)
		}
	}
	for _, id := range e.ackedSegmentHolderIDs {
		for _, fn := range e.onAckSegmentHolder {
			fn(id)
		}
	}
	if e.onHoldersChanged != nil {
		for _, entry := range e.holdersChangedEntries {
			e.onHoldersChanged(entry)
		}
	}
}

func (e applyEffects) firePruneFinalizeCallbacks() {
	// applyPruneNode finalizes drained-ExpectedFrom chunks atomically
	// inside the same apply. Fire onFinalizeDelete per chunk so audit /
	// cache-eviction subscribers see the same stream of finalize signals
	// they would have if a CmdFinalizeDelete had applied per chunk. Skip
	// if onPruneNode was the subscriber's only hook (they get the same
	// information through the slice).
	if e.onFinalizeDelete == nil {
		return
	}
	for _, id := range e.prunedFinalizable {
		e.onFinalizeDelete(id)
	}
}

// applyLocked dispatches to the per-command apply function under the
// FSM mutex and gathers the post-apply effects.
func (f *FSM) applyLocked(cmd *gastrologv1.VaultCtlCommand) (any, applyEffects) {
	var (
		result any
		fx     applyEffects
	)

	f.mu.Lock()
	f.ready = true
	switch c := cmd.GetCommand().(type) {
	case *gastrologv1.VaultCtlCommand_CreateChunk:
		result = f.applyCreate(c.CreateChunk)
		fx.createdEntry = f.captureEntry(result, chunkIDFromProto(c.CreateChunk.GetId()))
	case *gastrologv1.VaultCtlCommand_SealChunk:
		sealID := chunkIDFromProto(c.SealChunk.GetId())
		head := f.sealedManifestHeadLocked()
		hadSealedManifest := head != nil && head.ChunkID == sealID
		result = f.applySeal(c.SealChunk)
		fx.sealedEntry = f.captureEntry(result, sealID)
		if result == nil && hadSealedManifest {
			idCopy := sealID
			fx.sealedManifestClearedID = &idCopy
		}
	case *gastrologv1.VaultCtlCommand_UploadChunk:
		result = f.applyUpload(c.UploadChunk)
		fx.uploadedEntry = f.captureEntry(result, chunkIDFromProto(c.UploadChunk.GetId()))
	case *gastrologv1.VaultCtlCommand_RetentionPending:
		result = f.applyRetentionPending(c.RetentionPending)
		fx.retentionPendingID = captureID(result, chunkIDFromProto(c.RetentionPending.GetId()))
	case *gastrologv1.VaultCtlCommand_RequestDelete:
		var entry *PendingDelete
		entry, result = f.applyRequestDelete(c.RequestDelete)
		fx.requestedDelete = entry
	case *gastrologv1.VaultCtlCommand_AckDelete:
		var (
			id        *chunk.ChunkID
			nodeID    string
			finalized bool
		)
		id, nodeID, finalized, result = f.applyAckDelete(c.AckDelete)
		fx.ackedDeleteID = id
		fx.ackedDeleteNodeID = nodeID
		// A draining ack atomically finalizes inside the same apply.
		// Surface the finalize to subscribers via the existing
		// onFinalizeDelete callback so audit / cache-eviction paths
		// fire identically to an explicit CmdFinalizeDelete.
		if finalized {
			fx.finalizedDeleteID = id
		}
	case *gastrologv1.VaultCtlCommand_FinalizeDelete:
		fx.finalizedDeleteID, result = f.applyFinalizeDelete(c.FinalizeDelete)
	case *gastrologv1.VaultCtlCommand_PruneNode:
		var (
			node        string
			finalizable []chunk.ChunkID
		)
		node, finalizable, result = f.applyPruneNode(c.PruneNode)
		fx.prunedNode = node
		fx.prunedFinalizable = finalizable
	case *gastrologv1.VaultCtlCommand_AttachOffsets:
		result = f.applyAttachOffsets(c.AttachOffsets)
	case *gastrologv1.VaultCtlCommand_BeginSeal:
		result = f.applyBeginSeal(c.BeginSeal)
	case *gastrologv1.VaultCtlCommand_RepatriateChunk:
		result = f.applyRepatriate(c.RepatriateChunk)
		// Surface to onCreate subscribers so post-create wiring
		// (retention, indexes, etc.) reacts identically to a normal
		// CmdCreateChunk path.
		fx.createdEntry = f.captureEntry(result, chunkIDFromProto(c.RepatriateChunk.GetEntry().GetId()))
	case *gastrologv1.VaultCtlCommand_ClearTransferSource:
		result = f.applyClearTransferSource(c.ClearTransferSource)
	case *gastrologv1.VaultCtlCommand_ArchiveChunk:
		result = f.applyArchiveChunk(c.ArchiveChunk)
	default:
		var ok bool
		result, fx, ok = f.tryApplySegmentPipelineLocked(cmd)
		if !ok {
			result = fmt.Errorf("unknown chunk FSM command: %T", cmd.GetCommand())
		}
	}
	f.attachApplyCallbacks(&fx)
	f.mu.Unlock()

	return result, fx
}

// tryApplySegmentPipelineLocked dispatches completed-segment registry and
// open-chunk manifest commands. Caller holds f.mu.
func (f *FSM) applyPublishCompletedSegmentsBatch(c *gastrologv1.PublishCompletedSegmentsCommand) (any, applyEffects, bool) {
	var fx applyEffects
	var newOnes []CompletedSegmentEntry
	for _, seg := range c.GetSegments() {
		segID := glid.FromBytes(seg.GetSegmentId())
		had := f.completedSegments[segID] != nil
		if err := f.applyPublishCompletedSegment(seg); err != nil {
			return err, fx, true
		}
		if !had {
			if e := f.completedSegments[segID]; e != nil {
				cp := *e
				newOnes = append(newOnes, cp)
			}
		}
	}
	if len(newOnes) > 0 {
		fx.publishedSegments = newOnes
	}
	return nil, fx, true
}

func (f *FSM) tryApplySegmentPipelineLocked(cmd *gastrologv1.VaultCtlCommand) (any, applyEffects, bool) {
	var fx applyEffects
	switch c := cmd.GetCommand().(type) {
	case *gastrologv1.VaultCtlCommand_PublishCompletedSegment:
		segID := glid.FromBytes(c.PublishCompletedSegment.GetSegmentId())
		had := f.completedSegments[segID] != nil
		result := f.applyPublishCompletedSegment(c.PublishCompletedSegment)
		if result == nil && !had {
			if e := f.completedSegments[segID]; e != nil {
				cp := *e
				fx.publishedSegment = &cp
			}
		}
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_PublishCompletedSegments:
		return f.applyPublishCompletedSegmentsBatch(c.PublishCompletedSegments)
	case *gastrologv1.VaultCtlCommand_OpenChunkManifest:
		result, opened := f.applyOpenChunkManifestLocked(c.OpenChunkManifest)
		fx.openChunkOpened = opened
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_AddOpenChunkSegmentRef:
		result, refAdded := f.applyAddOpenChunkSegmentRefLocked(c.AddOpenChunkSegmentRef)
		fx.openChunkRefAdded = refAdded
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_AddOpenChunkSegmentRefs:
		result, refAdded := f.applyAddOpenChunkSegmentRefsLocked(c.AddOpenChunkSegmentRefs)
		fx.openChunkRefAdded = refAdded
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_SealOpenChunkManifest:
		result, sealed := f.applySealOpenChunkManifestLocked(c.SealOpenChunkManifest)
		fx.sealedManifest = sealed
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_DiscardOpenChunkManifest:
		deleted, result := f.applyDiscardOpenChunkManifest(c.DiscardOpenChunkManifest)
		if deleted != nil {
			// Discarding an empty open-chunk manifest removes its f.chunks
			// entry. There are no on-disk files (the manifest carried no
			// refs/records), so only a DELETED event is owed — route it
			// through the receipt protocol's finalize callback, which emits
			// DELETED without any file/index deletion.
			fx.finalizedDeleteID = deleted
		}
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_ReleaseSegments:
		released := f.applyReleaseSegments(c.ReleaseSegments)
		if len(released) > 0 {
			fx.releasedSegmentIDs = released
		}
		return nil, fx, true
	case *gastrologv1.VaultCtlCommand_DiscardUnbuildableManifests:
		discarded, err := f.applyDiscardUnbuildableManifests(c.DiscardUnbuildableManifests)
		if err != nil {
			return err, fx, true
		}
		fx.discardedManifestIDs = discarded
		return nil, fx, true
	case *gastrologv1.VaultCtlCommand_AckSegmentHolder:
		added, result := f.applyAckSegmentHolder(c.AckSegmentHolder)
		fx.ackedSegmentHolderIDs = added
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_AckChunkHolder:
		changed, result := f.applyAckChunkHolder(c.AckChunkHolder)
		fx.holdersChangedEntries = f.captureEntries(result, changed)
		return result, fx, true
	case *gastrologv1.VaultCtlCommand_RevokeChunkHolder:
		changed, result := f.applyRevokeChunkHolder(c.RevokeChunkHolder)
		fx.holdersChangedEntries = f.captureEntries(result, changed)
		return result, fx, true
	default:
		return nil, applyEffects{}, false
	}
}

// attachApplyCallbacks copies subscriber hooks into fx for post-apply firing.
// Caller holds f.mu.
func (f *FSM) attachApplyCallbacks(fx *applyEffects) {
	fx.onCreate = f.onCreate
	fx.onUpload = f.onUpload
	if len(f.onSeal) > 0 {
		fx.onSeal = make([]func(ManifestEntry), 0, len(f.onSeal))
		for _, fn := range f.onSeal {
			fx.onSeal = append(fx.onSeal, fn)
		}
	}
	if len(f.onSealedManifest) > 0 {
		fx.onSealedManifest = make([]func(*OpenChunkManifest), 0, len(f.onSealedManifest))
		for _, fn := range f.onSealedManifest {
			fx.onSealedManifest = append(fx.onSealedManifest, fn)
		}
	}
	fx.onSealedManifestCleared = f.onSealedManifestCleared
	if len(f.onPublishCompletedSegment) > 0 {
		fx.onPublishCompletedSegment = make([]func(CompletedSegmentEntry), 0, len(f.onPublishCompletedSegment))
		for _, fn := range f.onPublishCompletedSegment {
			fx.onPublishCompletedSegment = append(fx.onPublishCompletedSegment, fn)
		}
	}
	if len(f.onOpenChunkManifest) > 0 {
		fx.onOpenChunkManifest = make([]func(*OpenChunkManifest), 0, len(f.onOpenChunkManifest))
		for _, fn := range f.onOpenChunkManifest {
			fx.onOpenChunkManifest = append(fx.onOpenChunkManifest, fn)
		}
	}
	if len(f.onOpenChunkRefAdded) > 0 {
		fx.onOpenChunkRefAdded = make([]func(*OpenChunkManifest), 0, len(f.onOpenChunkRefAdded))
		for _, fn := range f.onOpenChunkRefAdded {
			fx.onOpenChunkRefAdded = append(fx.onOpenChunkRefAdded, fn)
		}
	}
	if len(f.onReleaseSegments) > 0 {
		fx.onReleaseSegments = maps.Clone(f.onReleaseSegments)
	}
	if len(f.onAckSegmentHolder) > 0 {
		fx.onAckSegmentHolder = maps.Clone(f.onAckSegmentHolder)
	}
	fx.onRetentionPending = f.onRetentionPending
	fx.onRequestDelete = f.onRequestDelete
	fx.onAckDelete = f.onAckDelete
	fx.onHoldersChanged = f.onHoldersChanged
	fx.onFinalizeDelete = f.onFinalizeDelete
	fx.onPruneNode = f.onPruneNode
}

// chunkIDFromProto converts a 16-byte proto field to a chunk.ChunkID. A
// missing or short field yields the zero ID (handled downstream as "not
// found" by the apply* functions).
func chunkIDFromProto(b []byte) chunk.ChunkID {
	return chunk.ChunkID(glid.FromBytes(b))
}

// captureEntry returns a copy of the chunk entry for id, or nil if the
// apply errored or the entry is absent. Caller MUST hold f.mu.
func (f *FSM) captureEntry(applyResult any, id chunk.ChunkID) *ManifestEntry {
	if applyResult != nil {
		return nil
	}
	e := f.chunks[id]
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

// captureEntries copies the post-apply entries for the given chunk IDs,
// or nil if the apply errored. Entry copies are safe to hand outside the
// lock: Holders is copy-on-write (see applyAckChunkHolder). Caller holds
// f.mu.
func (f *FSM) captureEntries(applyResult any, ids []chunk.ChunkID) []ManifestEntry {
	if applyResult != nil || len(ids) == 0 {
		return nil
	}
	out := make([]ManifestEntry, 0, len(ids))
	for _, id := range ids {
		if e := f.chunks[id]; e != nil {
			out = append(out, *e)
		}
	}
	return out
}

// captureID returns id, or nil if the apply errored. Lock-free.
func captureID(applyResult any, id chunk.ChunkID) *chunk.ChunkID {
	if applyResult != nil {
		return nil
	}
	return &id
}

// SnapshotProto builds the proto representation of all FSM state. Exported
// so the outer vaultraft FSM can embed it in a VaultGroupSnapshot without a
// re-marshal round-trip.
func (f *FSM) SnapshotProto() *gastrologv1.VaultCtlSnapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.snapshotProtoLocked()
}

// snapshotProtoLocked builds the proto snapshot. Map-backed sections (entries,
// tombstones, pending deletes) are emitted in chunk-ID order so equal FSM
// state always yields a byte-identical snapshot. InstallSnapshot does not
// require canonical bytes, but determinism keeps snapshot diffing, debugging,
// and round-trip equality checks sane.
func (f *FSM) snapshotProtoLocked() *gastrologv1.VaultCtlSnapshot {
	snap := &gastrologv1.VaultCtlSnapshot{
		Entries:           make([]*gastrologv1.ManifestEntry, 0, len(f.chunks)),
		Tombstones:        make([]*gastrologv1.Tombstone, 0, len(f.tombstones)),
		PendingDeletes:    make([]*gastrologv1.PendingDelete, 0, len(f.pendingDeletes)),
		CompletedSegments: f.snapshotCompletedSegmentsLocked(),
		OpenChunk:         f.snapshotOpenChunkLocked(),
		SealedManifests:   f.snapshotSealedManifestsLocked(),
		SegmentResume:     f.snapshotSegmentResumeLocked(),
		SegmentChunks:     f.snapshotSegmentChunksLocked(),
	}

	releasedIDs := slices.SortedFunc(maps.Keys(f.releasedSegments), glid.Compare)
	for _, id := range releasedIDs {
		idCopy := id
		snap.ReleasedSegmentIds = append(snap.ReleasedSegmentIds, idCopy[:])
	}

	entryIDs := slices.SortedFunc(maps.Keys(f.chunks), chunk.ChunkID.Compare)
	for _, id := range entryIDs {
		snap.Entries = append(snap.Entries, entryToProto(f.chunks[id]))
	}

	tombIDs := slices.SortedFunc(maps.Keys(f.tombstones), chunk.ChunkID.Compare)
	for _, id := range tombIDs {
		idCopy := id
		snap.Tombstones = append(snap.Tombstones, &gastrologv1.Tombstone{
			ChunkId:        idCopy[:],
			DeletedAtNanos: f.tombstones[id].UnixNano(),
		})
	}

	pdIDs := slices.SortedFunc(maps.Keys(f.pendingDeletes), chunk.ChunkID.Compare)
	for _, id := range pdIDs {
		snap.PendingDeletes = append(snap.PendingDeletes, pendingDeleteToProto(f.pendingDeletes[id]))
	}

	return snap
}

// Snapshot returns a point-in-time snapshot of all chunk metadata.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	return &fsmSnapshot{snap: f.SnapshotProto()}, nil
}

// RestoreProto replaces FSM state from a decoded VaultCtlSnapshot.
func (f *FSM) RestoreProto(snap *gastrologv1.VaultCtlSnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.restoreFromProtoLocked(snap)
}

// Restore replaces FSM state from a snapshot (marshaled VaultCtlSnapshot).
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	raw, err := io.ReadAll(rc) //ok:io-readall proto.Unmarshal needs the full buffer; vault-ctl snapshots are bounded metadata (manifest+registry), not record data
	if err != nil {
		return fmt.Errorf("restore chunk FSM: read: %w", err)
	}
	var snap gastrologv1.VaultCtlSnapshot
	if err := proto.Unmarshal(raw, &snap); err != nil {
		return fmt.Errorf("restore chunk FSM: decode: %w", err)
	}
	f.RestoreProto(&snap)
	return nil
}

// restoreFromProtoLocked repopulates FSM state from a decoded snapshot.
// Caller MUST hold f.mu.
func (f *FSM) restoreFromProtoLocked(snap *gastrologv1.VaultCtlSnapshot) {
	f.chunks = make(map[chunk.ChunkID]*ManifestEntry, len(snap.GetEntries()))
	for _, pe := range snap.GetEntries() {
		e := entryFromProto(pe)
		f.chunks[e.ID] = &e
	}
	f.tombstones = make(map[chunk.ChunkID]time.Time, len(snap.GetTombstones()))
	for _, ts := range snap.GetTombstones() {
		f.tombstones[chunkIDFromProto(ts.GetChunkId())] = time.Unix(0, ts.GetDeletedAtNanos())
	}
	f.pendingDeletes = make(map[chunk.ChunkID]*PendingDelete, len(snap.GetPendingDeletes()))
	for _, pp := range snap.GetPendingDeletes() {
		p := pendingDeleteFromProto(pp)
		f.pendingDeletes[p.ChunkID] = &p
	}
	f.restoreCompletedSegmentsLocked(snap.GetCompletedSegments())
	f.releasedSegments = make(map[glid.GLID]struct{}, len(snap.GetReleasedSegmentIds()))
	for _, raw := range snap.GetReleasedSegmentIds() {
		f.releasedSegments[glid.FromBytes(raw)] = struct{}{}
	}
	f.restoreOpenChunkLocked(snap)
	f.repairSealedManifestChunkEntryLocked()
	f.ready = true
}

// ---------- Command application ----------

// CreateChunk inserts a new Active chunk entry.
func (f *FSM) applyCreate(c *gastrologv1.CreateChunkCommand) error {
	id := chunkIDFromProto(c.GetId())
	writeStart := time.Unix(0, c.GetWriteStartNanos())
	ingestStart := time.Unix(0, c.GetIngestStartNanos())
	sourceStart := time.Unix(0, c.GetSourceStartNanos())

	// Reject creates for tombstoned chunk IDs. If the vault already finalized
	// a delete for this ID, a later CreateChunk (late replication /
	// out-of-order Raft apply) must not resurrect it in the live map —
	// that is the ghost-chunk failure. The orchestrator's post-import path
	// separately cleans up any on-disk files via the tombstone re-check
	// after announce.
	if _, dead := f.tombstones[id]; dead {
		return nil
	}

	f.chunks[id] = &ManifestEntry{
		ID:          id,
		WriteStart:  writeStart,
		IngestStart: ingestStart,
		SourceStart: sourceStart,
		State:       chunk.ChunkStateActive,
	}
	return nil
}

// SealChunk records the final sealed-form metadata for a chunk.
//
// IngestStart and the monotonic flag are SealChunk fields: the chunk
// manager tracks the actual min IngestTS as records are appended (vs.
// the wall-clock createdAt that CmdCreateChunk seeded), and only the
// chunk manager knows whether the appended sequence stayed in
// IngestTS-ascending order. Both must reach the FSM at seal time so
// the manifest carries authoritative TS bounds and monotonicity for
// every reader (notably the histogram bucket math, which mis-renders
// non-monotonic chunks if it gets either field wrong).
func (f *FSM) applySeal(c *gastrologv1.SealChunkCommand) error {
	id := chunkIDFromProto(c.GetId())

	e := f.chunks[id]
	if e == nil {
		if sm := f.sealedManifestByIDLocked(id); sm != nil {
			f.clearStaleSealTombstoneLocked(id)
			f.ensureManifestChunkEntryLocked(sm, chunk.ChunkStateSealing)
			e = f.chunks[id]
		}
	}
	if e == nil {
		return fmt.Errorf("seal chunk: %s not found", id)
	}
	e.WriteEnd = time.Unix(0, c.GetWriteEndNanos())
	e.RecordCount = c.GetRecordCount()
	e.Bytes = c.GetBytes()
	e.IngestEnd = time.Unix(0, c.GetIngestEndNanos())
	e.SourceEnd = time.Unix(0, c.GetSourceEndNanos())
	e.IngestStart = time.Unix(0, c.GetIngestStartNanos())
	e.IngestTSMonotonic = c.GetIngestTsMonotonic()
	e.State = chunk.ChunkStateSealed
	sealedAt := time.Unix(0, c.GetSealedAtNanos())
	if sealedAt.IsZero() {
		sealedAt = e.WriteEnd
	}
	e.SealedAt = sealedAt
	f.popSealedManifestHeadIfIDLocked(id)
	return nil
}

// BeginSeal: Active → Sealing transition. The leader proposes this when its
// rotation policy fires and before sealed-form assembly begins. The
// chunk's metadata still reflects active-form bookkeeping (no WriteEnd
// / final RecordCount yet — those come in CmdSealChunk). Idempotent:
// repeated BeginSeals on the same chunk are harmless.
func (f *FSM) applyBeginSeal(c *gastrologv1.BeginSealCommand) error {
	id := chunkIDFromProto(c.GetId())

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("begin seal: %s not found", id)
	}
	// Don't drop back from Sealed to Sealing on a stale replay.
	if e.State == chunk.ChunkStateSealed {
		return nil
	}
	e.State = chunk.ChunkStateSealing
	return nil
}

// RepatriateChunk payload: a full ManifestEntry encoded via
// encodeEntry's 123-byte fixed layout. Reuses the snapshot entry
// format so this command and snapshot replay share one schema —
// any field added to ManifestEntry only needs the entry codec
// updated once.
//
// Apply semantics:
//   - Refuse if the chunk is already in the manifest. Repatriation
//     is a recovery path for orphan files the FSM has lost; if the
//     entry exists, normal lifecycle commands should handle it.
//   - Refuse if tombstoned. The cluster has explicitly forgotten
//     this chunk; recreating it would resurrect a finalize-deleted
//     entry, the exact failure mode CmdCreateChunk's tombstone
//     guard prevents. Operators must clear the tombstone (out of
//     scope here — likely a separate `cluster purge-tombstone`
//     verb or a destructive `--force` flag).
//   - Otherwise insert the entry verbatim, with State forced to
//     Sealed regardless of what the payload says — repatriation
//     only handles sealed chunks (active-chunk state is in-memory
//     on the leader, not reconstructable from idx.log alone).
func (f *FSM) applyRepatriate(c *gastrologv1.RepatriateChunkCommand) error {
	if c.GetEntry() == nil {
		return errors.New("repatriate chunk: missing entry")
	}
	e := entryFromProto(c.GetEntry())
	if _, dead := f.tombstones[e.ID]; dead {
		return fmt.Errorf("repatriate chunk %s: refused (tombstoned)", e.ID)
	}
	if _, exists := f.chunks[e.ID]; exists {
		return fmt.Errorf("repatriate chunk %s: refused (already in manifest)", e.ID)
	}
	e.State = chunk.ChunkStateSealed
	entry := e
	f.chunks[entry.ID] = &entry
	return nil
}

// applyClearTransferSource clears a manifest entry's TransferSourceVaultID.
// Idempotent: a no-op (nil error, no state change) when the entry doesn't
// exist or the field is already the zero GLID — a replayed or retried
// clear must never error. See CmdClearTransferSource.
func (f *FSM) applyClearTransferSource(c *gastrologv1.ClearTransferSourceCommand) error {
	id := chunkIDFromProto(c.GetChunkId())
	e := f.chunks[id]
	if e == nil || e.TransferSourceVaultID.IsZero() {
		return nil
	}
	e.TransferSourceVaultID = glid.GLID{}
	return nil
}

// applyArchiveChunk records the storage class a chunk's cloud blob now sits
// in, and derives Archived from it. Idempotent: replaying the same class is a
// no-op, and an unknown chunk is ignored rather than erroring, because the
// archive itself already succeeded against the cloud store — failing the apply
// would not un-archive it, it would only make the FSM disagree with the blob.
// See CmdArchiveChunk.
func (f *FSM) applyArchiveChunk(c *gastrologv1.ArchiveChunkCommand) error {
	id := chunkIDFromProto(c.GetId())
	e := f.chunks[id]
	if e == nil {
		return nil
	}
	e.CloudStorageClass = c.GetCloudStorageClass()
	e.Archived = e.CloudStorageClass != ""
	return nil
}

// NewArchiveChunk builds an ArchiveChunk command message. See CmdArchiveChunk.
func NewArchiveChunk(id chunk.ChunkID, cloudStorageClass string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_ArchiveChunk{
		ArchiveChunk: &gastrologv1.ArchiveChunkCommand{Id: id[:], CloudStorageClass: cloudStorageClass},
	}}
}

// MarshalArchiveChunk builds the Raft log data for an ArchiveChunk command.
func MarshalArchiveChunk(id chunk.ChunkID, cloudStorageClass string) []byte {
	return mustMarshalCommand(NewArchiveChunk(id, cloudStorageClass))
}

// AttachOffsets: [16 ChunkID][8 IngestIdxOff][8 IngestIdxSize][8 SourceIdxOff][8 SourceIdxSize]
//
// Fired after sealToGLCB on the leader (and after finalizeImportedChunk
// on import paths) so every sealed chunk's section-offset metadata
// reaches the FSM, not just cloud-uploaded ones. The histogram's GLCB
// section-reader uses these offsets to mmap the IngestTS index out of
// data.glcb directly, eliminating the .ts-cache download path.
func (f *FSM) applyAttachOffsets(c *gastrologv1.AttachOffsetsCommand) error {
	id := chunkIDFromProto(c.GetId())

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("attach offsets: %s not found", id)
	}
	e.IngestIdxOffset = c.GetIngestIdxOffset()
	e.IngestIdxSize = c.GetIngestIdxSize()
	e.SourceIdxOffset = c.GetSourceIdxOffset()
	e.SourceIdxSize = c.GetSourceIdxSize()
	return nil
}

// UploadChunk records cloud-upload metadata and integrity fields.
func (f *FSM) applyUpload(c *gastrologv1.UploadChunkCommand) error {
	id := chunkIDFromProto(c.GetId())

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("upload chunk: %s not found", id)
	}
	e.CloudBytes = c.GetCloudBytes()
	e.IngestIdxOffset = c.GetIngestIdxOffset()
	e.IngestIdxSize = c.GetIngestIdxSize()
	e.SourceIdxOffset = c.GetSourceIdxOffset()
	e.SourceIdxSize = c.GetSourceIdxSize()
	e.CloudBacked = true

	// Integrity fields — present only on the extended form.
	if h := c.GetHash(); len(h) > 0 {
		copy(e.Hash[:], h)
		e.CloudServiceID = glid.FromBytes(c.GetCloudServiceId())
		e.KeyScheme = uint8(c.GetKeyScheme()) //nolint:gosec // G115: key scheme is a small enum; round-trips a uint8
	}
	return nil
}

// RetentionPending marks a chunk as pending retention deletion.
func (f *FSM) applyRetentionPending(c *gastrologv1.RetentionPendingCommand) error {
	id := chunkIDFromProto(c.GetId())
	if e := f.chunks[id]; e != nil {
		e.RetentionPending = true
	}
	return nil
}

// ---------- Command builders (used by callers before Raft.Apply) ----------
//
// Each Marshal* returns a marshaled gastrologv1.VaultCtlCommand. The
// New* builders return the typed message so the outer vaultraft envelope
// can wrap it without a re-marshal round-trip.

// mustMarshalCommand marshals a VaultCtlCommand. proto.Marshal of these
// in-memory messages cannot fail; a non-nil error indicates a programmer
// error (e.g. a nil oneof) and panics rather than returning corrupt bytes.
func mustMarshalCommand(cmd *gastrologv1.VaultCtlCommand) []byte {
	b, err := proto.Marshal(cmd)
	if err != nil {
		panic(fmt.Sprintf("vaultctlfsm: marshal command: %v", err))
	}
	return b
}

// NewCreateChunk builds a CreateChunk command message.
func NewCreateChunk(id chunk.ChunkID, writeStart, ingestStart, sourceStart time.Time) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_CreateChunk{CreateChunk: &gastrologv1.CreateChunkCommand{
		Id:               id[:],
		WriteStartNanos:  writeStart.UnixNano(),
		IngestStartNanos: ingestStart.UnixNano(),
		SourceStartNanos: sourceStart.UnixNano(),
	}}}
}

// MarshalCreateChunk builds the Raft log data for a CreateChunk command.
func MarshalCreateChunk(id chunk.ChunkID, writeStart, ingestStart, sourceStart time.Time) []byte {
	return mustMarshalCommand(NewCreateChunk(id, writeStart, ingestStart, sourceStart))
}

// NewSealChunk builds a SealChunk command message.
func NewSealChunk(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestStart, ingestEnd, sourceEnd time.Time, ingestTSMonotonic bool, sealedAt time.Time) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_SealChunk{SealChunk: &gastrologv1.SealChunkCommand{
		Id:                id[:],
		WriteEndNanos:     writeEnd.UnixNano(),
		RecordCount:       recordCount,
		Bytes:             bytes,
		IngestEndNanos:    ingestEnd.UnixNano(),
		SourceEndNanos:    sourceEnd.UnixNano(),
		IngestStartNanos:  ingestStart.UnixNano(),
		IngestTsMonotonic: ingestTSMonotonic,
		SealedAtNanos:     sealedAt.UnixNano(),
	}}}
}

// MarshalSealChunk builds the Raft log data for a SealChunk command.
func MarshalSealChunk(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestStart, ingestEnd, sourceEnd time.Time, ingestTSMonotonic bool, sealedAt time.Time) []byte {
	return mustMarshalCommand(NewSealChunk(id, writeEnd, recordCount, bytes, ingestStart, ingestEnd, sourceEnd, ingestTSMonotonic, sealedAt))
}

// NewBeginSeal builds a BeginSeal command message.
func NewBeginSeal(id chunk.ChunkID) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_BeginSeal{BeginSeal: &gastrologv1.BeginSealCommand{Id: id[:]}}}
}

// MarshalBeginSeal builds the Raft log data for a BeginSeal command.
func MarshalBeginSeal(id chunk.ChunkID) []byte {
	return mustMarshalCommand(NewBeginSeal(id))
}

// NewUploadChunk builds an UploadChunk command message. cloudBytes is the
// compressed cloud object's transport size, not any local disk size.
func NewUploadChunk(id chunk.ChunkID, cloudBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, hash [32]byte, cloudServiceID glid.GLID, keyScheme uint8) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_UploadChunk{UploadChunk: &gastrologv1.UploadChunkCommand{
		Id:              id[:],
		CloudBytes:      cloudBytes,
		IngestIdxOffset: ingestIdxOff,
		IngestIdxSize:   ingestIdxSize,
		SourceIdxOffset: sourceIdxOff,
		SourceIdxSize:   sourceIdxSize,
		Hash:            hash[:],
		CloudServiceId:  cloudServiceID[:],
		KeyScheme:       uint32(keyScheme),
	}}}
}

// MarshalUploadChunk builds the Raft log data for an UploadChunk command.
func MarshalUploadChunk(id chunk.ChunkID, cloudBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, hash [32]byte, cloudServiceID glid.GLID, keyScheme uint8) []byte {
	return mustMarshalCommand(NewUploadChunk(id, cloudBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize, hash, cloudServiceID, keyScheme))
}

// NewAttachOffsets builds a CmdAttachOffsets command message.
func NewAttachOffsets(id chunk.ChunkID, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_AttachOffsets{AttachOffsets: &gastrologv1.AttachOffsetsCommand{
		Id:              id[:],
		IngestIdxOffset: ingestIdxOff,
		IngestIdxSize:   ingestIdxSize,
		SourceIdxOffset: sourceIdxOff,
		SourceIdxSize:   sourceIdxSize,
	}}}
}

// MarshalAttachOffsets builds the Raft log data for a CmdAttachOffsets command.
func MarshalAttachOffsets(id chunk.ChunkID, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64) []byte {
	return mustMarshalCommand(NewAttachOffsets(id, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize))
}

// NewRetentionPending builds a RetentionPending command message.
func NewRetentionPending(id chunk.ChunkID) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_RetentionPending{RetentionPending: &gastrologv1.RetentionPendingCommand{Id: id[:]}}}
}

// MarshalRetentionPending builds the Raft log data for a RetentionPending command.
func MarshalRetentionPending(id chunk.ChunkID) []byte {
	return mustMarshalCommand(NewRetentionPending(id))
}

// NewRepatriateChunk builds a RepatriateChunk command message carrying the
// full ManifestEntry. State is forced to ChunkStateSealed on apply
// regardless of what entry.State says — only sealed chunks are
// repatriatable.
func NewRepatriateChunk(entry ManifestEntry) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_RepatriateChunk{RepatriateChunk: &gastrologv1.RepatriateChunkCommand{Entry: entryToProto(&entry)}}}
}

// MarshalRepatriateChunk builds the Raft log data for a RepatriateChunk
// command. Returns an error for signature compatibility with callers; the
// proto build cannot fail.
func MarshalRepatriateChunk(entry ManifestEntry) ([]byte, error) {
	return mustMarshalCommand(NewRepatriateChunk(entry)), nil
}

// NewClearTransferSource builds a ClearTransferSource command message. See
// CmdClearTransferSource.
func NewClearTransferSource(id chunk.ChunkID) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_ClearTransferSource{ClearTransferSource: &gastrologv1.ClearTransferSourceCommand{ChunkId: id[:]}}}
}

// MarshalClearTransferSource builds the Raft log data for a
// ClearTransferSource command.
func MarshalClearTransferSource(id chunk.ChunkID) []byte {
	return mustMarshalCommand(NewClearTransferSource(id))
}

// entryToProto converts a ManifestEntry to its proto representation,
// carrying every field including Hash / CloudServiceID / KeyScheme.
func entryToProto(e *ManifestEntry) *gastrologv1.ManifestEntry {
	return &gastrologv1.ManifestEntry{
		Id:                    e.ID[:],
		WriteStartNanos:       e.WriteStart.UnixNano(),
		WriteEndNanos:         e.WriteEnd.UnixNano(),
		RecordCount:           e.RecordCount,
		Bytes:                 e.Bytes,
		State:                 gastrologv1.ChunkState(e.State),
		CloudBytes:            e.CloudBytes,
		IngestStartNanos:      e.IngestStart.UnixNano(),
		IngestEndNanos:        e.IngestEnd.UnixNano(),
		SourceStartNanos:      e.SourceStart.UnixNano(),
		SourceEndNanos:        e.SourceEnd.UnixNano(),
		IngestTsMonotonic:     e.IngestTSMonotonic,
		CloudBacked:           e.CloudBacked,
		Archived:              e.Archived,
		CloudStorageClass:     e.CloudStorageClass,
		RetentionPending:      e.RetentionPending,
		IngestIdxOffset:       e.IngestIdxOffset,
		IngestIdxSize:         e.IngestIdxSize,
		SourceIdxOffset:       e.SourceIdxOffset,
		SourceIdxSize:         e.SourceIdxSize,
		Hash:                  e.Hash[:],
		CloudServiceId:        e.CloudServiceID[:],
		KeyScheme:             uint32(e.KeyScheme),
		SealedAtNanos:         e.SealedAt.UnixNano(),
		Holders:               slices.Clone(e.Holders),
		TransferSourceVaultId: glid.OptionalToProto(nonZeroGLID(e.TransferSourceVaultID)),
	}
}

// nonZeroGLID returns nil for the zero GLID and &g otherwise, so
// glid.OptionalToProto round-trips "no transfer source" as an empty proto
// bytes field rather than encoding 16 zero bytes.
func nonZeroGLID(g glid.GLID) *glid.GLID {
	if g.IsZero() {
		return nil
	}
	return &g
}

// entryFromProto converts a proto ManifestEntry back to the Go struct.
func entryFromProto(p *gastrologv1.ManifestEntry) ManifestEntry {
	e := ManifestEntry{
		ID:                chunkIDFromProto(p.GetId()),
		WriteStart:        time.Unix(0, p.GetWriteStartNanos()),
		WriteEnd:          time.Unix(0, p.GetWriteEndNanos()),
		RecordCount:       p.GetRecordCount(),
		Bytes:             p.GetBytes(),
		State:             chunk.ChunkState(p.GetState()), //nolint:gosec // G115: ChunkState enum values are 0-3, round-trips a uint8
		CloudBytes:        p.GetCloudBytes(),
		IngestStart:       time.Unix(0, p.GetIngestStartNanos()),
		IngestEnd:         time.Unix(0, p.GetIngestEndNanos()),
		SourceStart:       time.Unix(0, p.GetSourceStartNanos()),
		SourceEnd:         time.Unix(0, p.GetSourceEndNanos()),
		IngestTSMonotonic: p.GetIngestTsMonotonic(),
		SealedAt:          time.Unix(0, p.GetSealedAtNanos()),
		CloudBacked:       p.GetCloudBacked(),
		Archived:          p.GetArchived(),
		CloudStorageClass: p.GetCloudStorageClass(),
		RetentionPending:  p.GetRetentionPending(),
		IngestIdxOffset:   p.GetIngestIdxOffset(),
		IngestIdxSize:     p.GetIngestIdxSize(),
		SourceIdxOffset:   p.GetSourceIdxOffset(),
		SourceIdxSize:     p.GetSourceIdxSize(),
		CloudServiceID:    glid.FromBytes(p.GetCloudServiceId()),
		KeyScheme:         uint8(p.GetKeyScheme()), //nolint:gosec // G115: key scheme is a small enum; round-trips a uint8
		Holders:           slices.Clone(p.GetHolders()),
	}
	copy(e.Hash[:], p.GetHash())
	if src := glid.OptionalFromProto(p.GetTransferSourceVaultId()); src != nil {
		e.TransferSourceVaultID = *src
	}
	return e
}

// ---------- Snapshot ----------
//
// The snapshot is a marshaled gastrologv1.VaultCtlSnapshot.

type fsmSnapshot struct {
	snap *gastrologv1.VaultCtlSnapshot
}

func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	b, err := proto.Marshal(s.snap)
	if err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("persist chunk FSM snapshot: %w", err)
	}
	if _, err := sink.Write(b); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
