package vaultctlfsm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"

	hraft "github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// Command identifies the type of chunk metadata mutation. Since the
// gastrolog-5lrg7 protobuf migration, commands are encoded as
// gastrologv1.VaultCtlCommand (a oneof) rather than an opcode byte; these
// constants are retained as the canonical opcode↔proto-field mapping and are
// still used by the WAL inspector tooling.
type Command byte

const (
	CmdCreateChunk   Command = 1
	CmdSealChunk     Command = 2
	CmdCompressChunk Command = 3
	CmdUploadChunk   Command = 4

	// CmdDeleteChunk is retained for WAL replay backward-compat only. The
	// receipt protocol (CmdRequestDelete + acks + CmdFinalizeDelete) is
	// the canonical delete path; no producer in the current code path
	// emits new CmdDeleteChunk entries, and a forbidigo lint rule
	// (.golangci.yml) blocks reintroduction by banning new
	// MarshalDeleteChunk callers. The FSM continues to apply
	// CmdDeleteChunk so pre-migration WAL segments still replay
	// correctly. See gastrolog-51gme step 11.
	CmdDeleteChunk Command = 5

	CmdRetentionPending Command = 6

	// Receipt-based deletion protocol — gastrolog-51gme step 2. Replaces
	// single-shot CmdDeleteChunk fan-out with an N-way receipt protocol
	// that survives snapshot install and gives every node a first-class
	// "delete locally and ack" obligation. See docs in fsm_receipts.go.
	//
	// Numbering note: the prior CmdTransitionStreamed (7) and
	// CmdTransitionReceived (8) commands were removed entirely in
	// gastrolog-5sywa (Phase 4 follow-up). Per the project's "delete and
	// renumber, never reserved" rule, the receipt-protocol commands
	// renumbered down to 7/8/9.
	CmdRequestDelete  Command = 7 // vault leader proposes a delete; replicates the expected-acks set
	CmdAckDelete      Command = 8 // each expected node acks after handling its local side
	CmdFinalizeDelete Command = 9 // leader removes the entry once expectedFrom is empty

	// Membership-change cleanup — gastrolog-51gme step 10. After a node
	// is removed from this vault-ctl Raft group's voter set (decommissioned
	// or rebalanced away), the leader proposes CmdPruneNode to drop that
	// node's slot from every pendingDeletes entry's ExpectedFrom. Without
	// this, deletes proposed before the node left would never finalize:
	// the leader would hold the entry forever waiting for an ack from a
	// node that no longer participates.
	CmdPruneNode Command = 10

	// CmdAttachOffsets carries the GLCB blob's section-offset/size pairs
	// (IngestTS index, SourceTS index) once sealToGLCB has produced
	// data.glcb on the leader. Replaces the original "offsets
	// only land in the FSM via CmdUploadChunk" gap that left
	// sealed-but-not-yet-uploaded chunks with zero offsets in the
	// manifest, breaking the histogram's GLCB section-reader path. Fires
	// from chunk/file.Manager after sealToGLCB and from the import path
	// (orchestrator/vault_ops.finalizeImportedChunk) for replicated
	// chunks. See gastrolog-1dg3i.
	CmdAttachOffsets Command = 11

	// CmdBeginSeal carries the Active → Sealing transition: the leader
	// has stopped accepting appends on the chunk and is starting
	// sealed-form assembly. Distinct from CmdSealChunk so the cluster
	// observes the in-flight assembly window explicitly — uploads,
	// retention triggers, and replication-completeness checks gate on
	// State == Sealed; the Sealing entry is not yet a finished chunk
	// even though it's no longer accepting writes. See gastrolog-1huz5.
	CmdBeginSeal Command = 12

	// CmdRepatriateChunk re-introduces a sealed chunk's manifest entry
	// when the FSM has lost it but a local replica still exists on
	// disk (operator-driven recovery from FSM glitches, restore-from-
	// backup desync, etc.). Payload is the full ManifestEntry
	// reconstructed from the local chunk's idx.log headers. Apply
	// inserts the entry in Sealed state, refusing if the entry
	// already exists or is tombstoned. See gastrolog-32bf2.
	CmdRepatriateChunk Command = 13
)

// ManifestEntry holds the full metadata for one chunk in this vault's
// manifest (the FSM's set of chunks for one vault — see Manifest in
// docs/ubiquitous_language.md). Every chunk in the cluster is described by
// exactly one ManifestEntry, mutated only by the Cmd* applies. This is the
// Raft-replicated equivalent of file.Manager.chunkMeta + cloudIdx entries
// — and the source of truth they project from after the chunk redesign
// (gastrolog-2pw28).
type ManifestEntry struct {
	ID          chunk.ChunkID
	WriteStart  time.Time
	WriteEnd    time.Time
	RecordCount int64
	Bytes       int64
	// State is the chunk's lifecycle state (Active|Sealing|Sealed).
	// Phase 3 (gastrolog-1huz5) replaced the legacy Sealed bool with
	// this three-state field; consumers that just want "is the cluster
	// done sealing this chunk?" check `State == chunk.ChunkStateSealed`.
	// ChunkMeta still carries a separate `Sealed` bool because its
	// semantics differ — ChunkMeta.Sealed is the LOCAL active-form-closed
	// signal, distinct from this cluster-wide lifecycle state.
	State     chunk.ChunkState
	DiskBytes int64

	IngestStart time.Time
	IngestEnd   time.Time
	SourceStart time.Time
	SourceEnd   time.Time

	// IngestTSMonotonic is true when records were appended in IngestTS-
	// ascending order; the histogram fast path uses position-as-rank only
	// when this is true. Set by CmdSealChunk from the chunk manager's
	// running flag (which only flips one direction, true → false).
	IngestTSMonotonic bool

	CloudBacked      bool
	Archived         bool
	RetentionPending bool

	// Cloud-specific TOC offsets (GLCB format).
	IngestIdxOffset int64
	IngestIdxSize   int64
	SourceIdxOffset int64
	SourceIdxSize   int64

	// Integrity fields populated at upload time, verified on every cache
	// re-fetch. See gastrolog-grnc3.
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
// Sealed state (GLCB committed). Replaces the legacy Sealed bool
// after Phase 3 (gastrolog-1huz5). For ChunkMeta produced from this
// entry, ChunkMeta.Sealed carries the same answer for downstream
// consumers — but those consumers must remember that ChunkMeta from
// the local chunk Manager (un-overlaid) has DIFFERENT semantics:
// it's the local active-form-closed signal, which flips earlier.
func (e *ManifestEntry) IsSealed() bool {
	return e.State == chunk.ChunkStateSealed
}

// ToChunkMeta converts to the public chunk.ChunkMeta type.
func (e *ManifestEntry) ToChunkMeta() chunk.ChunkMeta {
	state := e.State
	return chunk.ChunkMeta{
		ID:                e.ID,
		WriteStart:        e.WriteStart,
		WriteEnd:          e.WriteEnd,
		RecordCount:       e.RecordCount,
		Bytes:             e.Bytes,
		Sealed:            state == chunk.ChunkStateSealed,
		State:             state,
		DiskBytes:         e.DiskBytes,
		IngestStart:       e.IngestStart,
		IngestEnd:         e.IngestEnd,
		SourceStart:       e.SourceStart,
		SourceEnd:         e.SourceEnd,
		IngestTSMonotonic: e.IngestTSMonotonic,
		CloudBacked:       e.CloudBacked,
		Archived:          e.Archived,
	}
}

// FSM is a Raft FSM that maintains chunk metadata for a single vault.
// All reads are local (no Raft round-trip). Writes go through Raft.Apply().
type FSM struct {
	mu       sync.RWMutex
	chunks   map[chunk.ChunkID]*ManifestEntry
	ready    bool // true after first Apply or Restore
	onDelete func(chunk.ChunkID)
	onUpload func(ManifestEntry) // called after CmdUploadChunk applies (outside lock)

	// Step-1 reconciler-wiring hooks for gastrolog-51gme. Each fires
	// outside the FSM mutex after the corresponding Cmd applies, so the
	// reconciler can project FSM state changes into local Manager state
	// without polling. No callers wired yet — adding the surface here
	// unblocks subsequent steps without requiring an FSM API churn.
	onCreate           func(ManifestEntry) // CmdCreateChunk applied; passes the freshly-created entry
	onSeal             func(ManifestEntry) // CmdSealChunk applied; passes the now-sealed entry
	onRetentionPending func(chunk.ChunkID) // CmdRetentionPending applied

	// Step-2 receipt-protocol state and hooks for gastrolog-51gme.
	// pendingDeletes is the queue of chunk deletes awaiting per-node
	// acknowledgement. See PendingDelete and the apply* functions in
	// fsm_receipts.go.
	pendingDeletes map[chunk.ChunkID]*PendingDelete

	onRequestDelete  func(PendingDelete)           // CmdRequestDelete applied; passes a copy of the new entry
	onAckDelete      func(chunk.ChunkID, string)   // CmdAckDelete applied; (chunkID, ackingNodeID)
	onFinalizeDelete func(chunk.ChunkID)           // CmdFinalizeDelete applied; expectedFrom was empty
	onPruneNode      func(string, []chunk.ChunkID) // CmdPruneNode applied; (prunedNodeID, finalizableChunks)
	onPublishFence   func(FenceRecord)             // CmdPublishFence applied

	// tombstones records chunk IDs that have been deleted, with the apply
	// timestamp of the delete. Consulted by the receive side of vault
	// replication to reject stale ImportSealed / Append commands that
	// arrive after a chunk has been deleted — closes the race between
	// retention and post-seal replication where a late replication RPC
	// could otherwise recreate a "ghost" chunk on a follower.
	// See gastrolog-11rzz.
	//
	// Periodically pruned by the orchestrator (entries older than the
	// replication-job deadline, typically a few minutes, cannot still be
	// in flight and are safe to drop).
	tombstones map[chunk.ChunkID]time.Time

	// Destination-vault sequence allocator control state (gastrolog-16w8x).
	// Owned by vault-ctl Raft; per-record acceptance metadata lives off Raft.
	seqNextSeq      uint64
	seqEpoch        uint64
	seqActiveSwaths map[string]*SeqActiveLease // holder ID -> outstanding swath
	seqBurnedTails  []SeqBurnedTail

	// Published fence history (F_1..F_n) for sequenced write-model vaults.
	fences []FenceRecord
}

// New creates an empty chunk metadata FSM.
func New() *FSM {
	return &FSM{
		chunks:          make(map[chunk.ChunkID]*ManifestEntry),
		tombstones:      make(map[chunk.ChunkID]time.Time),
		pendingDeletes:  make(map[chunk.ChunkID]*PendingDelete),
		seqActiveSwaths: make(map[string]*SeqActiveLease),
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
// See gastrolog-3pf9w.
func (f *FSM) SetOnCreate(fn func(ManifestEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onCreate = fn
}

// SetOnDelete registers a callback that fires after CmdDeleteChunk is
// applied to this FSM and the chunk is removed from the in-memory map.
// The callback runs OUTSIDE the FSM mutex so it can perform slow operations
// (filesystem deletes, index removal) without blocking other Apply calls.
//
// The callback is fired exactly once per actual deletion — if the same
// CmdDeleteChunk applies twice (e.g. log replay), the second call is a
// no-op because the entry is already gone, and the callback is not fired.
//
// Used by the orchestrator to delete local chunk files when a delete
// originating from any node propagates via Raft. The callback is expected
// to use a path that does NOT re-announce the delete (e.g. SilentDeleter)
// to avoid an infinite feedback loop.
func (f *FSM) SetOnDelete(fn func(chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onDelete = fn
}

// SetOnUpload registers a callback invoked (outside the FSM lock) after
// CmdUploadChunk applies. The callback receives a copy of the uploaded
// entry. Follower nodes use this to register cloud chunks in their local
// chunk manager without streaming any records.
func (f *FSM) SetOnUpload(fn func(ManifestEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onUpload = fn
}

// SetOnSeal registers a callback invoked (outside the FSM lock) after
// CmdSealChunk applies. The callback receives a copy of the now-sealed
// entry. The reconciler (gastrolog-51gme) uses this to project the
// FSM-side seal into the local Manager's chunk meta — closes the
// gastrolog-uccg6 active-vs-sealed divergence path that previously
// relied on a periodic disk-vs-FSM walk.
//
// Fires once per actual seal apply. A re-apply (log replay over an
// already-sealed entry) still fires the callback because the FSM
// idempotently re-writes the seal fields; the reconciler is expected
// to be idempotent in turn.
func (f *FSM) SetOnSeal(fn func(ManifestEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onSeal = fn
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
// (where appropriate) propose CmdAckDelete. Part of gastrolog-51gme's
// receipt-based deletion protocol.
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
// that the decommissioned node would otherwise have blocked. See
// gastrolog-51gme step 10.
func (f *FSM) SetOnPruneNode(fn func(prunedNodeID string, finalizable []chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onPruneNode = fn
}

// SetOnPublishFence registers a callback invoked (outside the FSM lock)
// after CmdPublishFence applies successfully.
func (f *FSM) SetOnPublishFence(fn func(FenceRecord)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onPublishFence = fn
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
// gastrologv1.VaultCtlCommand (gastrolog-5lrg7).
//
// The OnDelete callback (set via SetOnDelete) is invoked OUTSIDE the FSM
// mutex so that potentially-slow filesystem operations don't block other
// Apply calls. The callback fires exactly once per actual deletion.
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
	createdEntry       *ManifestEntry
	deletedID          *chunk.ChunkID
	uploadedEntry      *ManifestEntry
	sealedEntry        *ManifestEntry
	retentionPendingID *chunk.ChunkID
	requestedDelete    *PendingDelete
	ackedDeleteID      *chunk.ChunkID
	ackedDeleteNodeID  string
	finalizedDeleteID  *chunk.ChunkID
	prunedNode         string
	prunedFinalizable  []chunk.ChunkID
	publishedFence     *FenceRecord

	onCreate           func(ManifestEntry)
	onDelete           func(chunk.ChunkID)
	onUpload           func(ManifestEntry)
	onSeal             func(ManifestEntry)
	onRetentionPending func(chunk.ChunkID)
	onRequestDelete    func(PendingDelete)
	onAckDelete        func(chunk.ChunkID, string)
	onFinalizeDelete   func(chunk.ChunkID)
	onPruneNode        func(string, []chunk.ChunkID)
	onPublishFence     func(FenceRecord)
}

func (e applyEffects) fire() {
	if e.createdEntry != nil && e.onCreate != nil {
		e.onCreate(*e.createdEntry)
	}
	if e.deletedID != nil && e.onDelete != nil {
		e.onDelete(*e.deletedID)
	}
	if e.uploadedEntry != nil && e.onUpload != nil {
		e.onUpload(*e.uploadedEntry)
	}
	if e.sealedEntry != nil && e.onSeal != nil {
		e.onSeal(*e.sealedEntry)
	}
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
	if e.publishedFence != nil && e.onPublishFence != nil {
		e.onPublishFence(*e.publishedFence)
	}
	// gastrolog-15fm8: applyPruneNode now finalizes drained-ExpectedFrom
	// chunks atomically inside the same apply. Fire onFinalizeDelete
	// per chunk so audit / cache-eviction subscribers see the same
	// stream of finalize signals they would have if a CmdFinalizeDelete
	// had applied per chunk. Skip if onPruneNode was the subscriber's
	// only hook (they get the same information through the slice).
	if e.onFinalizeDelete != nil {
		for _, id := range e.prunedFinalizable {
			e.onFinalizeDelete(id)
		}
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
		result = f.applySeal(c.SealChunk)
		fx.sealedEntry = f.captureEntry(result, chunkIDFromProto(c.SealChunk.GetId()))
	case *gastrologv1.VaultCtlCommand_CompressChunk:
		result = f.applyCompress(c.CompressChunk)
	case *gastrologv1.VaultCtlCommand_UploadChunk:
		result = f.applyUpload(c.UploadChunk)
		fx.uploadedEntry = f.captureEntry(result, chunkIDFromProto(c.UploadChunk.GetId()))
	case *gastrologv1.VaultCtlCommand_DeleteChunk:
		fx.deletedID, result = f.applyDelete(c.DeleteChunk)
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
		// gastrolog-15fm8: a draining ack atomically finalizes inside
		// the same apply. Surface the finalize to subscribers via the
		// existing onFinalizeDelete callback so audit / cache-eviction
		// paths fire identically to an explicit CmdFinalizeDelete.
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
	case *gastrologv1.VaultCtlCommand_ReserveSeqRange:
		grant, reserveErr := f.applyReserveSeqRange(c.ReserveSeqRange)
		if reserveErr != nil {
			result = reserveErr
		} else {
			result = grant
		}
	case *gastrologv1.VaultCtlCommand_BurnSeqLeaseTail:
		result = f.applyBurnSeqLeaseTail(c.BurnSeqLeaseTail)
	case *gastrologv1.VaultCtlCommand_BumpSeqAllocatorEpoch:
		newEpoch, bumpErr := f.applyBumpSeqAllocatorEpoch(c.BumpSeqAllocatorEpoch)
		if bumpErr != nil {
			result = bumpErr
		} else {
			result = newEpoch
		}
	case *gastrologv1.VaultCtlCommand_PublishFence:
		rec, fenceErr := f.applyPublishFence(c.PublishFence)
		if fenceErr != nil {
			result = fenceErr
		} else {
			result = rec
			fx.publishedFence = rec
		}
	default:
		result = fmt.Errorf("unknown chunk FSM command: %T", cmd.GetCommand())
	}
	fx.onCreate = f.onCreate
	fx.onDelete = f.onDelete
	fx.onUpload = f.onUpload
	fx.onSeal = f.onSeal
	fx.onRetentionPending = f.onRetentionPending
	fx.onRequestDelete = f.onRequestDelete
	fx.onAckDelete = f.onAckDelete
	fx.onFinalizeDelete = f.onFinalizeDelete
	fx.onPruneNode = f.onPruneNode
	fx.onPublishFence = f.onPublishFence
	f.mu.Unlock()

	return result, fx
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

// captureID returns id, or nil if the apply errored. Lock-free.
func captureID(applyResult any, id chunk.ChunkID) *chunk.ChunkID {
	if applyResult != nil {
		return nil
	}
	return &id
}

// SnapshotProto builds the proto representation of all FSM state. Exported
// so the outer vaultraft FSM can embed it in a VaultGroupSnapshot without a
// re-marshal round-trip (gastrolog-5lrg7).
func (f *FSM) SnapshotProto() *gastrologv1.VaultCtlSnapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.snapshotProtoLocked()
}

// snapshotProtoLocked builds the proto snapshot. Map-backed sections (entries,
// tombstones, pending deletes) are emitted in chunk-ID order so equal FSM
// state always yields a byte-identical snapshot. InstallSnapshot does not
// require canonical bytes, but determinism keeps snapshot diffing, debugging,
// and round-trip equality checks sane (gastrolog-5lrg7).
func (f *FSM) snapshotProtoLocked() *gastrologv1.VaultCtlSnapshot {
	snap := &gastrologv1.VaultCtlSnapshot{
		Entries:        make([]*gastrologv1.ManifestEntry, 0, len(f.chunks)),
		Tombstones:     make([]*gastrologv1.Tombstone, 0, len(f.tombstones)),
		PendingDeletes: make([]*gastrologv1.PendingDelete, 0, len(f.pendingDeletes)),
	}

	entryIDs := slices.SortedFunc(maps.Keys(f.chunks), compareChunkID)
	for _, id := range entryIDs {
		snap.Entries = append(snap.Entries, entryToProto(f.chunks[id]))
	}

	tombIDs := slices.SortedFunc(maps.Keys(f.tombstones), compareChunkID)
	for _, id := range tombIDs {
		idCopy := id
		snap.Tombstones = append(snap.Tombstones, &gastrologv1.Tombstone{
			ChunkId:        idCopy[:],
			DeletedAtNanos: f.tombstones[id].UnixNano(),
		})
	}

	pdIDs := slices.SortedFunc(maps.Keys(f.pendingDeletes), compareChunkID)
	for _, id := range pdIDs {
		snap.PendingDeletes = append(snap.PendingDeletes, pendingDeleteToProto(f.pendingDeletes[id]))
	}

	snap.SeqAllocator = seqAllocatorToProto(f.seqAllocatorSnapshotLocked())
	snap.Fences = fenceSnapshotToProto(f.fenceSnapshotLocked())
	return snap
}

// compareChunkID orders chunk IDs by their raw bytes for deterministic
// snapshot section ordering.
func compareChunkID(a, b chunk.ChunkID) int {
	return bytes.Compare(a[:], b[:])
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

	raw, err := io.ReadAll(rc)
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
	applySeqAllocatorSnapshotLocked(f, seqAllocatorFromProto(snap.GetSeqAllocator()))
	applyFenceSnapshotLocked(f, fenceSnapshotFromProto(snap.GetFences()))
	f.ready = true
}

// ---------- Command application ----------

// CreateChunk inserts a new Active chunk entry.
func (f *FSM) applyCreate(c *gastrologv1.CreateChunkCommand) error {
	id := chunkIDFromProto(c.GetId())
	writeStart := time.Unix(0, c.GetWriteStartNanos())
	ingestStart := time.Unix(0, c.GetIngestStartNanos())
	sourceStart := time.Unix(0, c.GetSourceStartNanos())

	// Reject creates for tombstoned chunk IDs. If the vault already applied
	// a DeleteChunk for this ID, a later CreateChunk (late replication /
	// out-of-order Raft apply) must not resurrect it in the live map —
	// that's exactly the ghost-chunk bug from gastrolog-11rzz. The
	// orchestrator's post-import path separately cleans up any on-disk
	// files via the tombstone re-check after announce.
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
	return nil
}

// BeginSeal: Active → Sealing transition. The leader proposes this when its
// rotation policy fires and before sealed-form assembly begins. The
// chunk's metadata still reflects active-form bookkeeping (no WriteEnd
// / final RecordCount yet — those come in CmdSealChunk). Idempotent:
// repeated BeginSeals on the same chunk are harmless. See gastrolog-1huz5.
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
//
// See gastrolog-32bf2.
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

// CompressChunk records the on-disk size of a sealed chunk.
func (f *FSM) applyCompress(c *gastrologv1.CompressChunkCommand) error {
	id := chunkIDFromProto(c.GetId())

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("compress chunk: %s not found", id)
	}
	// Compressed flag is gone (gastrolog-24m1t step 7f) — sealed chunks
	// are GLCB which is zstd-compressed by construction. CmdCompressChunk
	// stays as a no-op apply* handler for WAL-replay backward compat;
	// only DiskBytes still carries useful information.
	e.DiskBytes = c.GetDiskBytes()
	return nil
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
	e.DiskBytes = c.GetDiskBytes()
	e.IngestIdxOffset = c.GetIngestIdxOffset()
	e.IngestIdxSize = c.GetIngestIdxSize()
	e.SourceIdxOffset = c.GetSourceIdxOffset()
	e.SourceIdxSize = c.GetSourceIdxSize()
	e.CloudBacked = true

	// Integrity fields (gastrolog-grnc3) — present only on the extended form.
	if h := c.GetHash(); len(h) > 0 {
		copy(e.Hash[:], h)
		e.CloudServiceID = glid.FromBytes(c.GetCloudServiceId())
		e.KeyScheme = uint8(c.GetKeyScheme()) //nolint:gosec // G115: key scheme is a small enum; round-trips a uint8
	}
	return nil
}

// DeleteChunk removes a chunk entry. Returns the deleted ID (or nil if the
// chunk wasn't present, e.g. on a replayed delete) so Apply can fire the
// onDelete callback exactly once per actual deletion.
func (f *FSM) applyDelete(c *gastrologv1.DeleteChunkCommand) (*chunk.ChunkID, error) {
	id := chunkIDFromProto(c.GetId())
	// Always record the tombstone — even when the chunk isn't currently in
	// the map. A CmdDeleteChunk that races with a pre-delete CmdCreateChunk
	// (via retry or reordered apply) could arrive first; the tombstone
	// ensures the late create-path still gets rejected. Timestamp uses the
	// FSM's notion of "now" — acceptable because every replica applies the
	// same log entry at the same logical time and the tombstone is only
	// used locally to short-circuit replication receivers.
	f.tombstones[id] = time.Now()
	if _, existed := f.chunks[id]; !existed {
		return nil, nil
	}
	delete(f.chunks, id)
	return &id, nil
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
// can wrap it without a re-marshal round-trip (gastrolog-5lrg7).

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
func NewSealChunk(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestStart, ingestEnd, sourceEnd time.Time, ingestTSMonotonic bool) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_SealChunk{SealChunk: &gastrologv1.SealChunkCommand{
		Id:                id[:],
		WriteEndNanos:     writeEnd.UnixNano(),
		RecordCount:       recordCount,
		Bytes:             bytes,
		IngestEndNanos:    ingestEnd.UnixNano(),
		SourceEndNanos:    sourceEnd.UnixNano(),
		IngestStartNanos:  ingestStart.UnixNano(),
		IngestTsMonotonic: ingestTSMonotonic,
	}}}
}

// MarshalSealChunk builds the Raft log data for a SealChunk command.
func MarshalSealChunk(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestStart, ingestEnd, sourceEnd time.Time, ingestTSMonotonic bool) []byte {
	return mustMarshalCommand(NewSealChunk(id, writeEnd, recordCount, bytes, ingestStart, ingestEnd, sourceEnd, ingestTSMonotonic))
}

// NewBeginSeal builds a BeginSeal command message. gastrolog-1huz5.
func NewBeginSeal(id chunk.ChunkID) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_BeginSeal{BeginSeal: &gastrologv1.BeginSealCommand{Id: id[:]}}}
}

// MarshalBeginSeal builds the Raft log data for a BeginSeal command.
func MarshalBeginSeal(id chunk.ChunkID) []byte {
	return mustMarshalCommand(NewBeginSeal(id))
}

// NewCompressChunk builds a CompressChunk command message.
func NewCompressChunk(id chunk.ChunkID, diskBytes int64) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_CompressChunk{CompressChunk: &gastrologv1.CompressChunkCommand{Id: id[:], DiskBytes: diskBytes}}}
}

// MarshalCompressChunk builds the Raft log data for a CompressChunk command.
func MarshalCompressChunk(id chunk.ChunkID, diskBytes int64) []byte {
	return mustMarshalCommand(NewCompressChunk(id, diskBytes))
}

// NewUploadChunk builds an UploadChunk command message. The integrity
// fields (hash, cloud service ID, key scheme) are gastrolog-grnc3 additions.
func NewUploadChunk(id chunk.ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, hash [32]byte, cloudServiceID glid.GLID, keyScheme uint8) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_UploadChunk{UploadChunk: &gastrologv1.UploadChunkCommand{
		Id:              id[:],
		DiskBytes:       diskBytes,
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
func MarshalUploadChunk(id chunk.ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, hash [32]byte, cloudServiceID glid.GLID, keyScheme uint8) []byte {
	return mustMarshalCommand(NewUploadChunk(id, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize, hash, cloudServiceID, keyScheme))
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

// NewDeleteChunk builds a DeleteChunk command message.
func NewDeleteChunk(id chunk.ChunkID) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_DeleteChunk{DeleteChunk: &gastrologv1.DeleteChunkCommand{Id: id[:]}}}
}

// MarshalDeleteChunk builds the Raft log data for a DeleteChunk command.
func MarshalDeleteChunk(id chunk.ChunkID) []byte {
	return mustMarshalCommand(NewDeleteChunk(id))
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
// repatriatable. See gastrolog-32bf2.
func NewRepatriateChunk(entry ManifestEntry) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_RepatriateChunk{RepatriateChunk: &gastrologv1.RepatriateChunkCommand{Entry: entryToProto(&entry)}}}
}

// MarshalRepatriateChunk builds the Raft log data for a RepatriateChunk
// command. Returns an error for signature compatibility with callers; the
// proto build cannot fail.
func MarshalRepatriateChunk(entry ManifestEntry) ([]byte, error) {
	return mustMarshalCommand(NewRepatriateChunk(entry)), nil
}

// entryToProto converts a ManifestEntry to its proto representation,
// carrying every field including Hash / CloudServiceID / KeyScheme.
func entryToProto(e *ManifestEntry) *gastrologv1.ManifestEntry {
	return &gastrologv1.ManifestEntry{
		Id:                e.ID[:],
		WriteStartNanos:   e.WriteStart.UnixNano(),
		WriteEndNanos:     e.WriteEnd.UnixNano(),
		RecordCount:       e.RecordCount,
		Bytes:             e.Bytes,
		State:             gastrologv1.ChunkState(e.State),
		DiskBytes:         e.DiskBytes,
		IngestStartNanos:  e.IngestStart.UnixNano(),
		IngestEndNanos:    e.IngestEnd.UnixNano(),
		SourceStartNanos:  e.SourceStart.UnixNano(),
		SourceEndNanos:    e.SourceEnd.UnixNano(),
		IngestTsMonotonic: e.IngestTSMonotonic,
		CloudBacked:       e.CloudBacked,
		Archived:          e.Archived,
		RetentionPending:  e.RetentionPending,
		IngestIdxOffset:   e.IngestIdxOffset,
		IngestIdxSize:     e.IngestIdxSize,
		SourceIdxOffset:   e.SourceIdxOffset,
		SourceIdxSize:     e.SourceIdxSize,
		Hash:              e.Hash[:],
		CloudServiceId:    e.CloudServiceID[:],
		KeyScheme:         uint32(e.KeyScheme),
	}
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
		DiskBytes:         p.GetDiskBytes(),
		IngestStart:       time.Unix(0, p.GetIngestStartNanos()),
		IngestEnd:         time.Unix(0, p.GetIngestEndNanos()),
		SourceStart:       time.Unix(0, p.GetSourceStartNanos()),
		SourceEnd:         time.Unix(0, p.GetSourceEndNanos()),
		IngestTSMonotonic: p.GetIngestTsMonotonic(),
		CloudBacked:       p.GetCloudBacked(),
		Archived:          p.GetArchived(),
		RetentionPending:  p.GetRetentionPending(),
		IngestIdxOffset:   p.GetIngestIdxOffset(),
		IngestIdxSize:     p.GetIngestIdxSize(),
		SourceIdxOffset:   p.GetSourceIdxOffset(),
		SourceIdxSize:     p.GetSourceIdxSize(),
		CloudServiceID:    glid.FromBytes(p.GetCloudServiceId()),
		KeyScheme:         uint8(p.GetKeyScheme()), //nolint:gosec // G115: key scheme is a small enum; round-trips a uint8
	}
	copy(e.Hash[:], p.GetHash())
	return e
}

// ---------- Snapshot ----------
//
// The snapshot is a marshaled gastrologv1.VaultCtlSnapshot (gastrolog-5lrg7).
// It replaced a hand-rolled versioned section format (magic "GLTRSNAP" +
// per-kind length-prefixed sections). Old snapshots that predate the proto
// format are NOT backward-readable by design; the project permits cluster
// re-initialization (zero deployments) and Raft regenerates snapshots from
// the log on the next apply cycle.

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
