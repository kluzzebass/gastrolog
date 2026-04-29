package raftfsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"time"

	"gastrolog/internal/chunk"

	hraft "github.com/hashicorp/raft"
)

// Command identifies the type of chunk metadata mutation.
type Command byte

const (
	CmdCreateChunk   Command = 1
	CmdSealChunk     Command = 2
	CmdCompressChunk Command = 3
	CmdUploadChunk   Command = 4
	CmdDeleteChunk      Command = 5
	CmdRetentionPending    Command = 6
	CmdTransitionStreamed  Command = 7
	CmdTransitionReceived Command = 8 // destination tier records receipt of a source chunk

	// Receipt-based deletion protocol — gastrolog-51gme step 2. Replaces
	// single-shot CmdDeleteChunk fan-out with an N-way receipt protocol
	// that survives snapshot install and gives every node a first-class
	// "delete locally and ack" obligation. See docs in fsm_receipts.go.
	CmdRequestDelete  Command = 9  // tier leader proposes a delete; replicates the expected-acks set
	CmdAckDelete      Command = 10 // each expected node acks after handling its local side
	CmdFinalizeDelete Command = 11 // leader removes the entry once expectedFrom is empty
)

// Entry holds the full metadata for one chunk in the FSM.
// This is the Raft-replicated equivalent of file.Manager.chunkMeta + cloudIdx entries.
type Entry struct {
	ID          chunk.ChunkID
	WriteStart  time.Time
	WriteEnd    time.Time
	RecordCount int64
	Bytes       int64
	Sealed      bool
	Compressed  bool
	DiskBytes   int64

	IngestStart time.Time
	IngestEnd   time.Time
	SourceStart time.Time
	SourceEnd   time.Time

	CloudBacked      bool
	Archived         bool
	RetentionPending    bool
	TransitionStreamed  bool // chunk records have been streamed to the next tier but deletion awaits destination replication confirmation
	NumFrames           int32

	// Cloud-specific TOC offsets (GLCB format).
	IngestIdxOffset int64
	IngestIdxSize   int64
	SourceIdxOffset int64
	SourceIdxSize   int64
}

// ToChunkMeta converts to the public chunk.ChunkMeta type.
func (e *Entry) ToChunkMeta() chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:          e.ID,
		WriteStart:  e.WriteStart,
		WriteEnd:    e.WriteEnd,
		RecordCount: e.RecordCount,
		Bytes:       e.Bytes,
		Sealed:      e.Sealed,
		Compressed:  e.Compressed,
		DiskBytes:   e.DiskBytes,
		IngestStart: e.IngestStart,
		IngestEnd:   e.IngestEnd,
		SourceStart: e.SourceStart,
		SourceEnd:   e.SourceEnd,
		CloudBacked: e.CloudBacked,
		Archived:    e.Archived,
		NumFrames:   e.NumFrames,
	}
}

// FSM is a Raft FSM that maintains chunk metadata for a single tier.
// All reads are local (no Raft round-trip). Writes go through Raft.Apply().
type FSM struct {
	mu       sync.RWMutex
	chunks   map[chunk.ChunkID]*Entry
	ready    bool // true after first Apply or Restore
	onDelete func(chunk.ChunkID)
	onUpload func(Entry) // called after CmdUploadChunk applies (outside lock)

	// Step-1 reconciler-wiring hooks for gastrolog-51gme. Each fires
	// outside the FSM mutex after the corresponding Cmd applies, so the
	// reconciler can project FSM state changes into local Manager state
	// without polling. No callers wired yet — adding the surface here
	// unblocks subsequent steps without requiring an FSM API churn.
	onSeal               func(Entry)         // CmdSealChunk applied; passes the now-sealed entry
	onRetentionPending   func(chunk.ChunkID) // CmdRetentionPending applied
	onTransitionStreamed func(chunk.ChunkID) // CmdTransitionStreamed applied (source-tier signal)
	onTransitionReceived func(chunk.ChunkID) // CmdTransitionReceived applied (destination-tier confirmation, ID is the source chunk's ID)

	// Step-2 receipt-protocol state and hooks for gastrolog-51gme.
	// pendingDeletes is the queue of chunk deletes awaiting per-node
	// acknowledgement. See PendingDelete and the apply* functions in
	// fsm_receipts.go.
	pendingDeletes map[chunk.ChunkID]*PendingDelete

	onRequestDelete  func(PendingDelete) // CmdRequestDelete applied; passes a copy of the new entry
	onAckDelete      func(chunk.ChunkID, string) // CmdAckDelete applied; (chunkID, ackingNodeID)
	onFinalizeDelete func(chunk.ChunkID)         // CmdFinalizeDelete applied; expectedFrom was empty

	// transitionReceipts tracks source chunk IDs whose records have been
	// imported into this tier. The source tier checks this set to confirm
	// that the destination (this tier) has durably committed the data
	// before expiring the source chunk. Keyed by source chunk ID.
	// See gastrolog-4913n.
	transitionReceipts map[chunk.ChunkID]bool

	// tombstones records chunk IDs that have been deleted, with the apply
	// timestamp of the delete. Consulted by the receive side of tier
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
}

// New creates an empty chunk metadata FSM.
func New() *FSM {
	return &FSM{
		chunks:             make(map[chunk.ChunkID]*Entry),
		transitionReceipts: make(map[chunk.ChunkID]bool),
		tombstones:         make(map[chunk.ChunkID]time.Time),
		pendingDeletes:     make(map[chunk.ChunkID]*PendingDelete),
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
func (f *FSM) SetOnUpload(fn func(Entry)) {
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
func (f *FSM) SetOnSeal(fn func(Entry)) {
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

// SetOnTransitionStreamed registers a callback invoked (outside the FSM
// lock) after CmdTransitionStreamed applies. The callback receives the
// source-side chunk ID. Source-tier reconcilers use this to track which
// chunks are awaiting destination receipt before local expiry.
func (f *FSM) SetOnTransitionStreamed(fn func(chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onTransitionStreamed = fn
}

// SetOnTransitionReceived registers a callback invoked (outside the FSM
// lock) after CmdTransitionReceived applies. The callback receives the
// SOURCE chunk ID — the destination tier's FSM is recording a receipt
// for the source. Destination-tier reconcilers don't need this; the
// source-tier reconciler does (paired with the cross-tier confirmation
// sweep), and any node hosting both source and destination tiers can
// register the callback on each tier's FSM.
func (f *FSM) SetOnTransitionReceived(fn func(chunk.ChunkID)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.onTransitionReceived = fn
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

// ---------- Reads (local, no Raft) ----------

// Get returns a copy of a chunk's metadata, or nil if not found.
func (f *FSM) Get(id chunk.ChunkID) *Entry {
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
func (f *FSM) List() []Entry {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]Entry, 0, len(f.chunks))
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

// Apply handles a Raft log entry. The log data is a command byte followed
// by the command-specific payload.
//
// The OnDelete callback (set via SetOnDelete) is invoked OUTSIDE the FSM
// mutex so that potentially-slow filesystem operations don't block other
// Apply calls. The callback fires exactly once per actual deletion.
func (f *FSM) Apply(log *hraft.Log) any {
	if len(log.Data) == 0 {
		return errors.New("empty chunk FSM command")
	}
	cmd := Command(log.Data[0])
	payload := log.Data[1:]

	result, fx := f.applyLocked(cmd, payload)
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
	deletedID            *chunk.ChunkID
	uploadedEntry        *Entry
	sealedEntry          *Entry
	retentionPendingID   *chunk.ChunkID
	transitionStreamedID *chunk.ChunkID
	transitionReceivedID *chunk.ChunkID
	requestedDelete      *PendingDelete
	ackedDeleteID        *chunk.ChunkID
	ackedDeleteNodeID    string
	finalizedDeleteID    *chunk.ChunkID

	onDelete             func(chunk.ChunkID)
	onUpload             func(Entry)
	onSeal               func(Entry)
	onRetentionPending   func(chunk.ChunkID)
	onTransitionStreamed func(chunk.ChunkID)
	onTransitionReceived func(chunk.ChunkID)
	onRequestDelete      func(PendingDelete)
	onAckDelete          func(chunk.ChunkID, string)
	onFinalizeDelete     func(chunk.ChunkID)
}

func (e applyEffects) fire() {
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
	if e.transitionStreamedID != nil && e.onTransitionStreamed != nil {
		e.onTransitionStreamed(*e.transitionStreamedID)
	}
	if e.transitionReceivedID != nil && e.onTransitionReceived != nil {
		e.onTransitionReceived(*e.transitionReceivedID)
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
}

// applyLocked dispatches to the per-command apply function under the
// FSM mutex and gathers the post-apply effects.
func (f *FSM) applyLocked(cmd Command, payload []byte) (any, applyEffects) {
	var (
		result any
		fx     applyEffects
	)

	f.mu.Lock()
	f.ready = true
	switch cmd {
	case CmdCreateChunk:
		result = f.applyCreate(payload)
	case CmdSealChunk:
		result = f.applySeal(payload)
		fx.sealedEntry = f.captureEntry(result, payload)
	case CmdCompressChunk:
		result = f.applyCompress(payload)
	case CmdUploadChunk:
		result = f.applyUpload(payload)
		fx.uploadedEntry = f.captureEntry(result, payload)
	case CmdDeleteChunk:
		fx.deletedID, result = f.applyDelete(payload)
	case CmdRetentionPending:
		result = f.applyRetentionPending(payload)
		fx.retentionPendingID = captureID(result, payload)
	case CmdTransitionStreamed:
		result = f.applyTransitionStreamed(payload)
		fx.transitionStreamedID = captureID(result, payload)
	case CmdTransitionReceived:
		result = f.applyTransitionReceived(payload)
		fx.transitionReceivedID = captureID(result, payload)
	case CmdRequestDelete:
		var entry *PendingDelete
		entry, result = f.applyRequestDelete(payload)
		fx.requestedDelete = entry
	case CmdAckDelete:
		var (
			id     *chunk.ChunkID
			nodeID string
		)
		id, nodeID, result = f.applyAckDelete(payload)
		fx.ackedDeleteID = id
		fx.ackedDeleteNodeID = nodeID
	case CmdFinalizeDelete:
		fx.finalizedDeleteID, result = f.applyFinalizeDelete(payload)
	default:
		result = fmt.Errorf("unknown chunk FSM command: %d", cmd)
	}
	fx.onDelete = f.onDelete
	fx.onUpload = f.onUpload
	fx.onSeal = f.onSeal
	fx.onRetentionPending = f.onRetentionPending
	fx.onTransitionStreamed = f.onTransitionStreamed
	fx.onTransitionReceived = f.onTransitionReceived
	fx.onRequestDelete = f.onRequestDelete
	fx.onAckDelete = f.onAckDelete
	fx.onFinalizeDelete = f.onFinalizeDelete
	f.mu.Unlock()

	return result, fx
}

// captureEntry returns a copy of the chunk entry whose ID is the first
// 16 bytes of payload, or nil if the apply errored or the entry is
// absent. Caller MUST hold f.mu.
func (f *FSM) captureEntry(applyResult any, payload []byte) *Entry {
	if applyResult != nil || len(payload) < 16 {
		return nil
	}
	var id chunk.ChunkID
	copy(id[:], payload[:16])
	e := f.chunks[id]
	if e == nil {
		return nil
	}
	cp := *e
	return &cp
}

// captureID returns the chunk ID at the start of payload, or nil if the
// apply errored or the payload is too short for an ID. Lock-free.
func captureID(applyResult any, payload []byte) *chunk.ChunkID {
	if applyResult != nil || len(payload) < 16 {
		return nil
	}
	var id chunk.ChunkID
	copy(id[:], payload[:16])
	return &id
}

// Snapshot returns a point-in-time snapshot of all chunk metadata.
func (f *FSM) Snapshot() (hraft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	entries := make([]Entry, 0, len(f.chunks))
	for _, e := range f.chunks {
		entries = append(entries, *e)
	}
	receipts := make([]chunk.ChunkID, 0, len(f.transitionReceipts))
	for id := range f.transitionReceipts {
		receipts = append(receipts, id)
	}
	tombstones := make(map[chunk.ChunkID]time.Time, len(f.tombstones))
	maps.Copy(tombstones, f.tombstones)
	pendingDeletes := make([]PendingDelete, 0, len(f.pendingDeletes))
	for _, p := range f.pendingDeletes {
		pendingDeletes = append(pendingDeletes, p.Copy())
	}
	return &fsmSnapshot{
		entries:        entries,
		receipts:       receipts,
		tombstones:     tombstones,
		pendingDeletes: pendingDeletes,
	}, nil
}

// Restore replaces FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	snap, err := decodeSnapshot(rc)
	if err != nil {
		return fmt.Errorf("restore chunk FSM: %w", err)
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	f.chunks = make(map[chunk.ChunkID]*Entry, len(snap.entries))
	for i := range snap.entries {
		f.chunks[snap.entries[i].ID] = &snap.entries[i]
	}
	f.transitionReceipts = make(map[chunk.ChunkID]bool, len(snap.receipts))
	for _, id := range snap.receipts {
		f.transitionReceipts[id] = true
	}
	f.tombstones = snap.tombstones
	if f.tombstones == nil {
		f.tombstones = make(map[chunk.ChunkID]time.Time)
	}
	f.pendingDeletes = snap.pendingDeletes
	if f.pendingDeletes == nil {
		f.pendingDeletes = make(map[chunk.ChunkID]*PendingDelete)
	}
	f.ready = true
	return nil
}

// ---------- Command application ----------

// CreateChunk: [16 bytes ChunkID][8 bytes WriteStart nanos][8 bytes IngestStart nanos][8 bytes SourceStart nanos]
func (f *FSM) applyCreate(data []byte) error {
	if len(data) < 40 {
		return fmt.Errorf("create chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	writeStart := time.Unix(0, int64(binary.BigEndian.Uint64(data[16:24])))   //nolint:gosec // G115: safe round-trip from uint64 nano timestamp
	ingestStart := time.Unix(0, int64(binary.BigEndian.Uint64(data[24:32]))) //nolint:gosec // G115: safe round-trip from uint64 nano timestamp
	sourceStart := time.Unix(0, int64(binary.BigEndian.Uint64(data[32:40]))) //nolint:gosec // G115: safe round-trip from uint64 nano timestamp

	// Reject creates for tombstoned chunk IDs. If the tier already applied
	// a DeleteChunk for this ID, a later CreateChunk (late replication /
	// out-of-order Raft apply) must not resurrect it in the live map —
	// that's exactly the ghost-chunk bug from gastrolog-11rzz. The
	// orchestrator's post-import path separately cleans up any on-disk
	// files via the tombstone re-check after announce.
	if _, dead := f.tombstones[id]; dead {
		return nil
	}

	f.chunks[id] = &Entry{
		ID:          id,
		WriteStart:  writeStart,
		IngestStart: ingestStart,
		SourceStart: sourceStart,
	}
	return nil
}

// SealChunk: [16 bytes ChunkID][8 WriteEnd][8 RecordCount][8 Bytes][8 IngestEnd][8 SourceEnd]
func (f *FSM) applySeal(data []byte) error {
	if len(data) < 56 {
		return fmt.Errorf("seal chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("seal chunk: %s not found", id)
	}
	e.WriteEnd = time.Unix(0, int64(binary.BigEndian.Uint64(data[16:24])))   //nolint:gosec // G115: nano timestamp round-trip
	e.RecordCount = int64(binary.BigEndian.Uint64(data[24:32]))             //nolint:gosec // G115: record count round-trip
	e.Bytes = int64(binary.BigEndian.Uint64(data[32:40]))                   //nolint:gosec // G115: byte count round-trip
	e.IngestEnd = time.Unix(0, int64(binary.BigEndian.Uint64(data[40:48]))) //nolint:gosec // G115: nano timestamp round-trip
	e.SourceEnd = time.Unix(0, int64(binary.BigEndian.Uint64(data[48:56]))) //nolint:gosec // G115: nano timestamp round-trip
	e.Sealed = true
	return nil
}

// CompressChunk: [16 bytes ChunkID][8 DiskBytes]
func (f *FSM) applyCompress(data []byte) error {
	if len(data) < 24 {
		return fmt.Errorf("compress chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("compress chunk: %s not found", id)
	}
	e.DiskBytes = int64(binary.BigEndian.Uint64(data[16:24])) //nolint:gosec // G115: round-trip
	e.Compressed = true
	return nil
}

// UploadChunk: [16 ChunkID][8 DiskBytes][8 IngestIdxOff][8 IngestIdxSize][8 SourceIdxOff][8 SourceIdxSize][4 NumFrames]
func (f *FSM) applyUpload(data []byte) error {
	if len(data) < 60 {
		return fmt.Errorf("upload chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])

	e := f.chunks[id]
	if e == nil {
		return fmt.Errorf("upload chunk: %s not found", id)
	}
	e.DiskBytes = int64(binary.BigEndian.Uint64(data[16:24]))       //nolint:gosec // G115: round-trip
	e.IngestIdxOffset = int64(binary.BigEndian.Uint64(data[24:32])) //nolint:gosec // G115: round-trip
	e.IngestIdxSize = int64(binary.BigEndian.Uint64(data[32:40]))   //nolint:gosec // G115: round-trip
	e.SourceIdxOffset = int64(binary.BigEndian.Uint64(data[40:48])) //nolint:gosec // G115: round-trip
	e.SourceIdxSize = int64(binary.BigEndian.Uint64(data[48:56]))   //nolint:gosec // G115: round-trip
	e.NumFrames = int32(binary.BigEndian.Uint32(data[56:60])) //nolint:gosec // G115: safe round-trip from uint32 frame count
	e.CloudBacked = true
	return nil
}

// DeleteChunk: [16 bytes ChunkID]. Returns the deleted ID (or nil if the
// chunk wasn't present, e.g. on a replayed delete) so Apply can fire the
// onDelete callback exactly once per actual deletion.
func (f *FSM) applyDelete(data []byte) (*chunk.ChunkID, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("delete chunk: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
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

// RetentionPending: [16 bytes ChunkID]
func (f *FSM) applyRetentionPending(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("retention pending: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	if e := f.chunks[id]; e != nil {
		e.RetentionPending = true
	}
	return nil
}

// TransitionStreamed: [16 bytes ChunkID]
func (f *FSM) applyTransitionStreamed(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("transition streamed: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	if e := f.chunks[id]; e != nil {
		e.TransitionStreamed = true
	}
	return nil
}

// ---------- Command builders (used by callers before Raft.Apply) ----------

// MarshalCreateChunk builds the Raft log data for a CreateChunk command.
func MarshalCreateChunk(id chunk.ChunkID, writeStart, ingestStart, sourceStart time.Time) []byte {
	buf := make([]byte, 1+40)
	buf[0] = byte(CmdCreateChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(writeStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[25:33], uint64(ingestStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[33:41], uint64(sourceStart.UnixNano()))
	return buf
}

// MarshalSealChunk builds the Raft log data for a SealChunk command.
func MarshalSealChunk(id chunk.ChunkID, writeEnd time.Time, recordCount, bytes int64, ingestEnd, sourceEnd time.Time) []byte {
	buf := make([]byte, 1+56)
	buf[0] = byte(CmdSealChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(writeEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[25:33], uint64(recordCount)) //nolint:gosec // G115: safe round-trip for record count
	binary.BigEndian.PutUint64(buf[33:41], uint64(bytes))     //nolint:gosec // G115: safe round-trip for byte count
	binary.BigEndian.PutUint64(buf[41:49], uint64(ingestEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[49:57], uint64(sourceEnd.UnixNano()))
	return buf
}

// MarshalCompressChunk builds the Raft log data for a CompressChunk command.
func MarshalCompressChunk(id chunk.ChunkID, diskBytes int64) []byte {
	buf := make([]byte, 1+24)
	buf[0] = byte(CmdCompressChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(diskBytes)) //nolint:gosec // G115: safe round-trip for disk bytes
	return buf
}

// MarshalUploadChunk builds the Raft log data for an UploadChunk command.
func MarshalUploadChunk(id chunk.ChunkID, diskBytes, ingestIdxOff, ingestIdxSize, sourceIdxOff, sourceIdxSize int64, numFrames int32) []byte {
	buf := make([]byte, 1+60)
	buf[0] = byte(CmdUploadChunk)
	copy(buf[1:17], id[:])
	binary.BigEndian.PutUint64(buf[17:25], uint64(diskBytes))    //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[25:33], uint64(ingestIdxOff))  //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[33:41], uint64(ingestIdxSize)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[41:49], uint64(sourceIdxOff))  //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[49:57], uint64(sourceIdxSize)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint32(buf[57:61], uint32(numFrames)) //nolint:gosec // G115: safe round-trip for frame count
	return buf
}

// MarshalDeleteChunk builds the Raft log data for a DeleteChunk command.
func MarshalDeleteChunk(id chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdDeleteChunk)
	copy(buf[1:17], id[:])
	return buf
}

// MarshalRetentionPending builds the Raft log data for a RetentionPending command.
func MarshalRetentionPending(id chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdRetentionPending)
	copy(buf[1:17], id[:])
	return buf
}

// TransitionReceived: [16 bytes source ChunkID]
func (f *FSM) applyTransitionReceived(data []byte) error {
	if len(data) < 16 {
		return fmt.Errorf("transition received: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	f.transitionReceipts[id] = true
	return nil
}

// HasTransitionReceipt returns true if this tier has committed a receipt
// for the given source chunk ID. Called by the source tier's confirmation
// sweep to verify the destination has durably received the records.
func (f *FSM) HasTransitionReceipt(sourceChunkID chunk.ChunkID) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.transitionReceipts[sourceChunkID]
}

// MarshalTransitionReceived builds the Raft log data for a TransitionReceived command.
func MarshalTransitionReceived(sourceChunkID chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdTransitionReceived)
	copy(buf[1:17], sourceChunkID[:])
	return buf
}

// MarshalTransitionStreamed builds the Raft log data for a TransitionStreamed command.
func MarshalTransitionStreamed(id chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdTransitionStreamed)
	copy(buf[1:17], id[:])
	return buf
}

// ---------- Snapshot ----------
//
// Format (version 1):
//
//	[8 bytes magic "GLTRSNAP"]
//	[4 bytes version: uint32 big-endian]
//	[repeating sections until EOF:]
//	  [1 byte sectionKind]
//	  [4 bytes payload length: uint32 big-endian]
//	  [payload bytes]
//
// Section kinds:
//	1 = chunk entries   (payload: N×126 byte fixed entries)
//	2 = transition receipts (payload: 4 byte count + N×16 byte IDs)
//	3 = tombstones      (payload: 4 byte count + N×(16 ID + 8 nanos))
//
// Replaces an older sentinel-based layout that read 126-byte entries until
// EOF and used impossible-ChunkID markers to in-band the extra sections.
// The sentinel approach worked but grew brittle with each new section and
// assumed first-byte values that new identity schemes could eventually
// violate. Versioned header lets the decoder switch cleanly and gives us
// real room to evolve. Old snapshots that predate this format are NOT
// backward-readable by design; Raft will regenerate snapshots from the
// log on next apply cycle, and pre-production data dirs get wiped anyway.

var snapshotMagic = [8]byte{'G', 'L', 'T', 'R', 'S', 'N', 'A', 'P'}

const snapshotVersion uint32 = 1

type sectionKind byte

const (
	sectionEntries        sectionKind = 1
	sectionReceipts       sectionKind = 2
	sectionTombstones     sectionKind = 3
	sectionPendingDeletes sectionKind = 4 // gastrolog-51gme step 2
)

type fsmSnapshot struct {
	entries        []Entry
	receipts       []chunk.ChunkID
	tombstones     map[chunk.ChunkID]time.Time
	pendingDeletes []PendingDelete // gastrolog-51gme step 2
}

func (s *fsmSnapshot) Persist(sink hraft.SnapshotSink) error {
	if err := writeSnapshotHeader(sink); err != nil {
		_ = sink.Cancel()
		return err
	}

	// Section: entries. Payload is N×entrySize bytes.
	entriesPayloadLen := uint32(len(s.entries)) * entrySize //nolint:gosec // G115: entry count fits uint32
	if err := writeSectionHeader(sink, sectionEntries, entriesPayloadLen); err != nil {
		_ = sink.Cancel()
		return err
	}
	for i := range s.entries {
		if err := encodeEntry(sink, &s.entries[i]); err != nil {
			_ = sink.Cancel()
			return err
		}
	}

	// Section: receipts. Payload is 4-byte count + N×16 IDs.
	if len(s.receipts) > 0 {
		payloadLen := uint32(4 + len(s.receipts)*16) //nolint:gosec // G115: fits uint32
		if err := writeSectionHeader(sink, sectionReceipts, payloadLen); err != nil {
			_ = sink.Cancel()
			return err
		}
		var countBuf [4]byte
		binary.BigEndian.PutUint32(countBuf[:], uint32(len(s.receipts))) //nolint:gosec // G115: fits uint32
		if _, err := sink.Write(countBuf[:]); err != nil {
			_ = sink.Cancel()
			return err
		}
		for _, id := range s.receipts {
			if _, err := sink.Write(id[:]); err != nil {
				_ = sink.Cancel()
				return err
			}
		}
	}

	// Section: tombstones. Payload is 4-byte count + N×(16 ID + 8 nanos).
	if len(s.tombstones) > 0 {
		payloadLen := uint32(4 + len(s.tombstones)*24) //nolint:gosec // G115: fits uint32
		if err := writeSectionHeader(sink, sectionTombstones, payloadLen); err != nil {
			_ = sink.Cancel()
			return err
		}
		var countBuf [4]byte
		binary.BigEndian.PutUint32(countBuf[:], uint32(len(s.tombstones))) //nolint:gosec // G115: fits uint32
		if _, err := sink.Write(countBuf[:]); err != nil {
			_ = sink.Cancel()
			return err
		}
		var tsBuf [24]byte
		for id, ts := range s.tombstones {
			copy(tsBuf[0:16], id[:])
			binary.BigEndian.PutUint64(tsBuf[16:24], uint64(ts.UnixNano()))
			if _, err := sink.Write(tsBuf[:]); err != nil {
				_ = sink.Cancel()
				return err
			}
		}
	}

	// Section: pendingDeletes (gastrolog-51gme step 2). Variable-length
	// per entry; encoder writes the section header with the precomputed
	// payload size.
	if err := encodePendingDeletesSection(sink, s.pendingDeletes); err != nil {
		_ = sink.Cancel()
		return err
	}

	return sink.Close()
}

// writeSnapshotHeader writes the 12-byte header (8 magic + 4 version).
func writeSnapshotHeader(w io.Writer) error {
	if _, err := w.Write(snapshotMagic[:]); err != nil {
		return fmt.Errorf("write snapshot magic: %w", err)
	}
	var verBuf [4]byte
	binary.BigEndian.PutUint32(verBuf[:], snapshotVersion)
	if _, err := w.Write(verBuf[:]); err != nil {
		return fmt.Errorf("write snapshot version: %w", err)
	}
	return nil
}

// writeSectionHeader writes the 5-byte section header (1 kind + 4 length).
func writeSectionHeader(w io.Writer, kind sectionKind, payloadLen uint32) error {
	var buf [5]byte
	buf[0] = byte(kind)
	binary.BigEndian.PutUint32(buf[1:5], payloadLen)
	if _, err := w.Write(buf[:]); err != nil {
		return fmt.Errorf("write section header (kind=%d): %w", kind, err)
	}
	return nil
}

func (s *fsmSnapshot) Release() {}

// Snapshot encoding: each entry is a fixed-size binary record.
// Layout per entry (168 bytes):
//   16  ChunkID
//   8   WriteStart (nanos)
//   8   WriteEnd (nanos)
//   8   RecordCount
//   8   Bytes
//   8   DiskBytes
//   8   IngestStart (nanos)
//   8   IngestEnd (nanos)
//   8   SourceStart (nanos)
//   8   SourceEnd (nanos)
//   8   IngestIdxOffset
//   8   IngestIdxSize
//   8   SourceIdxOffset
//   8   SourceIdxSize
//   4   NumFrames
//   2   Flags (bit 0=sealed, 1=compressed, 2=cloudBacked, 3=archived)
// Total: 126 bytes (keeping it compact)

const entrySize = 126

func encodeEntry(w io.Writer, e *Entry) error {
	var buf [entrySize]byte
	copy(buf[0:16], e.ID[:])
	binary.BigEndian.PutUint64(buf[16:24], uint64(e.WriteStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[24:32], uint64(e.WriteEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[32:40], uint64(e.RecordCount)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[40:48], uint64(e.Bytes))       //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[48:56], uint64(e.DiskBytes))   //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[56:64], uint64(e.IngestStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[64:72], uint64(e.IngestEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[72:80], uint64(e.SourceStart.UnixNano()))
	binary.BigEndian.PutUint64(buf[80:88], uint64(e.SourceEnd.UnixNano()))
	binary.BigEndian.PutUint64(buf[88:96], uint64(e.IngestIdxOffset)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[96:104], uint64(e.IngestIdxSize))   //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[104:112], uint64(e.SourceIdxOffset)) //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint64(buf[112:120], uint64(e.SourceIdxSize))   //nolint:gosec // G115: round-trip
	binary.BigEndian.PutUint32(buf[120:124], uint32(e.NumFrames)) //nolint:gosec // G115: safe round-trip for frame count
	var flags uint16
	if e.Sealed {
		flags |= 1 << 0
	}
	if e.Compressed {
		flags |= 1 << 1
	}
	if e.CloudBacked {
		flags |= 1 << 2
	}
	if e.Archived {
		flags |= 1 << 3
	}
	if e.RetentionPending {
		flags |= 1 << 4
	}
	if e.TransitionStreamed {
		flags |= 1 << 5
	}
	binary.BigEndian.PutUint16(buf[124:126], flags)
	_, err := w.Write(buf[:])
	return err
}

// decodedSnapshot bundles every section the snapshot decoder can
// produce. New sections add a field here rather than churning the
// decodeSnapshot return signature.
type decodedSnapshot struct {
	entries        []Entry
	receipts       []chunk.ChunkID
	tombstones     map[chunk.ChunkID]time.Time
	pendingDeletes map[chunk.ChunkID]*PendingDelete // gastrolog-51gme step 2
}

// decodeSnapshot reads a versioned snapshot. Unknown section kinds are
// skipped (forward compatibility within the same version); unknown
// versions are rejected.
func decodeSnapshot(r io.Reader) (*decodedSnapshot, error) {
	if err := readSnapshotHeader(r); err != nil {
		return nil, err
	}

	out := &decodedSnapshot{}

	var hdr [5]byte
	for {
		n, err := io.ReadFull(r, hdr[:])
		if n == 0 {
			break // clean end-of-stream
		}
		if err != nil {
			return nil, fmt.Errorf("read section header: %w", err)
		}
		kind := sectionKind(hdr[0])
		payloadLen := binary.BigEndian.Uint32(hdr[1:5])

		section := io.LimitReader(r, int64(payloadLen))
		switch kind {
		case sectionEntries:
			out.entries, err = readEntriesSection(section, payloadLen)
		case sectionReceipts:
			out.receipts, err = readReceiptsSection(section)
		case sectionTombstones:
			out.tombstones, err = readTombstonesSection(section)
		case sectionPendingDeletes:
			out.pendingDeletes, err = readPendingDeletesSection(section)
		default:
			// Unknown section — skip. Forward-compat for new sections in
			// the same format version.
			_, err = io.Copy(io.Discard, section)
		}
		if err != nil {
			return nil, fmt.Errorf("section kind=%d: %w", kind, err)
		}
		// Drain any bytes the section reader didn't consume (defensive).
		if _, err := io.Copy(io.Discard, section); err != nil {
			return nil, fmt.Errorf("drain section kind=%d: %w", kind, err)
		}
	}

	return out, nil
}

// readSnapshotHeader validates the magic and version. Returns an error if
// the magic is wrong (corrupt or old-format snapshot) or version is not
// recognized.
func readSnapshotHeader(r io.Reader) error {
	var magic [8]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return fmt.Errorf("read snapshot magic: %w", err)
	}
	if magic != snapshotMagic {
		return errors.New("snapshot magic mismatch — incompatible or corrupt format")
	}
	var verBuf [4]byte
	if _, err := io.ReadFull(r, verBuf[:]); err != nil {
		return fmt.Errorf("read snapshot version: %w", err)
	}
	version := binary.BigEndian.Uint32(verBuf[:])
	if version != snapshotVersion {
		return fmt.Errorf("snapshot version %d unsupported (this build handles %d)", version, snapshotVersion)
	}
	return nil
}

func readEntriesSection(r io.Reader, payloadLen uint32) ([]Entry, error) {
	if payloadLen%entrySize != 0 {
		return nil, fmt.Errorf("entries payload %d bytes not a multiple of %d", payloadLen, entrySize)
	}
	count := payloadLen / entrySize
	entries := make([]Entry, 0, count)
	var buf [entrySize]byte
	for i := range count {
		if _, err := io.ReadFull(r, buf[:]); err != nil {
			return nil, fmt.Errorf("read entry %d: %w", i, err)
		}
		var id chunk.ChunkID
		copy(id[:], buf[0:16])
		flags := binary.BigEndian.Uint16(buf[124:126])
		entries = append(entries, Entry{
			ID:              id,
			WriteStart:      time.Unix(0, int64(binary.BigEndian.Uint64(buf[16:24]))),  //nolint:gosec // G115: round-trip
			WriteEnd:        time.Unix(0, int64(binary.BigEndian.Uint64(buf[24:32]))),  //nolint:gosec // G115: round-trip
			RecordCount:     int64(binary.BigEndian.Uint64(buf[32:40])),                //nolint:gosec // G115: round-trip
			Bytes:           int64(binary.BigEndian.Uint64(buf[40:48])),                //nolint:gosec // G115: round-trip
			DiskBytes:       int64(binary.BigEndian.Uint64(buf[48:56])),                //nolint:gosec // G115: round-trip
			IngestStart:     time.Unix(0, int64(binary.BigEndian.Uint64(buf[56:64]))),  //nolint:gosec // G115: round-trip
			IngestEnd:       time.Unix(0, int64(binary.BigEndian.Uint64(buf[64:72]))),  //nolint:gosec // G115: round-trip
			SourceStart:     time.Unix(0, int64(binary.BigEndian.Uint64(buf[72:80]))),  //nolint:gosec // G115: round-trip
			SourceEnd:       time.Unix(0, int64(binary.BigEndian.Uint64(buf[80:88]))),  //nolint:gosec // G115: round-trip
			IngestIdxOffset: int64(binary.BigEndian.Uint64(buf[88:96])),                //nolint:gosec // G115: round-trip
			IngestIdxSize:   int64(binary.BigEndian.Uint64(buf[96:104])),               //nolint:gosec // G115: round-trip
			SourceIdxOffset: int64(binary.BigEndian.Uint64(buf[104:112])),              //nolint:gosec // G115: round-trip
			SourceIdxSize:   int64(binary.BigEndian.Uint64(buf[112:120])),              //nolint:gosec // G115: round-trip
			NumFrames:       int32(binary.BigEndian.Uint32(buf[120:124])),              //nolint:gosec // G115: round-trip
			Sealed:             flags&(1<<0) != 0,
			Compressed:         flags&(1<<1) != 0,
			CloudBacked:        flags&(1<<2) != 0,
			Archived:           flags&(1<<3) != 0,
			RetentionPending:   flags&(1<<4) != 0,
			TransitionStreamed: flags&(1<<5) != 0,
		})
	}
	return entries, nil
}

func readReceiptsSection(r io.Reader) ([]chunk.ChunkID, error) {
	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf("read receipts count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBuf[:])
	if count == 0 {
		return nil, nil
	}
	buf := make([]byte, int(count)*16)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("read receipts payload: %w", err)
	}
	ids := make([]chunk.ChunkID, count)
	for i := range ids {
		copy(ids[i][:], buf[i*16:(i+1)*16])
	}
	return ids, nil
}

func readTombstonesSection(r io.Reader) (map[chunk.ChunkID]time.Time, error) {
	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf("read tombstones count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBuf[:])
	if count == 0 {
		return nil, nil
	}
	out := make(map[chunk.ChunkID]time.Time, count)
	var entry [24]byte
	for i := range count {
		if _, err := io.ReadFull(r, entry[:]); err != nil {
			return nil, fmt.Errorf("read tombstone %d: %w", i, err)
		}
		var id chunk.ChunkID
		copy(id[:], entry[0:16])
		ns := int64(binary.BigEndian.Uint64(entry[16:24])) //nolint:gosec // G115: nano timestamp round-trip
		out[id] = time.Unix(0, ns)
	}
	return out, nil
}
