package vaultctlfsm

// Receipt-based deletion protocol — gastrolog-51gme step 2.
//
// The single-shot CmdDeleteChunk path can't survive snapshot install:
// when a node has been offline long enough that the rest of the cluster
// has snapshotted past the individual delete log entries, the rejoining
// node's FSM.Restore sets the new state directly without firing the
// per-entry onDelete callbacks. The local Manager never learns about
// those deletions and only catches up via a periodic disk-vs-FSM walk
// (the path we're deleting in step 5).
//
// The replacement is an N-way receipt protocol that lives in the FSM
// state itself, so a snapshot carries it across the boundary intact:
//
//   1. Vault leader proposes CmdRequestDelete(chunkID, expectedFrom, reason).
//      The FSM stores the entry in pendingDeletes with expectedFrom equal
//      to the placement membership at proposal time.
//   2. Each node in expectedFrom handles the local side (delete the file
//      if it has one, no-op if it never had one) and proposes
//      CmdAckDelete(chunkID, nodeID). The FSM removes nodeID from
//      expectedFrom.
//   3. When expectedFrom is empty, the leader proposes
//      CmdFinalizeDelete(chunkID), which removes the entry from
//      pendingDeletes.
//
// Snapshot survivability comes for free: pendingDeletes is part of the
// FSM state and serializes into a new snapshot section. A node restored
// from snapshot sees its own ack obligations and can proceed normally
// — no special catchup path.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
)

// PendingDelete is one entry in the receipt-based deletion queue. The
// deletion is "in flight" until expectedFrom is empty and the leader
// has applied CmdFinalizeDelete.
type PendingDelete struct {
	ChunkID      chunk.ChunkID
	Reason       string          // free-form, single-line; e.g. "retention-ttl", "transition-source-expire"
	ProposedAt   time.Time       // when CmdRequestDelete was applied
	ExpectedFrom map[string]bool // node IDs that still owe a CmdAckDelete; empty = ready to finalize
}

// Copy returns a deep copy safe to hand outside the FSM lock. Used when
// firing the onRequestDelete callback so callers can mutate freely.
func (p *PendingDelete) Copy() PendingDelete {
	out := PendingDelete{
		ChunkID:      p.ChunkID,
		Reason:       p.Reason,
		ProposedAt:   p.ProposedAt,
		ExpectedFrom: make(map[string]bool, len(p.ExpectedFrom)),
	}
	maps.Copy(out.ExpectedFrom, p.ExpectedFrom)
	return out
}

// ---------- Reads (local, no Raft) ----------

// PendingDeletes returns a snapshot of all in-flight deletes. The
// returned slice is freshly allocated; the entries are deep copies.
// Reconcilers walk this on FSM Restore to identify obligations they
// owe to the cluster.
func (f *FSM) PendingDeletes() []PendingDelete {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]PendingDelete, 0, len(f.pendingDeletes))
	for _, p := range f.pendingDeletes {
		out = append(out, p.Copy())
	}
	return out
}

// PendingDelete returns a copy of the in-flight delete entry for
// chunkID, or nil if there is no such entry.
func (f *FSM) PendingDelete(chunkID chunk.ChunkID) *PendingDelete {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p, ok := f.pendingDeletes[chunkID]
	if !ok {
		return nil
	}
	cp := p.Copy()
	return &cp
}

// IsExpectedToAck reports whether this nodeID still owes a CmdAckDelete
// for the given chunkID. False if there is no pending delete for that
// chunk, or if the node already acked, or if the node was never in the
// expected set.
func (f *FSM) IsExpectedToAck(chunkID chunk.ChunkID, nodeID string) bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p, ok := f.pendingDeletes[chunkID]
	if !ok {
		return false
	}
	return p.ExpectedFrom[nodeID]
}

// ChunkResidency returns the set of node IDs that currently hold the
// chunk's bytes, sourced authoritatively from the vault-ctl FSM state.
//
// For chunks WITHOUT an in-flight delete: residency = the supplied
// placement set. Every placement member is expected to hold the chunk
// once catchup has converged; transient under-replication windows
// during catchup are bounded by the missing-replica sweep
// (gastrolog-19241).
//
// For chunks WITH an in-flight delete (entry exists in pendingDeletes):
// residency = ExpectedFrom — the nodes that still owe a CmdAckDelete.
// A node remains in ExpectedFrom until its CmdAckDelete is applied to
// the FSM; until then, it still holds its local copy. Acknowledged
// nodes have already deleted the chunk locally and are correctly
// excluded.
//
// For chunks not in the FSM at all: returns nil. The chunk either
// never existed, was tombstoned, or finalized.
//
// Used by the WatchChunks server handler to stamp authoritative
// replica info on outbound events so clients never have to reconstruct
// it from per-node event evidence. See gastrolog-66vmg.
func (f *FSM) ChunkResidency(chunkID chunk.ChunkID, placementNodeIDs []string) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if _, ok := f.chunks[chunkID]; !ok {
		// Chunk gone from the FSM — fully deleted or never existed.
		// pendingDeletes entries are removed by applyFinalizeDelete
		// before the FSM tombstone, so this branch covers both the
		// "never existed" and "finalized and tombstoned" cases.
		if _, tombstoned := f.tombstones[chunkID]; tombstoned {
			return nil
		}
		return nil
	}
	if p, ok := f.pendingDeletes[chunkID]; ok {
		out := make([]string, 0, len(p.ExpectedFrom))
		for nid := range p.ExpectedFrom {
			out = append(out, nid)
		}
		return out
	}
	if len(placementNodeIDs) == 0 {
		return nil
	}
	out := make([]string, len(placementNodeIDs))
	copy(out, placementNodeIDs)
	return out
}

// ---------- Apply functions (caller MUST hold f.mu) ----------

// applyRequestDelete records a new in-flight delete with the expected-acks
// set captured at proposal time.
func (f *FSM) applyRequestDelete(c *gastrologv1.RequestDeleteCommand) (*PendingDelete, error) {
	chunkID := chunkIDFromProto(c.GetId())

	// Idempotency: if this chunk already has a pending entry, treat
	// the second request as a no-op (and don't fire the callback).
	// Re-proposing CmdRequestDelete would otherwise reset the
	// expectedFrom set and erase any acks already in-flight.
	if _, exists := f.pendingDeletes[chunkID]; exists {
		return nil, nil
	}

	expectedFrom := make(map[string]bool, len(c.GetExpectedFrom()))
	for _, n := range c.GetExpectedFrom() {
		expectedFrom[n] = true
	}
	p := &PendingDelete{
		ChunkID:      chunkID,
		Reason:       c.GetReason(),
		ProposedAt:   time.Unix(0, c.GetProposedAtNanos()),
		ExpectedFrom: expectedFrom,
	}
	f.pendingDeletes[chunkID] = p

	cp := p.Copy()
	return &cp, nil
}

// applyAckDelete removes the acking node from a pending delete's expected set.
//
// Returns the chunk ID and the acking node ID for the post-apply
// callback. If the entry is gone (already finalized) or the node was
// never expected, the apply succeeds but returns nil — Raft has the
// entry, the FSM is consistent, and the callback is suppressed.
//
// gastrolog-15fm8: when this ack drains ExpectedFrom to empty, the
// apply ALSO finalizes the delete atomically — removing the
// pendingDeletes entry, removing the manifest entry, and writing the
// tombstone. This is the same FSM-local mutation that
// applyFinalizeDelete performs; folding it in here closes the
// leader-only "natural finalize" leak where leadership transfer
// between the last ack apply and the leader's onAckDelete callback
// dropped the CmdFinalizeDelete proposal. CmdFinalizeDelete stays in
// the protocol for explicit external triggers (operator-initiated
// cleanup) but the receipt protocol's natural completion no longer
// depends on a leader-only post-apply callback. See gastrolog-3qr8z.
func (f *FSM) applyAckDelete(c *gastrologv1.AckDeleteCommand) (*chunk.ChunkID, string, bool, error) {
	id := chunkIDFromProto(c.GetId())
	nodeID := c.GetNodeId()
	p, ok := f.pendingDeletes[id]
	if !ok {
		return nil, "", false, nil
	}
	if !p.ExpectedFrom[nodeID] {
		return nil, "", false, nil
	}
	delete(p.ExpectedFrom, nodeID)

	if len(p.ExpectedFrom) > 0 {
		return &id, nodeID, false, nil
	}
	// Natural finalize: every node has acked. Atomically tombstone the
	// chunk, remove the pendingDeletes entry, and remove the manifest
	// entry. Matches applyFinalizeDelete's mutation exactly.
	f.tombstones[id] = time.Now()
	delete(f.pendingDeletes, id)
	delete(f.chunks, id)
	return &id, nodeID, true, nil
}

// applyFinalizeDelete removes the entry from pendingDeletes, removes the
// chunk metadata from the manifest (f.chunks), and records a tombstone —
// matching the legacy CmdDeleteChunk apply contract. The leader proposes
// this once expectedFrom is empty for an entry. Without the
// chunks/tombstone updates the manifest carries dead entries forever
// and stale ImportSealed RPCs could re-create the chunk after the
// cluster believed it deleted. Idempotent: re-applying for an entry
// already finalized is a no-op (tombstone update aside).
func (f *FSM) applyFinalizeDelete(c *gastrologv1.FinalizeDeleteCommand) (*chunk.ChunkID, error) {
	id := chunkIDFromProto(c.GetId())

	// Tombstone unconditionally so a stale ImportSealed/Append/Seal
	// command racing the receipt protocol can be rejected even if the
	// chunk metadata never lived in this FSM. Same rationale as
	// applyDelete (CmdDeleteChunk) under gastrolog-11rzz.
	f.tombstones[id] = time.Now()

	_, hadPending := f.pendingDeletes[id]
	if hadPending {
		delete(f.pendingDeletes, id)
	}
	_, hadEntry := f.chunks[id]
	if hadEntry {
		delete(f.chunks, id)
	}

	// Fire the onFinalizeDelete callback whenever we actually changed
	// state (either a pending entry was removed OR the manifest entry
	// was removed). Callers use the callback for audit logging and
	// post-delete bookkeeping; an idempotent no-op should not fire it.
	if !hadPending && !hadEntry {
		return nil, nil
	}
	return &id, nil
}

// applyPruneNode removes nodeID from every pendingDeletes entry's
// ExpectedFrom set. Returns the prunedNodeID and the slice of chunkIDs
// whose ExpectedFrom became empty as a result of the prune — those
// chunks are atomically finalized in the same apply (gastrolog-15fm8).
//
// Idempotent: pruning a node that no entry expected from is a no-op.
// Pruning twice yields the same final state (the second pass finds
// nothing to remove and returns an empty finalizable list).
//
// gastrolog-15fm8: the pre-fix shape returned `finalizable` and
// relied on the leader's onPruneNode callback to propose
// CmdFinalizeDelete for each chunk in a goroutine. That goroutine
// dropped on leadership transfer between the prune apply and the
// callback firing, leaving pendingDeletes entries with empty
// ExpectedFrom stranded forever. The fix folds the finalize INTO this
// apply: chunks with drained ExpectedFrom are tombstoned, removed from
// pendingDeletes, and removed from f.chunks atomically before this
// function returns. The `finalizable` slice is still returned for the
// onPruneNode callback's audit / observability use; subscribers
// expecting onFinalizeDelete for each chunk receive it via the
// per-chunk firing in fsm.go's applyLocked dispatch.
func (f *FSM) applyPruneNode(c *gastrologv1.PruneNodeCommand) (string, []chunk.ChunkID, error) {
	nodeID := c.GetNodeId()
	if nodeID == "" {
		return "", nil, errors.New("prune node: empty node ID")
	}

	var finalizable []chunk.ChunkID
	for chunkID, p := range f.pendingDeletes {
		if !p.ExpectedFrom[nodeID] {
			continue
		}
		delete(p.ExpectedFrom, nodeID)
		if len(p.ExpectedFrom) == 0 {
			finalizable = append(finalizable, chunkID)
		}
	}
	now := time.Now()
	for _, chunkID := range finalizable {
		f.tombstones[chunkID] = now
		delete(f.pendingDeletes, chunkID)
		delete(f.chunks, chunkID)
	}
	return nodeID, finalizable, nil
}

// ---------- Command builders (used by callers before Raft.Apply) ----------

// NewRequestDelete builds a CmdRequestDelete command message.
// expectedFrom is the set of node IDs expected to ack; reason is a
// short free-form string identifying why the chunk is being deleted
// (e.g. "retention-ttl", "transition-source-expire", "manual-delete-rpc").
func NewRequestDelete(id chunk.ChunkID, proposedAt time.Time, reason string, expectedFrom []string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_RequestDelete{RequestDelete: &gastrologv1.RequestDeleteCommand{
		Id:              id[:],
		ProposedAtNanos: proposedAt.UnixNano(),
		Reason:          reason,
		ExpectedFrom:    expectedFrom,
	}}}
}

// MarshalRequestDelete builds the Raft log data for CmdRequestDelete.
func MarshalRequestDelete(id chunk.ChunkID, proposedAt time.Time, reason string, expectedFrom []string) []byte {
	return mustMarshalCommand(NewRequestDelete(id, proposedAt, reason, expectedFrom))
}

// NewAckDelete builds a CmdAckDelete command message.
func NewAckDelete(id chunk.ChunkID, nodeID string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_AckDelete{AckDelete: &gastrologv1.AckDeleteCommand{Id: id[:], NodeId: nodeID}}}
}

// MarshalAckDelete builds the Raft log data for CmdAckDelete.
func MarshalAckDelete(id chunk.ChunkID, nodeID string) []byte {
	return mustMarshalCommand(NewAckDelete(id, nodeID))
}

// NewFinalizeDelete builds a CmdFinalizeDelete command message.
func NewFinalizeDelete(id chunk.ChunkID) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_FinalizeDelete{FinalizeDelete: &gastrologv1.FinalizeDeleteCommand{Id: id[:]}}}
}

// MarshalFinalizeDelete builds the Raft log data for CmdFinalizeDelete.
func MarshalFinalizeDelete(id chunk.ChunkID) []byte {
	return mustMarshalCommand(NewFinalizeDelete(id))
}

// NewPruneNode builds a CmdPruneNode command message. The leader
// proposes this after a node is removed from the vault-ctl Raft group
// (decommissioned or rebalanced away) so its outstanding ack obligations
// don't pin pendingDeletes entries forever. See gastrolog-51gme step 10.
func NewPruneNode(nodeID string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{Command: &gastrologv1.VaultCtlCommand_PruneNode{PruneNode: &gastrologv1.PruneNodeCommand{NodeId: nodeID}}}
}

// MarshalPruneNode builds the Raft log data for CmdPruneNode.
func MarshalPruneNode(nodeID string) []byte {
	return mustMarshalCommand(NewPruneNode(nodeID))
}

// ---------- Snapshot encode / decode ----------
//
// Section format (sectionPendingDeletes = 4):
//
//	4 bytes  count of entries (BE uint32)
//	repeated per entry:
//	  16 bytes  chunk ID
//	   8 bytes  proposedAt nanos (BE int64)
//	   2 bytes  reason length (BE uint16)
//	   N bytes  reason
//	   4 bytes  expectedFrom count (BE uint32)
//	   repeated:
//	      2 bytes  node ID length (BE uint16)
//	      M bytes  node ID

func encodePendingDeletesSection(w io.Writer, entries []PendingDelete) error {
	if len(entries) == 0 {
		return nil // omit section entirely when empty
	}
	// Compute payload length up front so writeSectionHeader sees the
	// correct figure (defensive: snapshot writer expects fixed-length
	// sections so a partial truncation can be detected).
	payloadLen := 4
	for i := range entries {
		payloadLen += 16 + 8 + 2 + len(entries[i].Reason) + 4
		for n := range entries[i].ExpectedFrom {
			payloadLen += 2 + len(n)
		}
	}
	if payloadLen > 0xFFFFFFFF {
		return errors.New("pendingDeletes section exceeds 4 GiB; corruption suspected")
	}
	if err := writeSectionHeader(w, sectionPendingDeletes, uint32(payloadLen)); err != nil {
		return err
	}
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(entries))) //nolint:gosec // G115: count fits uint32
	if _, err := w.Write(countBuf[:]); err != nil {
		return fmt.Errorf("write pending-deletes count: %w", err)
	}
	for i := range entries {
		if err := encodePendingDeleteEntry(w, &entries[i]); err != nil {
			return err
		}
	}
	return nil
}

func encodePendingDeleteEntry(w io.Writer, p *PendingDelete) error {
	var hdr [16 + 8 + 2]byte
	copy(hdr[0:16], p.ChunkID[:])
	binary.BigEndian.PutUint64(hdr[16:24], uint64(p.ProposedAt.UnixNano()))
	binary.BigEndian.PutUint16(hdr[24:26], uint16(len(p.Reason))) //nolint:gosec // G115: reason length bounded
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write pending-delete header: %w", err)
	}
	if _, err := w.Write([]byte(p.Reason)); err != nil {
		return fmt.Errorf("write pending-delete reason: %w", err)
	}
	var efc [4]byte
	binary.BigEndian.PutUint32(efc[:], uint32(len(p.ExpectedFrom))) //nolint:gosec // G115: cluster size fits uint32
	if _, err := w.Write(efc[:]); err != nil {
		return fmt.Errorf("write pending-delete expected-from count: %w", err)
	}
	for n := range p.ExpectedFrom {
		var nl [2]byte
		binary.BigEndian.PutUint16(nl[:], uint16(len(n))) //nolint:gosec // G115: node ID strings are <64KB
		if _, err := w.Write(nl[:]); err != nil {
			return fmt.Errorf("write pending-delete node id length: %w", err)
		}
		if _, err := w.Write([]byte(n)); err != nil {
			return fmt.Errorf("write pending-delete node id: %w", err)
		}
	}
	return nil
}

func readPendingDeletesSection(r io.Reader) (map[chunk.ChunkID]*PendingDelete, error) {
	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf("read pending-deletes count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBuf[:])
	out := make(map[chunk.ChunkID]*PendingDelete, count)
	for i := range count {
		var hdr [16 + 8 + 2]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, fmt.Errorf("read pending-delete %d header: %w", i, err)
		}
		var p PendingDelete
		copy(p.ChunkID[:], hdr[0:16])
		p.ProposedAt = time.Unix(0, int64(binary.BigEndian.Uint64(hdr[16:24]))) //nolint:gosec // G115: round-trip
		reasonLen := int(binary.BigEndian.Uint16(hdr[24:26]))
		if reasonLen > 0 {
			reasonBuf := make([]byte, reasonLen)
			if _, err := io.ReadFull(r, reasonBuf); err != nil {
				return nil, fmt.Errorf("read pending-delete %d reason: %w", i, err)
			}
			p.Reason = string(reasonBuf)
		}
		var efcBuf [4]byte
		if _, err := io.ReadFull(r, efcBuf[:]); err != nil {
			return nil, fmt.Errorf("read pending-delete %d expected-from count: %w", i, err)
		}
		efc := binary.BigEndian.Uint32(efcBuf[:])
		p.ExpectedFrom = make(map[string]bool, efc)
		for j := range efc {
			var nlBuf [2]byte
			if _, err := io.ReadFull(r, nlBuf[:]); err != nil {
				return nil, fmt.Errorf("read pending-delete %d node %d length: %w", i, j, err)
			}
			nl := int(binary.BigEndian.Uint16(nlBuf[:]))
			nbuf := make([]byte, nl)
			if _, err := io.ReadFull(r, nbuf); err != nil {
				return nil, fmt.Errorf("read pending-delete %d node %d body: %w", i, j, err)
			}
			p.ExpectedFrom[string(nbuf)] = true
		}
		pp := p
		out[p.ChunkID] = &pp
	}
	return out, nil
}
