package raftfsm

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
//   1. Tier leader proposes CmdRequestDelete(chunkID, expectedFrom, reason).
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

// ---------- Apply functions (caller MUST hold f.mu) ----------

// CmdRequestDelete payload:
//
//	16 bytes  chunk ID
//	 8 bytes  proposedAt nanos (BE int64)
//	 2 bytes  reason length (BE uint16)
//	 N bytes  reason string
//	 4 bytes  expectedFrom count (BE uint32)
//	repeated:
//	   2 bytes  node ID length (BE uint16)
//	   M bytes  node ID string
func (f *FSM) applyRequestDelete(data []byte) (*PendingDelete, error) {
	entry, err := decodeRequestDelete(data)
	if err != nil {
		return nil, err
	}

	// Idempotency: if this chunk already has a pending entry, treat
	// the second request as a no-op (and don't fire the callback).
	// Re-proposing CmdRequestDelete would otherwise reset the
	// expectedFrom set and erase any acks already in-flight.
	if _, exists := f.pendingDeletes[entry.ChunkID]; exists {
		return nil, nil
	}

	p := &PendingDelete{
		ChunkID:      entry.ChunkID,
		Reason:       entry.Reason,
		ProposedAt:   entry.ProposedAt,
		ExpectedFrom: entry.ExpectedFrom,
	}
	f.pendingDeletes[entry.ChunkID] = p

	cp := p.Copy()
	return &cp, nil
}

// CmdAckDelete payload:
//
//	16 bytes  chunk ID
//	 2 bytes  node ID length (BE uint16)
//	 N bytes  node ID string
//
// Returns the chunk ID and the acking node ID for the post-apply
// callback. If the entry is gone (already finalized) or the node was
// never expected, the apply succeeds but returns nil — Raft has the
// entry, the FSM is consistent, and the callback is suppressed.
func (f *FSM) applyAckDelete(data []byte) (*chunk.ChunkID, string, error) {
	id, nodeID, err := decodeAckDelete(data)
	if err != nil {
		return nil, "", err
	}
	p, ok := f.pendingDeletes[id]
	if !ok {
		return nil, "", nil
	}
	if !p.ExpectedFrom[nodeID] {
		return nil, "", nil
	}
	delete(p.ExpectedFrom, nodeID)
	return &id, nodeID, nil
}

// CmdFinalizeDelete payload:
//
//	16 bytes  chunk ID
//
// The leader proposes this once expectedFrom is empty for an entry.
// Apply removes the entry from pendingDeletes. Idempotent: if the
// entry is already gone, the apply is a no-op.
func (f *FSM) applyFinalizeDelete(data []byte) (*chunk.ChunkID, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("finalize delete: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	if _, ok := f.pendingDeletes[id]; !ok {
		return nil, nil
	}
	delete(f.pendingDeletes, id)
	return &id, nil
}

// applyPruneNode removes nodeID from every pendingDeletes entry's
// ExpectedFrom set. Returns the prunedNodeID and the slice of chunkIDs
// whose ExpectedFrom became empty as a result of the prune (i.e.,
// chunks that are now ready for the leader to propose CmdFinalizeDelete).
//
// Wire format: [1 byte cmd][2 bytes nodeID-len][nodeID-bytes].
//
// Idempotent: pruning a node that no entry expected from is a no-op.
// Pruning twice yields the same final state (the second pass finds
// nothing to remove and returns an empty finalizable list).
func (f *FSM) applyPruneNode(data []byte) (string, []chunk.ChunkID, error) {
	if len(data) < 2 {
		return "", nil, fmt.Errorf("prune node: payload too short (%d bytes)", len(data))
	}
	nodeLen := int(binary.BigEndian.Uint16(data[0:2]))
	if len(data) < 2+nodeLen {
		return "", nil, fmt.Errorf("prune node: truncated node ID (%d bytes for %d)", len(data)-2, nodeLen)
	}
	nodeID := string(data[2 : 2+nodeLen])
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
	return nodeID, finalizable, nil
}

// ---------- Command builders (used by callers before Raft.Apply) ----------

// MarshalRequestDelete builds the Raft log data for CmdRequestDelete.
// expectedFrom is the set of node IDs expected to ack; reason is a
// short free-form string identifying why the chunk is being deleted
// (e.g. "retention-ttl", "transition-source-expire", "manual-delete-rpc").
func MarshalRequestDelete(id chunk.ChunkID, proposedAt time.Time, reason string, expectedFrom []string) []byte {
	if len(reason) > 0xFFFF {
		reason = reason[:0xFFFF] // defensive truncate; reasons are short
	}
	size := 1 + 16 + 8 + 2 + len(reason) + 4
	for _, n := range expectedFrom {
		size += 2 + len(n)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, byte(CmdRequestDelete))
	buf = append(buf, id[:]...)
	var nanos [8]byte
	binary.BigEndian.PutUint64(nanos[:], uint64(proposedAt.UnixNano()))
	buf = append(buf, nanos[:]...)
	var rl [2]byte
	binary.BigEndian.PutUint16(rl[:], uint16(len(reason))) //nolint:gosec // G115: bounded above
	buf = append(buf, rl[:]...)
	buf = append(buf, reason...)
	var efc [4]byte
	binary.BigEndian.PutUint32(efc[:], uint32(len(expectedFrom))) //nolint:gosec // G115: cluster size fits uint32
	buf = append(buf, efc[:]...)
	for _, n := range expectedFrom {
		var nl [2]byte
		binary.BigEndian.PutUint16(nl[:], uint16(len(n))) //nolint:gosec // G115: node ID strings are <64KB
		buf = append(buf, nl[:]...)
		buf = append(buf, n...)
	}
	return buf
}

// MarshalAckDelete builds the Raft log data for CmdAckDelete.
func MarshalAckDelete(id chunk.ChunkID, nodeID string) []byte {
	buf := make([]byte, 0, 1+16+2+len(nodeID))
	buf = append(buf, byte(CmdAckDelete))
	buf = append(buf, id[:]...)
	var nl [2]byte
	binary.BigEndian.PutUint16(nl[:], uint16(len(nodeID))) //nolint:gosec // G115: node ID strings are <64KB
	buf = append(buf, nl[:]...)
	buf = append(buf, nodeID...)
	return buf
}

// MarshalFinalizeDelete builds the Raft log data for CmdFinalizeDelete.
func MarshalFinalizeDelete(id chunk.ChunkID) []byte {
	buf := make([]byte, 1+16)
	buf[0] = byte(CmdFinalizeDelete)
	copy(buf[1:17], id[:])
	return buf
}

// MarshalPruneNode builds the Raft log data for CmdPruneNode. Wire
// format: [1 byte cmd][2 bytes nodeID-len][nodeID-bytes]. The leader
// proposes this after a node is removed from the vault-ctl Raft group
// (decommissioned or rebalanced away) so its outstanding ack obligations
// don't pin pendingDeletes entries forever. See gastrolog-51gme step 10.
func MarshalPruneNode(nodeID string) []byte {
	if len(nodeID) > 0xFFFF {
		nodeID = nodeID[:0xFFFF]
	}
	buf := make([]byte, 0, 1+2+len(nodeID))
	buf = append(buf, byte(CmdPruneNode))
	var nl [2]byte
	binary.BigEndian.PutUint16(nl[:], uint16(len(nodeID))) //nolint:gosec // G115: node ID strings are <64KB
	buf = append(buf, nl[:]...)
	buf = append(buf, nodeID...)
	return buf
}

// ---------- Wire decoders (shared between Apply and snapshot Restore) ----------

func decodeRequestDelete(data []byte) (PendingDelete, error) {
	if len(data) < 16+8+2+4 {
		return PendingDelete{}, fmt.Errorf("request delete: payload too short (%d bytes)", len(data))
	}
	var entry PendingDelete
	copy(entry.ChunkID[:], data[:16])
	entry.ProposedAt = time.Unix(0, int64(binary.BigEndian.Uint64(data[16:24]))) //nolint:gosec // G115: nano timestamp round-trip

	off := 24
	reasonLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	if len(data) < off+reasonLen+4 {
		return PendingDelete{}, errors.New("request delete: payload truncated in reason")
	}
	entry.Reason = string(data[off : off+reasonLen])
	off += reasonLen

	expectedCount := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	entry.ExpectedFrom = make(map[string]bool, expectedCount)
	for i := range expectedCount {
		if len(data) < off+2 {
			return PendingDelete{}, fmt.Errorf("request delete: payload truncated reading node id %d length", i)
		}
		nl := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if len(data) < off+nl {
			return PendingDelete{}, fmt.Errorf("request delete: payload truncated reading node id %d body", i)
		}
		entry.ExpectedFrom[string(data[off:off+nl])] = true
		off += nl
	}
	return entry, nil
}

func decodeAckDelete(data []byte) (chunk.ChunkID, string, error) {
	if len(data) < 16+2 {
		return chunk.ChunkID{}, "", fmt.Errorf("ack delete: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	nl := int(binary.BigEndian.Uint16(data[16:18]))
	if len(data) < 18+nl {
		return chunk.ChunkID{}, "", errors.New("ack delete: payload truncated in node id")
	}
	return id, string(data[18 : 18+nl]), nil
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
	binary.BigEndian.PutUint16(hdr[24:26], uint16(len(p.Reason)))           //nolint:gosec // G115: reason length bounded
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
