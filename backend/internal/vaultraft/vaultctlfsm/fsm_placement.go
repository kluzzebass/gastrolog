// Per-chunk Receiving/Holding placement state for the fan-out data-plane
// epic (gastrolog-2ujjh / gastrolog-4cxw0).
//
// Placement lives parallel to ManifestEntry, keyed by ChunkID — see
// docs/fan-out-data-plane-design.md § "Storage layout: FSM schema
// additions". The split exists so the existing 123-byte fixed
// ManifestEntry encoding stays unchanged and old nodes (or future
// minimal-build nodes) restoring the snapshot can skip the placement
// section entirely (forward-compat).
//
// Invariants enforced by the apply* functions:
//   - Holding ⊇ Receiving (CmdAddReceiving adds to both atomically; only
//     the standalone CmdAddHolding adds to Holding without Receiving).
//   - PendingPulls keys are nodes that have been BeginHoldingRemoval'd
//     but not yet drained of acks.
//   - FinalSetHash is set once at CmdSealChunk time (or stays zero).
//
// Idempotency: every apply* is a no-op when the requested state is
// already satisfied (re-adding an existing Receiving member, acking a
// pull that has no PendingPulls entry, etc.). Re-applies during log
// replay yield the same final state.

package vaultctlfsm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"gastrolog/internal/chunk"
)

// ChunkPlacement carries the per-chunk membership state — the
// Receiving / Holding sets and the in-flight Holding-removal
// receipts. Stored in its own per-vault map alongside chunks and
// pendingDeletes; serialized in its own snapshot section so the
// existing ManifestEntry encoding stays unchanged.
type ChunkPlacement struct {
	// Receiving is the set of nodes that should receive new fan-out
	// writes for this chunk's active window. Empty after Sealing.
	Receiving []string
	// Holding is the set of nodes that hold (or have held, and are
	// draining) the chunk's records. Invariant: Holding ⊇ Receiving.
	Holding []string
	// PendingPulls tracks in-flight Holding-removal: when a fromNode
	// is being removed from Holding, every remaining Receiving member
	// must ack having pulled fromNode's records. Key = fromNode; value
	// = set of toNodes still owing a CmdAckPull. When the set drains,
	// fromNode is removed from Holding. Mirrors PendingDelete's
	// ExpectedFrom pattern, inverted: the keyed node is leaving rather
	// than already gone.
	PendingPulls map[string]map[string]bool
	// FinalSetHash is the converged set-hash at seal time, populated
	// by CmdSealChunk's optional FinalSet argument (gastrolog-37k2b).
	// Zero value means unset (chunks pre-seal). Receiving members
	// compare their local set-hash against this value as
	// defense-in-depth divergence detection.
	FinalSetHash [32]byte
}

// HasReceiving reports whether nodeID is in Receiving.
func (p *ChunkPlacement) HasReceiving(nodeID string) bool {
	return slices.Contains(p.Receiving, nodeID)
}

// HasHolding reports whether nodeID is in Holding.
func (p *ChunkPlacement) HasHolding(nodeID string) bool {
	return slices.Contains(p.Holding, nodeID)
}

// Copy returns a deep copy safe to hand outside the FSM lock.
func (p *ChunkPlacement) Copy() ChunkPlacement {
	out := ChunkPlacement{
		Receiving:    append([]string(nil), p.Receiving...),
		Holding:      append([]string(nil), p.Holding...),
		FinalSetHash: p.FinalSetHash,
	}
	if len(p.PendingPulls) > 0 {
		out.PendingPulls = make(map[string]map[string]bool, len(p.PendingPulls))
		for from, set := range p.PendingPulls {
			cp := make(map[string]bool, len(set))
			maps.Copy(cp, set)
			out.PendingPulls[from] = cp
		}
	}
	return out
}

// ---------- Reads (local, no Raft) ----------

// Placement returns a copy of the per-chunk placement state, or nil if
// no placement entry exists (single-node / memory-mode chunks, or
// chunks that have never had a placement command applied).
func (f *FSM) Placement(id chunk.ChunkID) *ChunkPlacement {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p, ok := f.placements[id]
	if !ok {
		return nil
	}
	cp := p.Copy()
	return &cp
}

// ReceivingFor returns a copy of the Receiving slice for chunkID, or
// nil if no placement exists. Caller may mutate the returned slice.
// Hot-path lookup used by the orchestrator's writeLoop when fanning
// out a record to vault-active-chunk receivers.
func (f *FSM) ReceivingFor(id chunk.ChunkID) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p, ok := f.placements[id]
	if !ok {
		return nil
	}
	out := make([]string, len(p.Receiving))
	copy(out, p.Receiving)
	return out
}

// HoldingFor returns a copy of the Holding slice for chunkID, or nil
// if no placement exists. Authoritative cluster-wide residency for
// chunks that have crossed the fan-out path (the placement-derived
// ChunkResidency closure is the fallback for chunks without
// placement state, e.g. single-node and memory-mode vaults).
func (f *FSM) HoldingFor(id chunk.ChunkID) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	p, ok := f.placements[id]
	if !ok {
		return nil
	}
	out := make([]string, len(p.Holding))
	copy(out, p.Holding)
	return out
}

// ---------- Apply functions (caller MUST hold f.mu) ----------

// applyAddReceiving adds nodeID to both Receiving and Holding atomically.
// This is the placement-edit hot path: a node entering Receiving
// implicitly enters Holding too (Holding ⊇ Receiving invariant).
// The multi-set mutation mirrors applyAckDelete's pattern of mutating
// multiple maps in one apply.
//
// Idempotent: if nodeID is already in both sets, the apply is a no-op
// (and the callback fires with the existing state).
//
// CmdAddReceiving payload:
//
//	16 bytes  chunk ID
//	 2 bytes  node ID length (BE uint16)
//	 N bytes  node ID string
func (f *FSM) applyAddReceiving(data []byte) (*chunk.ChunkID, string, error) {
	id, nodeID, err := decodeChunkNode(data, "add receiving")
	if err != nil {
		return nil, "", err
	}
	p := f.placements[id]
	if p == nil {
		p = &ChunkPlacement{}
		f.placements[id] = p
	}
	if !slices.Contains(p.Receiving, nodeID) {
		p.Receiving = append(p.Receiving, nodeID)
	}
	if !slices.Contains(p.Holding, nodeID) {
		p.Holding = append(p.Holding, nodeID)
	}
	return &id, nodeID, nil
}

// applyRemoveReceiving removes nodeID from Receiving (only). The node
// stays in Holding until a separate CmdBeginHoldingRemoval + drained
// PendingPulls finishes draining.
//
// Idempotent: removing a node that was never in Receiving is a no-op.
//
// CmdRemoveReceiving payload: same as CmdAddReceiving.
func (f *FSM) applyRemoveReceiving(data []byte) (*chunk.ChunkID, string, error) {
	id, nodeID, err := decodeChunkNode(data, "remove receiving")
	if err != nil {
		return nil, "", err
	}
	p := f.placements[id]
	if p == nil {
		return &id, nodeID, nil
	}
	p.Receiving = slices.DeleteFunc(p.Receiving, func(s string) bool { return s == nodeID })
	return &id, nodeID, nil
}

// applyAddHolding adds nodeID to Holding without touching Receiving.
// Used by reconcile catch-up completion: a node that has acquired the
// chunk's records (via orphan repatriation, snapshot restore, etc.)
// without first joining Receiving registers its Holding membership
// here. Per-chunk (one apply per chunk) — not per-record.
//
// Idempotent: re-adding is a no-op.
//
// CmdAddHolding payload: same as CmdAddReceiving.
func (f *FSM) applyAddHolding(data []byte) (*chunk.ChunkID, string, error) {
	id, nodeID, err := decodeChunkNode(data, "add holding")
	if err != nil {
		return nil, "", err
	}
	p := f.placements[id]
	if p == nil {
		p = &ChunkPlacement{}
		f.placements[id] = p
	}
	if !slices.Contains(p.Holding, nodeID) {
		p.Holding = append(p.Holding, nodeID)
	}
	return &id, nodeID, nil
}

// applyBeginHoldingRemoval starts the Holding-removal receipt protocol
// for fromNode. expectedFrom is the set of toNodes that must ack having
// pulled fromNode's records before fromNode can leave Holding. Mirrors
// applyRequestDelete's shape, inverted.
//
// Idempotent: re-applying for the same fromNode is a no-op (does not
// reset the expectedFrom set, preserving any acks already in flight).
//
// CmdBeginHoldingRemoval payload:
//
//	16 bytes  chunk ID
//	 2 bytes  fromNode length (BE uint16)
//	 N bytes  fromNode string
//	 4 bytes  expectedFrom count (BE uint32)
//	repeated:
//	   2 bytes  toNode length (BE uint16)
//	   M bytes  toNode string
func (f *FSM) applyBeginHoldingRemoval(data []byte) (*chunk.ChunkID, string, error) {
	if len(data) < 16+2 {
		return nil, "", fmt.Errorf("begin holding removal: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	off := 16

	fromLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	if len(data) < off+fromLen+4 {
		return nil, "", errors.New("begin holding removal: truncated fromNode")
	}
	fromNode := string(data[off : off+fromLen])
	off += fromLen

	expectedCount := int(binary.BigEndian.Uint32(data[off : off+4]))
	off += 4
	expected := make(map[string]bool, expectedCount)
	for i := range expectedCount {
		if len(data) < off+2 {
			return nil, "", fmt.Errorf("begin holding removal: truncated reading toNode %d length", i)
		}
		nl := int(binary.BigEndian.Uint16(data[off : off+2]))
		off += 2
		if len(data) < off+nl {
			return nil, "", fmt.Errorf("begin holding removal: truncated reading toNode %d body", i)
		}
		expected[string(data[off:off+nl])] = true
		off += nl
	}

	p := f.placements[id]
	if p == nil {
		p = &ChunkPlacement{}
		f.placements[id] = p
	}
	if p.PendingPulls == nil {
		p.PendingPulls = make(map[string]map[string]bool)
	}
	// Idempotency: an existing entry stays — don't reset acks in flight.
	if _, exists := p.PendingPulls[fromNode]; exists {
		return &id, fromNode, nil
	}
	p.PendingPulls[fromNode] = expected
	return &id, fromNode, nil
}

// applyAckPull records toNode's acknowledgement that it has pulled
// fromNode's records for this chunk. When the PendingPulls[fromNode]
// set drains to empty, fromNode is atomically removed from Holding and
// the entry is removed from PendingPulls (matching applyAckDelete's
// "natural finalize" pattern; no separate CmdFinalizeHoldingRemoval).
//
// Returns (chunkID, fromNode, toNode, finalized) for the callback.
// Idempotent: acking when no PendingPulls entry exists or the toNode
// was never expected is a successful no-op.
//
// CmdAckPull payload:
//
//	16 bytes  chunk ID
//	 2 bytes  fromNode length (BE uint16)
//	 N bytes  fromNode string
//	 2 bytes  toNode length (BE uint16)
//	 M bytes  toNode string
func (f *FSM) applyAckPull(data []byte) (*chunk.ChunkID, string, string, bool, error) {
	if len(data) < 16+2 {
		return nil, "", "", false, fmt.Errorf("ack pull: payload too short (%d bytes)", len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	off := 16

	fromLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	if len(data) < off+fromLen+2 {
		return nil, "", "", false, errors.New("ack pull: truncated fromNode")
	}
	fromNode := string(data[off : off+fromLen])
	off += fromLen

	toLen := int(binary.BigEndian.Uint16(data[off : off+2]))
	off += 2
	if len(data) < off+toLen {
		return nil, "", "", false, errors.New("ack pull: truncated toNode")
	}
	toNode := string(data[off : off+toLen])

	p := f.placements[id]
	if p == nil || p.PendingPulls == nil {
		return &id, fromNode, toNode, false, nil
	}
	set, ok := p.PendingPulls[fromNode]
	if !ok {
		return &id, fromNode, toNode, false, nil
	}
	if !set[toNode] {
		return &id, fromNode, toNode, false, nil
	}
	delete(set, toNode)
	if len(set) > 0 {
		return &id, fromNode, toNode, false, nil
	}
	// Drained: atomically finalize the Holding removal.
	delete(p.PendingPulls, fromNode)
	if len(p.PendingPulls) == 0 {
		p.PendingPulls = nil
	}
	p.Holding = slices.DeleteFunc(p.Holding, func(s string) bool { return s == fromNode })
	return &id, fromNode, toNode, true, nil
}

// pruneNodeFromPlacements removes nodeID from every chunk's Receiving,
// Holding, and PendingPulls (both as a fromNode key and as a toNode
// inside other entries' sets). Called from applyPruneNode so a node
// departing the vault-ctl Raft group doesn't pin holding-removal
// receipts forever. Caller MUST hold f.mu.
//
// Returns the chunks whose PendingPulls drained as a result, paralleling
// applyPruneNode's finalizable slice. Caller atomically finalizes those
// by removing the drained fromNode from Holding (already done here) and
// dropping the PendingPulls entry.
func (f *FSM) pruneNodeFromPlacements(nodeID string) {
	for _, p := range f.placements {
		p.Receiving = slices.DeleteFunc(p.Receiving, func(s string) bool { return s == nodeID })
		p.Holding = slices.DeleteFunc(p.Holding, func(s string) bool { return s == nodeID })
		// Drop the node as a fromNode key (it's leaving the cluster;
		// its Holding-removal receipt is moot — its records are gone
		// with it).
		delete(p.PendingPulls, nodeID)
		// Drop the node as a toNode in remaining entries' expected
		// sets. If draining finalizes any fromNode, finalize it.
		for fromNode, set := range p.PendingPulls {
			if !set[nodeID] {
				continue
			}
			delete(set, nodeID)
			if len(set) == 0 {
				delete(p.PendingPulls, fromNode)
				p.Holding = slices.DeleteFunc(p.Holding, func(s string) bool { return s == fromNode })
			}
		}
		if len(p.PendingPulls) == 0 {
			p.PendingPulls = nil
		}
	}
}

// ---------- Command builders ----------

// MarshalAddReceiving builds the Raft log data for CmdAddReceiving.
func MarshalAddReceiving(id chunk.ChunkID, nodeID string) []byte {
	return marshalChunkNode(byte(CmdAddReceiving), id, nodeID)
}

// MarshalRemoveReceiving builds the Raft log data for CmdRemoveReceiving.
func MarshalRemoveReceiving(id chunk.ChunkID, nodeID string) []byte {
	return marshalChunkNode(byte(CmdRemoveReceiving), id, nodeID)
}

// MarshalAddHolding builds the Raft log data for CmdAddHolding.
func MarshalAddHolding(id chunk.ChunkID, nodeID string) []byte {
	return marshalChunkNode(byte(CmdAddHolding), id, nodeID)
}

// MarshalBeginHoldingRemoval builds the Raft log data for
// CmdBeginHoldingRemoval. expectedFrom is the set of toNodes that owe
// a pull-ack before fromNode is removed from Holding.
func MarshalBeginHoldingRemoval(id chunk.ChunkID, fromNode string, expectedFrom []string) []byte {
	if len(fromNode) > 0xFFFF {
		fromNode = fromNode[:0xFFFF]
	}
	size := 1 + 16 + 2 + len(fromNode) + 4
	for _, n := range expectedFrom {
		size += 2 + len(n)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, byte(CmdBeginHoldingRemoval))
	buf = append(buf, id[:]...)
	var fl [2]byte
	binary.BigEndian.PutUint16(fl[:], uint16(len(fromNode))) //nolint:gosec // G115: bounded above
	buf = append(buf, fl[:]...)
	buf = append(buf, fromNode...)
	var efc [4]byte
	binary.BigEndian.PutUint32(efc[:], uint32(len(expectedFrom))) //nolint:gosec // G115: cluster size fits uint32
	buf = append(buf, efc[:]...)
	for _, n := range expectedFrom {
		var nl [2]byte
		binary.BigEndian.PutUint16(nl[:], uint16(len(n))) //nolint:gosec // G115: node ID strings <64KB
		buf = append(buf, nl[:]...)
		buf = append(buf, n...)
	}
	return buf
}

// MarshalAckPull builds the Raft log data for CmdAckPull.
func MarshalAckPull(id chunk.ChunkID, fromNode, toNode string) []byte {
	if len(fromNode) > 0xFFFF {
		fromNode = fromNode[:0xFFFF]
	}
	if len(toNode) > 0xFFFF {
		toNode = toNode[:0xFFFF]
	}
	buf := make([]byte, 0, 1+16+2+len(fromNode)+2+len(toNode))
	buf = append(buf, byte(CmdAckPull))
	buf = append(buf, id[:]...)
	var fl [2]byte
	binary.BigEndian.PutUint16(fl[:], uint16(len(fromNode))) //nolint:gosec // G115: bounded above
	buf = append(buf, fl[:]...)
	buf = append(buf, fromNode...)
	var tl [2]byte
	binary.BigEndian.PutUint16(tl[:], uint16(len(toNode))) //nolint:gosec // G115: bounded above
	buf = append(buf, tl[:]...)
	buf = append(buf, toNode...)
	return buf
}

// marshalChunkNode is the shared marshaler for commands whose payload
// is [16 bytes chunkID][2 bytes nodeID length][nodeID bytes].
func marshalChunkNode(opcode byte, id chunk.ChunkID, nodeID string) []byte {
	if len(nodeID) > 0xFFFF {
		nodeID = nodeID[:0xFFFF]
	}
	buf := make([]byte, 0, 1+16+2+len(nodeID))
	buf = append(buf, opcode)
	buf = append(buf, id[:]...)
	var nl [2]byte
	binary.BigEndian.PutUint16(nl[:], uint16(len(nodeID))) //nolint:gosec // G115: node ID strings <64KB
	buf = append(buf, nl[:]...)
	buf = append(buf, nodeID...)
	return buf
}

// decodeChunkNode is the shared decoder for the [16 chunkID][2 nlen][N]
// payload shape used by CmdAddReceiving / CmdRemoveReceiving /
// CmdAddHolding.
func decodeChunkNode(data []byte, opName string) (chunk.ChunkID, string, error) {
	if len(data) < 16+2 {
		return chunk.ChunkID{}, "", fmt.Errorf("%s: payload too short (%d bytes)", opName, len(data))
	}
	var id chunk.ChunkID
	copy(id[:], data[:16])
	nl := int(binary.BigEndian.Uint16(data[16:18]))
	if len(data) < 18+nl {
		return chunk.ChunkID{}, "", fmt.Errorf("%s: truncated node id", opName)
	}
	return id, string(data[18 : 18+nl]), nil
}

// ---------- Snapshot encode / decode ----------
//
// Two sections (gastrolog-4cxw0):
//
//	sectionChunkPlacement = 4
//	  Per-chunk Receiving + Holding + FinalSetHash. PendingPulls live
//	  in section 5 to keep the placement section fixed-shape and cheap
//	  to scan.
//
//	  Layout:
//	    4 bytes  count of entries (BE uint32)
//	    repeated per entry:
//	      16 bytes  chunk ID
//	      32 bytes  FinalSetHash
//	       4 bytes  Receiving count (BE uint32)
//	       repeated:
//	          2 bytes  node ID length (BE uint16)
//	          N bytes  node ID string
//	       4 bytes  Holding count (BE uint32)
//	       repeated:
//	          2 bytes  node ID length (BE uint16)
//	          N bytes  node ID string
//
//	sectionPendingPulls = 5
//	  Per-chunk PendingPulls.
//
//	  Layout:
//	    4 bytes  count of entries (BE uint32)
//	    repeated per entry:
//	      16 bytes  chunk ID
//	       4 bytes  fromNode count (BE uint32)
//	       repeated:
//	          2 bytes  fromNode length (BE uint16)
//	          N bytes  fromNode string
//	          4 bytes  toNode count (BE uint32)
//	          repeated:
//	             2 bytes  toNode length (BE uint16)
//	             M bytes  toNode string

func encodeChunkPlacementSection(w io.Writer, placements map[chunk.ChunkID]*ChunkPlacement) error {
	// Only emit entries that have any state worth serializing — empty
	// placements (no nodes, no hash) get omitted to keep the section
	// small for single-node / memory-mode clusters.
	type pair struct {
		id chunk.ChunkID
		p  *ChunkPlacement
	}
	live := make([]pair, 0, len(placements))
	for id, p := range placements {
		if p == nil {
			continue
		}
		if len(p.Receiving) == 0 && len(p.Holding) == 0 && p.FinalSetHash == [32]byte{} {
			continue
		}
		live = append(live, pair{id, p})
	}
	if len(live) == 0 {
		return nil
	}
	payloadLen := 4
	for _, e := range live {
		payloadLen += 16 + 32 + 4 + 4
		for _, n := range e.p.Receiving {
			payloadLen += 2 + len(n)
		}
		for _, n := range e.p.Holding {
			payloadLen += 2 + len(n)
		}
	}
	if payloadLen > 0xFFFFFFFF {
		return errors.New("chunkPlacement section exceeds 4 GiB; corruption suspected")
	}
	if err := writeSectionHeader(w, sectionChunkPlacement, uint32(payloadLen)); err != nil {
		return err
	}
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(live))) //nolint:gosec // G115: bounded
	if _, err := w.Write(countBuf[:]); err != nil {
		return fmt.Errorf("write chunkPlacement count: %w", err)
	}
	for _, e := range live {
		if err := encodeChunkPlacementEntry(w, e.id, e.p); err != nil {
			return err
		}
	}
	return nil
}

func encodeChunkPlacementEntry(w io.Writer, id chunk.ChunkID, p *ChunkPlacement) error {
	var hdr [16 + 32]byte
	copy(hdr[0:16], id[:])
	copy(hdr[16:48], p.FinalSetHash[:])
	if _, err := w.Write(hdr[:]); err != nil {
		return fmt.Errorf("write chunkPlacement header: %w", err)
	}
	if err := writeNodeList(w, p.Receiving); err != nil {
		return fmt.Errorf("write chunkPlacement Receiving: %w", err)
	}
	if err := writeNodeList(w, p.Holding); err != nil {
		return fmt.Errorf("write chunkPlacement Holding: %w", err)
	}
	return nil
}

func encodePendingPullsSection(w io.Writer, placements map[chunk.ChunkID]*ChunkPlacement) error {
	type pair struct {
		id chunk.ChunkID
		p  *ChunkPlacement
	}
	live := make([]pair, 0, len(placements))
	for id, p := range placements {
		if p == nil || len(p.PendingPulls) == 0 {
			continue
		}
		live = append(live, pair{id, p})
	}
	if len(live) == 0 {
		return nil
	}
	payloadLen := 4
	for _, e := range live {
		payloadLen += 16 + 4
		for from, set := range e.p.PendingPulls {
			payloadLen += 2 + len(from) + 4
			for to := range set {
				payloadLen += 2 + len(to)
			}
		}
	}
	if payloadLen > 0xFFFFFFFF {
		return errors.New("pendingPulls section exceeds 4 GiB; corruption suspected")
	}
	if err := writeSectionHeader(w, sectionPendingPulls, uint32(payloadLen)); err != nil {
		return err
	}
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(live))) //nolint:gosec // G115: bounded
	if _, err := w.Write(countBuf[:]); err != nil {
		return fmt.Errorf("write pendingPulls count: %w", err)
	}
	for _, e := range live {
		if err := encodePendingPullsEntry(w, e.id, e.p.PendingPulls); err != nil {
			return err
		}
	}
	return nil
}

func encodePendingPullsEntry(w io.Writer, id chunk.ChunkID, pulls map[string]map[string]bool) error {
	if _, err := w.Write(id[:]); err != nil {
		return fmt.Errorf("write pendingPulls chunkID: %w", err)
	}
	var fc [4]byte
	binary.BigEndian.PutUint32(fc[:], uint32(len(pulls))) //nolint:gosec // G115: bounded
	if _, err := w.Write(fc[:]); err != nil {
		return fmt.Errorf("write pendingPulls fromNode count: %w", err)
	}
	for from, set := range pulls {
		var fl [2]byte
		binary.BigEndian.PutUint16(fl[:], uint16(len(from))) //nolint:gosec // G115: bounded
		if _, err := w.Write(fl[:]); err != nil {
			return fmt.Errorf("write pendingPulls fromNode length: %w", err)
		}
		if _, err := w.Write([]byte(from)); err != nil {
			return fmt.Errorf("write pendingPulls fromNode: %w", err)
		}
		var tc [4]byte
		binary.BigEndian.PutUint32(tc[:], uint32(len(set))) //nolint:gosec // G115: bounded
		if _, err := w.Write(tc[:]); err != nil {
			return fmt.Errorf("write pendingPulls toNode count: %w", err)
		}
		for to := range set {
			var tl [2]byte
			binary.BigEndian.PutUint16(tl[:], uint16(len(to))) //nolint:gosec // G115: bounded
			if _, err := w.Write(tl[:]); err != nil {
				return fmt.Errorf("write pendingPulls toNode length: %w", err)
			}
			if _, err := w.Write([]byte(to)); err != nil {
				return fmt.Errorf("write pendingPulls toNode: %w", err)
			}
		}
	}
	return nil
}

// writeNodeList serializes a []string as [4-byte count][repeated: 2-byte
// len + body].
func writeNodeList(w io.Writer, nodes []string) error {
	var cb [4]byte
	binary.BigEndian.PutUint32(cb[:], uint32(len(nodes))) //nolint:gosec // G115: bounded
	if _, err := w.Write(cb[:]); err != nil {
		return fmt.Errorf("write node-list count: %w", err)
	}
	for _, n := range nodes {
		var nl [2]byte
		binary.BigEndian.PutUint16(nl[:], uint16(len(n))) //nolint:gosec // G115: bounded
		if _, err := w.Write(nl[:]); err != nil {
			return fmt.Errorf("write node-list entry length: %w", err)
		}
		if _, err := w.Write([]byte(n)); err != nil {
			return fmt.Errorf("write node-list entry body: %w", err)
		}
	}
	return nil
}

func readChunkPlacementSection(r io.Reader) (map[chunk.ChunkID]*ChunkPlacement, error) {
	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf("read chunkPlacement count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBuf[:])
	out := make(map[chunk.ChunkID]*ChunkPlacement, count)
	for i := range count {
		var hdr [16 + 32]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, fmt.Errorf("read chunkPlacement %d header: %w", i, err)
		}
		var id chunk.ChunkID
		copy(id[:], hdr[0:16])
		p := &ChunkPlacement{}
		copy(p.FinalSetHash[:], hdr[16:48])
		var err error
		p.Receiving, err = readNodeList(r)
		if err != nil {
			return nil, fmt.Errorf("read chunkPlacement %d Receiving: %w", i, err)
		}
		p.Holding, err = readNodeList(r)
		if err != nil {
			return nil, fmt.Errorf("read chunkPlacement %d Holding: %w", i, err)
		}
		out[id] = p
	}
	return out, nil
}

func readPendingPullsSection(r io.Reader) (map[chunk.ChunkID]map[string]map[string]bool, error) {
	var countBuf [4]byte
	if _, err := io.ReadFull(r, countBuf[:]); err != nil {
		return nil, fmt.Errorf("read pendingPulls count: %w", err)
	}
	count := binary.BigEndian.Uint32(countBuf[:])
	out := make(map[chunk.ChunkID]map[string]map[string]bool, count)
	for i := range count {
		var hdr [16 + 4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return nil, fmt.Errorf("read pendingPulls %d header: %w", i, err)
		}
		var id chunk.ChunkID
		copy(id[:], hdr[0:16])
		fromCount := binary.BigEndian.Uint32(hdr[16:20])
		pulls := make(map[string]map[string]bool, fromCount)
		for j := range fromCount {
			var fl [2]byte
			if _, err := io.ReadFull(r, fl[:]); err != nil {
				return nil, fmt.Errorf("read pendingPulls %d.%d fromNode len: %w", i, j, err)
			}
			fromLen := int(binary.BigEndian.Uint16(fl[:]))
			fromBuf := make([]byte, fromLen)
			if _, err := io.ReadFull(r, fromBuf); err != nil {
				return nil, fmt.Errorf("read pendingPulls %d.%d fromNode: %w", i, j, err)
			}
			var tc [4]byte
			if _, err := io.ReadFull(r, tc[:]); err != nil {
				return nil, fmt.Errorf("read pendingPulls %d.%d toNode count: %w", i, j, err)
			}
			toCount := binary.BigEndian.Uint32(tc[:])
			set := make(map[string]bool, toCount)
			for k := range toCount {
				var tl [2]byte
				if _, err := io.ReadFull(r, tl[:]); err != nil {
					return nil, fmt.Errorf("read pendingPulls %d.%d.%d toNode len: %w", i, j, k, err)
				}
				toLen := int(binary.BigEndian.Uint16(tl[:]))
				toBuf := make([]byte, toLen)
				if _, err := io.ReadFull(r, toBuf); err != nil {
					return nil, fmt.Errorf("read pendingPulls %d.%d.%d toNode: %w", i, j, k, err)
				}
				set[string(toBuf)] = true
			}
			pulls[string(fromBuf)] = set
		}
		out[id] = pulls
	}
	return out, nil
}

// readNodeList reverses writeNodeList.
func readNodeList(r io.Reader) ([]string, error) {
	var cb [4]byte
	if _, err := io.ReadFull(r, cb[:]); err != nil {
		return nil, fmt.Errorf("read node-list count: %w", err)
	}
	count := binary.BigEndian.Uint32(cb[:])
	if count == 0 {
		return nil, nil
	}
	out := make([]string, 0, count)
	for i := range count {
		var nl [2]byte
		if _, err := io.ReadFull(r, nl[:]); err != nil {
			return nil, fmt.Errorf("read node-list %d length: %w", i, err)
		}
		l := int(binary.BigEndian.Uint16(nl[:]))
		buf := make([]byte, l)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, fmt.Errorf("read node-list %d body: %w", i, err)
		}
		out = append(out, string(buf))
	}
	return out, nil
}
