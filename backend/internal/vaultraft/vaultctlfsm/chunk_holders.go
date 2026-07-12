package vaultctlfsm

import (
	"errors"
	"slices"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
)

// Chunk holder receipts: which nodes hold a sealed chunk's verified GLCB
// bytes. Holders are EARNED (local build, catch-up pull) and REVOKED
// (stat-miss), so residency reflects bytes, not intent. Receipts are the
// ONLY source of ChunkResidency for live chunks — no placement fallback:
// an optimistic default both overstated (a home that lost its bytes kept
// being counted while retention starved on chunks it didn't hold) and
// made residency non-monotonic (full placement set collapsing to the
// true holders when the first receipt landed — the sealed-pips-regress-
// to-amber bug, gastrolog-68wsli).

// applyAckChunkHolder appends nodeID to each chunk's holder set. Idempotent
// per chunk; unknown chunk IDs are skipped (sealed-then-expunged races).
// Copy-on-write: entry copies handed out by Get/List share the old backing
// array, which is never mutated. Caller holds f.mu.
func (f *FSM) applyAckChunkHolder(c *gastrologv1.AckChunkHolderCommand) ([]chunk.ChunkID, error) {
	nodeID := c.GetNodeId()
	if nodeID == "" {
		return nil, errors.New("ack chunk holder: node id required")
	}
	var added []chunk.ChunkID
	for _, raw := range c.GetChunkIds() {
		id := chunkIDFromProto(raw)
		entry, ok := f.chunks[id]
		if !ok {
			continue
		}
		if slices.Contains(entry.Holders, nodeID) {
			continue
		}
		holders := make([]string, 0, len(entry.Holders)+1)
		holders = append(holders, entry.Holders...)
		holders = append(holders, nodeID)
		entry.Holders = holders
		added = append(added, id)
	}
	return added, nil
}

// applyRevokeChunkHolder removes nodeID from each chunk's holder set.
// Idempotent; unknown chunks and absent claims are skipped. Copy-on-write,
// same contract as applyAckChunkHolder. Caller holds f.mu.
func (f *FSM) applyRevokeChunkHolder(c *gastrologv1.RevokeChunkHolderCommand) ([]chunk.ChunkID, error) {
	nodeID := c.GetNodeId()
	if nodeID == "" {
		return nil, errors.New("revoke chunk holder: node id required")
	}
	var removed []chunk.ChunkID
	for _, raw := range c.GetChunkIds() {
		id := chunkIDFromProto(raw)
		entry, ok := f.chunks[id]
		if !ok {
			continue
		}
		i := slices.Index(entry.Holders, nodeID)
		if i < 0 {
			continue
		}
		holders := make([]string, 0, len(entry.Holders)-1)
		holders = append(holders, entry.Holders[:i]...)
		holders = append(holders, entry.Holders[i+1:]...)
		entry.Holders = holders
		removed = append(removed, id)
	}
	return removed, nil
}

// NewAckChunkHolders builds an AckChunkHolder VaultCtlCommand covering every
// given chunk. Batched: one Raft apply per sweep pass, mirroring the segment
// holder receipts.
func NewAckChunkHolders(chunkIDs []chunk.ChunkID, nodeID string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{
		Command: &gastrologv1.VaultCtlCommand_AckChunkHolder{
			AckChunkHolder: &gastrologv1.AckChunkHolderCommand{
				ChunkIds: chunkIDsToProto(chunkIDs),
				NodeId:   nodeID,
			},
		},
	}
}

// MarshalAckChunkHolders builds Raft log data acking every given chunk in
// one command.
func MarshalAckChunkHolders(chunkIDs []chunk.ChunkID, nodeID string) ([]byte, error) {
	return mustMarshalCommand(NewAckChunkHolders(chunkIDs, nodeID)), nil
}

// NewRevokeChunkHolders builds a RevokeChunkHolder VaultCtlCommand covering
// every given chunk.
func NewRevokeChunkHolders(chunkIDs []chunk.ChunkID, nodeID string) *gastrologv1.VaultCtlCommand {
	return &gastrologv1.VaultCtlCommand{
		Command: &gastrologv1.VaultCtlCommand_RevokeChunkHolder{
			RevokeChunkHolder: &gastrologv1.RevokeChunkHolderCommand{
				ChunkIds: chunkIDsToProto(chunkIDs),
				NodeId:   nodeID,
			},
		},
	}
}

// MarshalRevokeChunkHolders builds Raft log data revoking every given chunk
// in one command.
func MarshalRevokeChunkHolders(chunkIDs []chunk.ChunkID, nodeID string) ([]byte, error) {
	return mustMarshalCommand(NewRevokeChunkHolders(chunkIDs, nodeID)), nil
}

func chunkIDsToProto(chunkIDs []chunk.ChunkID) [][]byte {
	raw := make([][]byte, len(chunkIDs))
	for i, id := range chunkIDs {
		raw[i] = append([]byte(nil), id[:]...)
	}
	return raw
}
