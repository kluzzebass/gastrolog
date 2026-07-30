package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/chunk"
)

// Residency semantics: replica_node_ids means holder receipts,
// everywhere. These tests pin the two wire-mapping rules that
// keep ListChunks and WatchChunks from ping-ponging against each other:
// the ListChunks overlay replaces the fan-out-derived set whenever the
// FSM knows the chunk (including with an EMPTY set — the honest
// pre-receipt window), and the WatchChunks stamp writes only a non-empty
// set (replica_count zero means "no authoritative value" on the wire).

func residencyTestMeta(cid chunk.ChunkID, nodes ...string) *apiv1.ChunkMeta {
	return &apiv1.ChunkMeta{
		Id:             cid[:],
		ReplicaNodeIds: nodes,
		ReplicaCount:   int32(len(nodes)), //nolint:gosec // G115: test fixture, tiny
	}
}

func TestOverlayResidencyNilKeepsFanOutFallback(t *testing.T) {
	t.Parallel()
	cid := chunk.NewChunkID()
	// nil = no FSM for the vault (memory mode / single-node) or chunk
	// unknown — the reachability-derived set is the only signal there.
	chunks := []*apiv1.ChunkMeta{residencyTestMeta(cid, "node-1", "node-2")}
	overlayResidencyWith(chunks, func(chunk.ChunkID) []string { return nil })
	if len(chunks[0].ReplicaNodeIds) != 2 || chunks[0].ReplicaCount != 2 {
		t.Fatalf("nil residency must keep the fan-out fallback, got %v (count %d)",
			chunks[0].ReplicaNodeIds, chunks[0].ReplicaCount)
	}
}

func TestOverlayResidencyEmptyReplacesFanOutSet(t *testing.T) {
	t.Parallel()
	cid := chunk.NewChunkID()
	// Empty = chunk known to the FSM, zero receipts yet. The fan-out set
	// (who answered the RPC round) must NOT survive as fake residency:
	// that optimism is exactly what regressed sealed pips to amber.
	chunks := []*apiv1.ChunkMeta{residencyTestMeta(cid, "node-1", "node-2", "node-3")}
	overlayResidencyWith(chunks, func(chunk.ChunkID) []string { return []string{} })
	if len(chunks[0].ReplicaNodeIds) != 0 || chunks[0].ReplicaCount != 0 {
		t.Fatalf("empty residency must replace the fan-out set, got %v (count %d)",
			chunks[0].ReplicaNodeIds, chunks[0].ReplicaCount)
	}
}

func TestOverlayResidencyHoldersReplaceSortedWithCount(t *testing.T) {
	t.Parallel()
	cid := chunk.NewChunkID()
	chunks := []*apiv1.ChunkMeta{residencyTestMeta(cid, "node-9")}
	overlayResidencyWith(chunks, func(chunk.ChunkID) []string {
		return []string{"node-2", "node-1"}
	})
	got := chunks[0].ReplicaNodeIds
	if len(got) != 2 || got[0] != "node-1" || got[1] != "node-2" {
		t.Fatalf("holder residency must replace, sorted; got %v", got)
	}
	if chunks[0].ReplicaCount != 2 {
		t.Fatalf("replica count = %d, want 2", chunks[0].ReplicaCount)
	}
}

func TestOverlayResidencySkipsMalformedChunkIDs(t *testing.T) {
	t.Parallel()
	// A chunk meta with a wrong-length ID (defensive: forwarded peer
	// data) is left untouched rather than matched against a zero ID.
	chunks := []*apiv1.ChunkMeta{{Id: []byte{0x01}, ReplicaNodeIds: []string{"node-1"}, ReplicaCount: 1}}
	overlayResidencyWith(chunks, func(chunk.ChunkID) []string {
		t.Fatal("residency lookup must not run for malformed IDs")
		return nil
	})
	if len(chunks[0].ReplicaNodeIds) != 1 {
		t.Fatalf("malformed-ID meta mutated: %v", chunks[0].ReplicaNodeIds)
	}
}

func TestApplyResidencyStampSkipsEmptyAndNil(t *testing.T) {
	t.Parallel()
	cid := chunk.NewChunkID()
	for name, residency := range map[string][]string{"nil": nil, "empty": {}} {
		meta := residencyTestMeta(cid, "node-1", "node-2")
		applyResidencyStamp(meta, residency)
		if len(meta.ReplicaNodeIds) != 2 || meta.ReplicaCount != 2 {
			t.Fatalf("%s residency must leave the event meta unstamped (count zero = no value on the wire), got %v (count %d)",
				name, meta.ReplicaNodeIds, meta.ReplicaCount)
		}
	}
}

func TestApplyResidencyStampWritesSortedHolders(t *testing.T) {
	t.Parallel()
	cid := chunk.NewChunkID()
	meta := residencyTestMeta(cid)
	applyResidencyStamp(meta, []string{"node-3", "node-1"})
	got := meta.ReplicaNodeIds
	if len(got) != 2 || got[0] != "node-1" || got[1] != "node-3" {
		t.Fatalf("stamped residency = %v, want sorted [node-1 node-3]", got)
	}
	if meta.ReplicaCount != 2 {
		t.Fatalf("replica count = %d, want 2", meta.ReplicaCount)
	}
}
