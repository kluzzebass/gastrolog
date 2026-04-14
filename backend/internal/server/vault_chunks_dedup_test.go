package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// TestDedupChunksCollapsesReplicas verifies that multiple entries for the same
// chunk ID are collapsed into one, with replica_count tracking the number of
// distinct copies.
func TestDedupChunksCollapsesReplicas(t *testing.T) {
	t.Parallel()

	input := []*apiv1.ChunkMeta{
		{Id: []byte("chunk-a"), RecordCount: 100, Sealed: true, Compressed: true},
		{Id: []byte("chunk-a"), RecordCount: 100, Sealed: true, Compressed: true},
		{Id: []byte("chunk-a"), RecordCount: 100, Sealed: true, Compressed: true},
		{Id: []byte("chunk-b"), RecordCount: 50, Sealed: true, Compressed: true},
		{Id: []byte("chunk-b"), RecordCount: 50, Sealed: true, Compressed: true},
	}

	out := dedupChunks(input)
	if len(out) != 2 {
		t.Fatalf("expected 2 unique chunks, got %d", len(out))
	}

	byID := make(map[string]*apiv1.ChunkMeta)
	for _, c := range out {
		byID[string(c.Id)] = c
	}
	if byID["chunk-a"].ReplicaCount != 3 {
		t.Errorf("chunk-a replica count = %d, want 3", byID["chunk-a"].ReplicaCount)
	}
	if byID["chunk-b"].ReplicaCount != 2 {
		t.Errorf("chunk-b replica count = %d, want 2", byID["chunk-b"].ReplicaCount)
	}
}

// TestDedupChunksPrefersSealedAndCompressed verifies that when multiple
// versions of the same chunk are reported (e.g. a follower's partial view
// and the leader's sealed/compressed view), the most advanced version wins.
func TestDedupChunksPrefersSealedAndCompressed(t *testing.T) {
	t.Parallel()

	// Order matters: put the partial version first to confirm the
	// authoritative version replaces it.
	input := []*apiv1.ChunkMeta{
		{Id: []byte("chunk-x"), RecordCount: 50, Sealed: false, Compressed: false},
		{Id: []byte("chunk-x"), RecordCount: 100, Sealed: true, Compressed: true},
	}

	out := dedupChunks(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	got := out[0]
	if !got.Sealed || !got.Compressed {
		t.Errorf("expected sealed+compressed, got sealed=%v compressed=%v", got.Sealed, got.Compressed)
	}
	if got.RecordCount != 100 {
		t.Errorf("expected record count 100 (from the authoritative version), got %d", got.RecordCount)
	}
	if got.ReplicaCount != 2 {
		t.Errorf("replica count = %d, want 2", got.ReplicaCount)
	}
}

// TestDedupChunksPrefersSealedOverUnsealed verifies the order-independence
// of the authoritative check.
func TestDedupChunksPrefersSealedOverUnsealed(t *testing.T) {
	t.Parallel()

	// Put authoritative first — the partial should NOT replace it.
	input := []*apiv1.ChunkMeta{
		{Id: []byte("chunk-x"), RecordCount: 100, Sealed: true, Compressed: true},
		{Id: []byte("chunk-x"), RecordCount: 50, Sealed: false, Compressed: false},
	}

	out := dedupChunks(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	got := out[0]
	if got.RecordCount != 100 {
		t.Errorf("expected record count 100, got %d", got.RecordCount)
	}
}

// TestDedupChunksEmptyInput verifies the empty case.
func TestDedupChunksEmptyInput(t *testing.T) {
	t.Parallel()
	out := dedupChunks(nil)
	if len(out) != 0 {
		t.Errorf("expected empty output for nil input, got %d entries", len(out))
	}
}

// TestDedupChunksSingleEntryReplicaCount verifies that a single-copy chunk
// gets replica_count=1.
func TestDedupChunksSingleEntryReplicaCount(t *testing.T) {
	t.Parallel()
	input := []*apiv1.ChunkMeta{
		{Id: []byte("solo"), RecordCount: 10, Sealed: true},
	}
	out := dedupChunks(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if out[0].ReplicaCount != 1 {
		t.Errorf("replica count = %d, want 1", out[0].ReplicaCount)
	}
}
