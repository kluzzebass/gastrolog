package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// TestDedupChunkReportsCollapsesReplicas verifies that multiple entries for the
// same chunk ID from different nodes are collapsed into one, with replica_count
// tracking the number of distinct nodes.
func TestDedupChunkReportsCollapsesReplicas(t *testing.T) {
	t.Parallel()

	meta := func(id string, records int64) *apiv1.ChunkMeta {
		return &apiv1.ChunkMeta{Id: []byte(id), RecordCount: records, Sealed: true, Compressed: true}
	}
	input := []chunkReport{
		{reportingNode: "n1", chunk: meta("chunk-a", 100)},
		{reportingNode: "n2", chunk: meta("chunk-a", 100)},
		{reportingNode: "n3", chunk: meta("chunk-a", 100)},
		{reportingNode: "n1", chunk: meta("chunk-b", 50)},
		{reportingNode: "n2", chunk: meta("chunk-b", 50)},
	}

	out := dedupChunkReports(input)
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

// TestDedupChunkReportsSameNodeDoesNotInflateReplicas verifies that duplicate
// list rows for the same chunk from one node (e.g. multiple local tiers) only
// count as one replica.
func TestDedupChunkReportsSameNodeDoesNotInflateReplicas(t *testing.T) {
	t.Parallel()

	input := []chunkReport{
		{reportingNode: "node-a", chunk: &apiv1.ChunkMeta{Id: []byte("c"), RecordCount: 10, Sealed: true, Compressed: true}},
		{reportingNode: "node-a", chunk: &apiv1.ChunkMeta{Id: []byte("c"), RecordCount: 10, Sealed: true, Compressed: true}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if out[0].ReplicaCount != 1 {
		t.Errorf("replica count = %d, want 1", out[0].ReplicaCount)
	}
}

// TestDedupChunkReportsPrefersSealedAndCompressed verifies that when multiple
// versions of the same chunk are reported (e.g. a follower's partial view
// and the leader's sealed/compressed view), the most advanced version wins.
func TestDedupChunkReportsPrefersSealedAndCompressed(t *testing.T) {
	t.Parallel()

	// Order matters: put the partial version first to confirm the
	// authoritative version replaces it.
	input := []chunkReport{
		{reportingNode: "follower", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 50, Sealed: false, Compressed: false}},
		{reportingNode: "leader", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 100, Sealed: true, Compressed: true}},
	}

	out := dedupChunkReports(input)
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

// TestDedupChunkReportsPrefersSealedOverUnsealed verifies the order-independence
// of the authoritative check.
func TestDedupChunkReportsPrefersSealedOverUnsealed(t *testing.T) {
	t.Parallel()

	input := []chunkReport{
		{reportingNode: "leader", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 100, Sealed: true, Compressed: true}},
		{reportingNode: "follower", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 50, Sealed: false, Compressed: false}},
	}

	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	got := out[0]
	if got.RecordCount != 100 {
		t.Errorf("expected record count 100, got %d", got.RecordCount)
	}
}

// TestDedupChunkReportsEmptyInput verifies the empty case.
func TestDedupChunkReportsEmptyInput(t *testing.T) {
	t.Parallel()
	out := dedupChunkReports(nil)
	if len(out) != 0 {
		t.Errorf("expected empty output for nil input, got %d entries", len(out))
	}
}

// TestDedupChunkReportsSingleEntryReplicaCount verifies that a single-copy chunk
// gets replica_count=1.
func TestDedupChunkReportsSingleEntryReplicaCount(t *testing.T) {
	t.Parallel()
	input := []chunkReport{
		{reportingNode: "solo", chunk: &apiv1.ChunkMeta{Id: []byte("solo"), RecordCount: 10, Sealed: true}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if out[0].ReplicaCount != 1 {
		t.Errorf("replica count = %d, want 1", out[0].ReplicaCount)
	}
}

// TestDedupChunkReportsOrsRetentionPending verifies that when the same chunk ID
// is reported from multiple nodes, retention_pending is true if any replica
// reported it (e.g. leader enriched vs follower not).
func TestDedupChunkReportsOrsRetentionPending(t *testing.T) {
	t.Parallel()
	input := []chunkReport{
		{reportingNode: "n1", chunk: &apiv1.ChunkMeta{Id: []byte("c"), Sealed: true, Compressed: true, RetentionPending: false}},
		{reportingNode: "n2", chunk: &apiv1.ChunkMeta{Id: []byte("c"), Sealed: true, Compressed: true, RetentionPending: true}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if !out[0].RetentionPending {
		t.Fatal("expected RetentionPending OR'd to true across replicas")
	}
}

// TestDedupChunkReportsOrsTransitionStreamed mirrors retention_pending OR logic
// for the post-transition, pre-delete phase on the source tier.
func TestDedupChunkReportsOrsTransitionStreamed(t *testing.T) {
	t.Parallel()
	input := []chunkReport{
		{reportingNode: "n1", chunk: &apiv1.ChunkMeta{Id: []byte("c"), Sealed: true, Compressed: true, TransitionStreamed: false}},
		{reportingNode: "n2", chunk: &apiv1.ChunkMeta{Id: []byte("c"), Sealed: true, Compressed: true, TransitionStreamed: true}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if !out[0].TransitionStreamed {
		t.Fatal("expected TransitionStreamed OR'd to true across replicas")
	}
}
