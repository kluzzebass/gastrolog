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
		return &apiv1.ChunkMeta{Id: []byte(id), RecordCount: records, Sealed: true}
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
// list rows for the same chunk from one node (e.g. multiple local instances) only
// count as one replica.
func TestDedupChunkReportsSameNodeDoesNotInflateReplicas(t *testing.T) {
	t.Parallel()

	input := []chunkReport{
		{reportingNode: "node-a", chunk: &apiv1.ChunkMeta{Id: []byte("c"), RecordCount: 10, Sealed: true}},
		{reportingNode: "node-a", chunk: &apiv1.ChunkMeta{Id: []byte("c"), RecordCount: 10, Sealed: true}},
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
		{reportingNode: "follower", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 50, Sealed: false}},
		{reportingNode: "leader", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 100, Sealed: true}},
	}

	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	got := out[0]
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
		{reportingNode: "leader", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 100, Sealed: true}},
		{reportingNode: "follower", chunk: &apiv1.ChunkMeta{Id: []byte("chunk-x"), RecordCount: 50, Sealed: false}},
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
		{reportingNode: "n1", chunk: &apiv1.ChunkMeta{Id: []byte("c"), Sealed: true, RetentionPending: false}},
		{reportingNode: "n2", chunk: &apiv1.ChunkMeta{Id: []byte("c"), Sealed: true, RetentionPending: true}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if !out[0].RetentionPending {
		t.Fatal("expected RetentionPending OR'd to true across replicas")
	}
}

// TestDedupChunkReportsActiveChunkLeaderWinsRegardlessOfOrder is the
// regression test for active-chunk record-count oscillation. When the same
// active chunk is reported by both the leader (authoritative RecordCount)
// and a follower (stale or zero RecordCount because followers only
// replicate sealed chunks), the merged result must reflect the leader's
// count regardless of the order in which the two reports arrived in the
// dedup pass.
//
// Without the RecordCount tiebreaker in moreAuthoritative, whichever
// report was inserted first wins. In the fan-out, parallel peer RTTs
// determine that order; when the leader peer occasionally misses its
// timeout in Kubernetes, the follower's stale value wins the round and
// the inspector's displayed active-chunk record count oscillates.
func TestDedupChunkReportsActiveChunkLeaderWinsRegardlessOfOrder(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		input []chunkReport
	}{
		{
			name: "leader first",
			input: []chunkReport{
				{reportingNode: "leader", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 100, Sealed: false}},
				{reportingNode: "follower", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 0, Sealed: false}},
			},
		},
		{
			name: "follower first",
			input: []chunkReport{
				{reportingNode: "follower", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 0, Sealed: false}},
				{reportingNode: "leader", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 100, Sealed: false}},
			},
		},
		{
			name: "two followers then leader",
			input: []chunkReport{
				{reportingNode: "f1", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 0, Sealed: false}},
				{reportingNode: "f2", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 0, Sealed: false}},
				{reportingNode: "leader", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 100, Sealed: false}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			out := dedupChunkReports(tc.input)
			if len(out) != 1 {
				t.Fatalf("expected 1 chunk, got %d", len(out))
			}
			if out[0].RecordCount != 100 {
				t.Errorf("expected leader's RecordCount (100) to win, got %d", out[0].RecordCount)
			}
		})
	}
}

// TestDedupChunkReportsActiveChunkHigherCountWins covers the generalized
// freshness contract: among two unsealed views, the one with the higher
// RecordCount is the more-advanced view. Independent of which node is
// labelled "leader" so the test isn't coupled to placement semantics.
func TestDedupChunkReportsActiveChunkHigherCountWins(t *testing.T) {
	t.Parallel()
	input := []chunkReport{
		{reportingNode: "a", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 42, Sealed: false}},
		{reportingNode: "b", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 73, Sealed: false}},
		{reportingNode: "c", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 57, Sealed: false}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if out[0].RecordCount != 73 {
		t.Errorf("expected highest RecordCount (73) to win, got %d", out[0].RecordCount)
	}
	if out[0].ReplicaCount != 3 {
		t.Errorf("replica count = %d, want 3", out[0].ReplicaCount)
	}
}

// TestDedupChunkReportsSealedStillBeatsUnsealed pins the lifecycle ordering:
// sealed wins even when the unsealed view carries a higher RecordCount
// (which shouldn't normally happen, but a stale active-chunk announcement
// could in principle outlive a chunk that has already been sealed on the
// leader — the sealed view is the post-lifecycle truth).
func TestDedupChunkReportsSealedStillBeatsUnsealed(t *testing.T) {
	t.Parallel()
	input := []chunkReport{
		{reportingNode: "stale", chunk: &apiv1.ChunkMeta{Id: []byte("c"), RecordCount: 999, Sealed: false}},
		{reportingNode: "current", chunk: &apiv1.ChunkMeta{Id: []byte("c"), RecordCount: 100, Sealed: true}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if !out[0].Sealed {
		t.Errorf("expected sealed view to win, got Sealed=%v", out[0].Sealed)
	}
	if out[0].RecordCount != 100 {
		t.Errorf("expected sealed view's RecordCount (100), got %d", out[0].RecordCount)
	}
}

// TestDedupChunkReportsActiveChunkTieIsStable verifies the secondary case:
// when two unsealed reports carry the same RecordCount, the choice is
// deterministic (first-wins via map iteration). The display value is the
// same on both branches, so behavior is stable from the UI's perspective.
func TestDedupChunkReportsActiveChunkTieIsStable(t *testing.T) {
	t.Parallel()
	input := []chunkReport{
		{reportingNode: "a", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 50, Sealed: false}},
		{reportingNode: "b", chunk: &apiv1.ChunkMeta{Id: []byte("active"), RecordCount: 50, Sealed: false}},
	}
	out := dedupChunkReports(input)
	if len(out) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(out))
	}
	if out[0].RecordCount != 50 {
		t.Errorf("expected RecordCount 50 (tie), got %d", out[0].RecordCount)
	}
}
