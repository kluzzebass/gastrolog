package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestChunkBucketTotals_InvertedRangeReturnsNil guards against the cluster
// crash where a transiently inconsistent ChunkMeta (IngestEnd < IngestStart
// during a seal / transition) caused makeslice to panic with "len out of
// range". The call must return nil, not panic.
func TestChunkBucketTotals_InvertedRangeReturnsNil(t *testing.T) {
	t.Parallel()

	totals := chunkBucketTotals(
		nil, nil,
		chunk.ChunkMeta{Sealed: true},
		time.Unix(0, 0),
		time.Second,
		10, 5,
	)
	if totals != nil {
		t.Fatalf("expected nil totals for inverted bucket range, got %v", totals)
	}
}

// TestTimechartChunkGroups_InvertedMetaIsNoOp guards the upstream caller
// that derives firstBucket / lastBucket from meta.IngestStart and
// meta.IngestEnd. When those are inverted (transient state during a seal),
// the chunk must contribute nothing rather than crash the node.
func TestTimechartChunkGroups_InvertedMetaIsNoOp(t *testing.T) {
	t.Parallel()

	start := time.Unix(0, 0)
	bucketWidth := time.Second
	numBuckets := 60

	// IngestStart is well after IngestEnd — only reachable via a partial
	// Raft apply where one of the two bounds got updated and the other
	// hasn't yet.
	meta := chunk.ChunkMeta{
		Sealed:      true,
		RecordCount: 1,
		IngestStart: start.Add(30 * time.Second),
		IngestEnd:   start.Add(2 * time.Second),
	}

	groupCounts := make([]map[string]int64, numBuckets)
	for i := range groupCounts {
		groupCounts[i] = make(map[string]int64)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("timechartChunkGroups panicked on inverted meta: %v", r)
		}
	}()
	timechartChunkGroups(nil, nil, meta, start, bucketWidth, numBuckets, 100, "level", groupCounts)

	for b, m := range groupCounts {
		if len(m) != 0 {
			t.Errorf("bucket %d got %d groups, want 0", b, len(m))
		}
	}
}
