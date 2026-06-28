package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// partialRankIR returns rank arithmetic for the first half of buckets only,
// simulating rankTotal < RecordCount when part of the chunk is outside the
// histogram window. The old overlap fallback smeared the full RecordCount
// across metadata bounds, painting phantom counts in empty buckets.
type partialRankIR struct {
	start         time.Time
	bucketWidth   time.Duration
	perBucket     int64
	activeBuckets int
}

func (ir *partialRankIR) FindIngestRank(_ chunk.ChunkID, ts time.Time) (uint64, bool) {
	if ts.Before(ir.start) {
		return 0, true
	}
	b := int(ts.Sub(ir.start) / ir.bucketWidth)
	if b < 0 {
		return 0, true
	}
	if b >= ir.activeBuckets {
		return uint64(ir.activeBuckets * int(ir.perBucket)), true
	}
	return uint64(b * int(ir.perBucket)), true
}

func (ir *partialRankIR) FindIngestPos(_ chunk.ChunkID, _ time.Time) (uint64, bool) {
	return 0, false
}

func TestTimechartChunkByIndexLocalPartialRankNoOverlapSmear(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 6, 24, 21, 8, 0, 0, time.UTC)
	const numBuckets = 10
	bucketWidth := 10 * time.Second
	start := t0

	meta := chunk.ChunkMeta{
		ID:                chunk.NewChunkID(),
		Sealed:            true,
		RecordCount:       1000,
		IngestStart:       t0,
		IngestEnd:         t0.Add(9 * time.Minute),
		IngestTSMonotonic: true,
	}

	ir := &partialRankIR{
		start:         start,
		bucketWidth:   bucketWidth,
		perBucket:     10,
		activeBuckets: 5, // ranks only for first 5 buckets → rankTotal=50
	}

	counts := make([]int64, numBuckets)
	if !timechartChunkByIndex(ir, meta, start, bucketWidth, 0, numBuckets-1, counts, nil) {
		t.Fatal("expected rank path to apply partial counts")
	}

	var total int64
	for i, c := range counts {
		total += c
		if i >= 5 && c != 0 {
			t.Errorf("bucket %d = %d, want 0 (no overlap smear into quiet period)", i, c)
		}
	}
	if total != 50 {
		t.Errorf("total = %d, want 50 (partial in-window rank counts only)", total)
	}
}
