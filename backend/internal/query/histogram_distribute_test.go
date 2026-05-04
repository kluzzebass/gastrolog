package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// TestDistributeChunkRecordsByOverlap covers the fallback used when a
// cloud chunk's local IngestTS index isn't cached — without this, the
// histogram silently undercounts the chunk's records (vault inspector
// shows N records, histogram shows N/2).
func TestDistributeChunkRecordsByOverlap(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 5, 3, 10, 0, 0, 0, time.UTC)
	bucketWidth := time.Minute
	const numBuckets = 10

	t.Run("chunk fully inside a single bucket", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		meta := chunk.ChunkMeta{
			RecordCount: 100,
			IngestStart: t0.Add(2*time.Minute + 10*time.Second),
			IngestEnd:   t0.Add(2*time.Minute + 50*time.Second),
		}
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 0, numBuckets-1, counts, nil, false)
		if counts[2] != 100 {
			t.Errorf("bucket 2 got %d, want 100", counts[2])
		}
		for i := range numBuckets {
			if i != 2 && counts[i] != 0 {
				t.Errorf("bucket %d got %d, want 0", i, counts[i])
			}
		}
	})

	t.Run("chunk spans exactly two buckets evenly", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		meta := chunk.ChunkMeta{
			RecordCount: 100,
			IngestStart: t0.Add(3 * time.Minute),
			IngestEnd:   t0.Add(5 * time.Minute),
		}
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 0, numBuckets-1, counts, nil, false)
		if counts[3] != 50 || counts[4] != 50 {
			t.Errorf("bucket [3]=%d [4]=%d, want 50/50", counts[3], counts[4])
		}
	})

	t.Run("chunk spans 4 buckets uniformly", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		meta := chunk.ChunkMeta{
			RecordCount: 200,
			IngestStart: t0.Add(2 * time.Minute),
			IngestEnd:   t0.Add(6 * time.Minute),
		}
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 0, numBuckets-1, counts, nil, false)
		var total int64
		for _, c := range counts {
			total += c
		}
		// Floor rounding; each of buckets 2..5 gets 50, sum should equal RecordCount exactly.
		if total != 200 {
			t.Errorf("sum = %d, want 200", total)
		}
		for i := 2; i <= 5; i++ {
			if counts[i] != 50 {
				t.Errorf("bucket %d got %d, want 50", i, counts[i])
			}
		}
	})

	t.Run("chunk partially before histogram start gets clipped to firstBucket", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		// Chunk spans 1m before histogram start through bucket 1 end (2m total).
		meta := chunk.ChunkMeta{
			RecordCount: 200,
			IngestStart: t0.Add(-1 * time.Minute),
			IngestEnd:   t0.Add(1 * time.Minute),
		}
		// firstBucket=0 so out-of-range portion (the 1 min before t0) is
		// dropped; only the in-range overlap (1 min, half the chunk) counts.
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 0, numBuckets-1, counts, nil, false)
		if counts[0] != 100 {
			t.Errorf("bucket 0 got %d, want 100 (half the chunk)", counts[0])
		}
		var rest int64
		for i := 1; i < numBuckets; i++ {
			rest += counts[i]
		}
		if rest != 0 {
			t.Errorf("buckets 1..N sum = %d, want 0", rest)
		}
	})

	t.Run("zero RecordCount is a no-op", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		meta := chunk.ChunkMeta{
			RecordCount: 0,
			IngestStart: t0,
			IngestEnd:   t0.Add(5 * time.Minute),
		}
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 0, numBuckets-1, counts, nil, false)
		for i, c := range counts {
			if c != 0 {
				t.Errorf("bucket %d got %d, want 0", i, c)
			}
		}
	})

	t.Run("zero-span chunk drops count into IngestStart bucket", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		meta := chunk.ChunkMeta{
			RecordCount: 100,
			IngestStart: t0.Add(3 * time.Minute),
			IngestEnd:   t0.Add(3 * time.Minute), // identical
		}
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 0, numBuckets-1, counts, nil, false)
		if counts[3] != 100 {
			t.Errorf("bucket 3 got %d, want 100", counts[3])
		}
	})

	// Regression: non-monotonic cloud chunks (IngestEnd < IngestStart) used
	// to dump RecordCount into the IngestStart bucket via the span≤0
	// branch — but for a cloud chunk built from out-of-order records,
	// IngestStart is the last-APPENDED record's TS (which can be recent),
	// not the chunk's actual TS upper bound. Result: histogram attributed
	// records to file-tier-only recent buckets every ~rotation interval.
	// The fix sorts the bounds before computing span/overlap so records
	// distribute across the chunk's actual [min, max] TS envelope.
	t.Run("non-monotonic cloud chunk distributes across sorted [min,max] envelope", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		cloudFlags := make([]bool, numBuckets)
		// Reversed: IngestStart later than IngestEnd. After sorting, the
		// envelope is [t0+2m, t0+7m], spanning 5 buckets — records should
		// distribute across buckets 2..6 (each 1m bucket overlaps by 1m
		// of the 5m span = 20% × 100 = 20 records). Bucket 7 should NOT
		// receive records.
		meta := chunk.ChunkMeta{
			RecordCount: 100,
			IngestStart: t0.Add(7 * time.Minute),
			IngestEnd:   t0.Add(2 * time.Minute), // earlier than IngestStart
			CloudBacked: true,
		}
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 2, 7, counts, cloudFlags, true)
		var total int64
		for _, c := range counts {
			total += c
		}
		if total != 100 {
			t.Errorf("total = %d, want 100", total)
		}
		// Bucket 7 should NOT receive records (TS=t0+7m is the boundary;
		// overlap with [t0+7m, t0+8m] is 0). Recent-bucket false-positive
		// regression check.
		if counts[7] != 0 {
			t.Errorf("bucket 7 got %d, want 0 (was the false-positive recent bucket)", counts[7])
		}
		if cloudFlags[7] {
			t.Error("bucket 7 should not be cloud-flagged (no records landed here)")
		}
		// Each of buckets 2..6 should have 20 records (uniform distribution).
		for i := 2; i <= 6; i++ {
			if counts[i] != 20 {
				t.Errorf("bucket %d got %d, want 20", i, counts[i])
			}
			if !cloudFlags[i] {
				t.Errorf("bucket %d should be cloud-flagged", i)
			}
		}
	})

	t.Run("monotonic cloud chunk marks cloudFlags only on overlapping buckets", func(t *testing.T) {
		counts := make([]int64, numBuckets)
		cloudFlags := make([]bool, numBuckets)
		// Chunk spans bucket 3 + bucket 4 only; firstBucket/lastBucket of
		// 0..9 covers a wider scan window (typical histogram bucket range).
		meta := chunk.ChunkMeta{
			RecordCount: 100,
			IngestStart: t0.Add(3 * time.Minute),
			IngestEnd:   t0.Add(5 * time.Minute),
			CloudBacked: true,
		}
		distributeChunkRecordsByOverlap(meta, t0, bucketWidth, 0, 9, counts, cloudFlags, true)
		// Buckets 3 and 4 should be cloud-flagged (records overlap).
		// All other buckets (no overlap → no records) should not be flagged.
		for i := range numBuckets {
			expectedFlag := (i == 3 || i == 4)
			if cloudFlags[i] != expectedFlag {
				t.Errorf("bucket %d cloudFlags=%v, want %v", i, cloudFlags[i], expectedFlag)
			}
		}
	})
}
