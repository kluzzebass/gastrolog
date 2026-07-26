package query

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// boundaryOnlyIR mimics the orchestrator IndexReader on a voter with no
// local ITSI bytes (gastrolog-enfwd): sealed monotonic chunk metadata
// answers rank 0 for timestamps strictly before IngestStart; every other
// lookup is unresolvable. Partial resolvability is exactly what the
// per-lookup ok tracking in timechartChunkByIndex must handle.
type boundaryOnlyIR struct {
	ingestStart time.Time
}

func (ir *boundaryOnlyIR) FindIngestRank(_ chunk.ChunkID, ts time.Time) (uint64, bool) {
	if ts.Before(ir.ingestStart) {
		return 0, true
	}
	return 0, false
}

func (ir *boundaryOnlyIR) FindIngestPos(_ chunk.ChunkID, ts time.Time) (uint64, bool) {
	return ir.FindIngestRank(chunk.ChunkID{}, ts)
}

func boundaryMeta(t0 time.Time, span time.Duration, cloudBacked bool) chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:                chunk.NewChunkID(),
		Sealed:            true,
		RecordCount:       500,
		IngestStart:       t0,
		IngestEnd:         t0.Add(span),
		IngestTSMonotonic: true,
		CloudBacked:       cloudBacked,
	}
}

// A sealed monotonic chunk that fits inside a single bucket counts EXACTLY
// from FSM metadata alone: startRank 0 at the bucket edge (boundary answer),
// endRank = RecordCount via the IngestEnd shortcut. No bytes involved.
func TestTimechartChunkByIndexSingleBucketExactFromMetadata(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	const numBuckets = 10
	bucketWidth := 10 * time.Second

	for _, cloud := range []bool{false, true} {
		// Chunk lives entirely inside bucket 3: [start+32s, start+38s].
		meta := boundaryMeta(start.Add(32*time.Second), 6*time.Second, cloud)
		ir := &boundaryOnlyIR{ingestStart: meta.IngestStart}

		counts := make([]int64, numBuckets)
		cloudFlags := make([]bool, numBuckets)
		if !timechartChunkByIndex(ir, meta, start, bucketWidth, 3, 3, counts, cloudFlags) {
			t.Fatalf("cloud=%v: expected exact metadata count to apply", cloud)
		}
		for i, c := range counts {
			want := int64(0)
			if i == 3 {
				want = meta.RecordCount
			}
			if c != want {
				t.Errorf("cloud=%v: bucket %d = %d, want %d", cloud, i, c, want)
			}
		}
		if cloudFlags[3] != cloud {
			t.Errorf("cloud=%v: cloudFlags[3] = %v", cloud, cloudFlags[3])
		}
	}
}

// A multi-bucket chunk with only boundary answers is NOT fully resolvable:
// rank arithmetic must be abandoned on the first interior miss. Cloud-backed
// chunks fall back to the proportional FSM overlap estimate (the
// gastrolog-1952x residual); local chunks report false and contribute
// nothing rather than fabricating counts.
func TestTimechartChunkByIndexPartialResolutionFallsBack(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	const numBuckets = 10
	bucketWidth := 10 * time.Second

	// Chunk spans buckets 1..6: [start+15s, start+65s].
	meta := boundaryMeta(start.Add(15*time.Second), 50*time.Second, false)
	ir := &boundaryOnlyIR{ingestStart: meta.IngestStart}

	counts := make([]int64, numBuckets)
	if timechartChunkByIndex(ir, meta, start, bucketWidth, 1, 6, counts, nil) {
		t.Fatal("local chunk with partial resolution must report false, not apply counts")
	}
	for i, c := range counts {
		if c != 0 {
			t.Errorf("local partial resolution leaked counts: bucket %d = %d", i, c)
		}
	}

	// Cloud-backed variant: overlap estimate distributes RecordCount across
	// the chunk's span instead of dropping it or dumping it in one bucket.
	cloudMeta := boundaryMeta(start.Add(15*time.Second), 50*time.Second, true)
	cloudIR := &boundaryOnlyIR{ingestStart: cloudMeta.IngestStart}
	cloudCounts := make([]int64, numBuckets)
	cloudFlags := make([]bool, numBuckets)
	if !timechartChunkByIndex(cloudIR, cloudMeta, start, bucketWidth, 1, 6, cloudCounts, cloudFlags) {
		t.Fatal("cloud-backed chunk must fall back to the overlap estimate")
	}
	var total int64
	nonZero := 0
	for _, c := range cloudCounts {
		total += c
		if c > 0 {
			nonZero++
		}
	}
	if total == 0 {
		t.Error("overlap estimate contributed nothing")
	}
	if total > cloudMeta.RecordCount {
		t.Errorf("overlap estimate total = %d, want <= RecordCount %d", total, cloudMeta.RecordCount)
	}
	if nonZero < 2 {
		t.Errorf("overlap estimate landed in %d bucket(s), want spread across the chunk span", nonZero)
	}
}

// Regression guard (latent pre-enfwd overcount): with a partially resolving
// reader, the old loop ignored per-lookup ok — trailing buckets past the
// chunk's IngestEnd combined a missed startRank (treated as 0) with the
// RecordCount shortcut, applying RecordCount to EVERY trailing bucket. The
// per-lookup tracking must reject the chunk instead.
func TestTimechartChunkByIndexNoTrailingBucketOvercount(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	const numBuckets = 10
	bucketWidth := 10 * time.Second

	// Chunk spans buckets 2..4; invoked over the FULL bucket range the way
	// timechartChunkByIngestTS's local retry path does.
	meta := boundaryMeta(start.Add(25*time.Second), 20*time.Second, false)
	ir := &boundaryOnlyIR{ingestStart: meta.IngestStart}

	counts := make([]int64, numBuckets)
	if timechartChunkByIndex(ir, meta, start, bucketWidth, 0, numBuckets-1, counts, nil) {
		t.Fatal("partially resolvable local chunk applied counts over the full range")
	}
	for i, c := range counts {
		if c != 0 {
			t.Errorf("trailing-bucket overcount: bucket %d = %d, want 0", i, c)
		}
	}
}

// A chunk whose IngestStart falls exactly on the bucket edge gets no
// boundary answer (strictly-before contract) — deterministic fallback, no
// spurious exactness claim.
func TestTimechartChunkByIndexEdgeAlignedStartUnresolvable(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 7, 1, 10, 0, 0, 0, time.UTC)
	bucketWidth := 10 * time.Second

	meta := boundaryMeta(start.Add(30*time.Second), 5*time.Second, false)
	ir := &boundaryOnlyIR{ingestStart: meta.IngestStart}

	counts := make([]int64, 10)
	// Bucket 3 starts exactly at IngestStart: strictly-before misses.
	if timechartChunkByIndex(ir, meta, start, bucketWidth, 3, 3, counts, nil) {
		t.Fatal("edge-aligned start must not resolve via the boundary answer")
	}
}
