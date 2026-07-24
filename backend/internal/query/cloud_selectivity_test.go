package query_test

// Tests for gastrolog-4of7c: applyCloudSelectivity scales a cloud-backed
// bucket's contribution by a single global local-filter ratio. That derived
// count must be visibly labeled as an estimate wherever it surfaces:
//   - ComputeHistogram's HistogramBucket.HasCloudData/CloudCount (sidebar
//     "Volume" histogram — search_histogram.go buildHistogramBuckets).
//   - runTimechart's TableResult sentinel columns (the `| timechart`
//     pipeline operator's chart/table output — histogram.go timechartToTable).
//   - the multi-node merge of that TableResult across cluster nodes
//     (server/query_merge.go), so a bucket flagged on one node doesn't get
//     silently dropped or split when combined with other nodes' results.
//
// A bucket whose count comes ONLY from an exact local filtered scan (no
// cloud-backed chunk touches it) must NOT carry the flag — it isn't derived.

import (
	"context"
	"fmt"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	"gastrolog/internal/memtest"
	"gastrolog/internal/query"
	"gastrolog/internal/querylang"
)

// buildCloudSelectivityFixture constructs a two-bucket, two-vault fixture:
//
//   - Bucket 0 [t0, t0+5s): 2 local records, both matching "keep". No cloud
//     data at all — this bucket's filtered count must come back exact.
//   - Bucket 1 [t0+5s, t0+10s): 2 local records (1 "keep", 1 "drop") AND
//     2 cloud-backed records (unfiltered, cloud content is irrelevant —
//     applyCloudSelectivity never scans it, only scales the raw count).
//
// Global local selectivity across both buckets: filtered=3 (2+1) /
// total=4 (2+2) = 0.75. Cloud bucket 1's raw count (2) scales to
// int64(2*0.75) = 1, so bucket 1's final count is 1 (local) + 1
// (estimated cloud) = 2.
func buildCloudSelectivityFixture(t *testing.T) (eng *query.Engine, q query.Query, t0 time.Time) {
	t.Helper()
	t0 = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	local := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	localRecords := []struct {
		offset time.Duration
		raw    string
	}{
		{0 * time.Second, "keep bucket0 a"},
		{1 * time.Second, "keep bucket0 b"},
		{5 * time.Second, "keep bucket1 a"},
		{6 * time.Second, "drop bucket1 b"},
	}
	for _, r := range localRecords {
		local.CM.Append(chunk.Record{
			IngestTS: t0.Add(r.offset),
			Raw:      []byte(r.raw),
		})
	}
	local.CM.Seal()

	cloud := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 2 {
		cloud.CM.Append(chunk.Record{
			IngestTS: t0.Add(5*time.Second + time.Duration(i)*time.Second),
			Raw:      fmt.Appendf(nil, "cloud-record-%d", i),
		})
	}
	cloud.CM.Seal()

	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}
	reg.vaults[glid.New()] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{local.CM, local.IM}
	reg.vaults[glid.New()] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{&cloudBackedCM{cloud.CM}, cloud.IM}

	eng = query.NewWithRegistry(reg, nil)

	expr, err := querylang.Parse("keep")
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	q = query.Query{
		BoolExpr: expr,
		Start:    t0,
		End:      t0.Add(10 * time.Second),
	}
	return eng, q, t0
}

// TestComputeHistogramCloudSelectivityFlagsScaledBucketOnly verifies that
// ComputeHistogram flags exactly the bucket whose count was scaled by
// applyCloudSelectivity, and leaves the exact local-only bucket unflagged
// with its precise filtered count.
func TestComputeHistogramCloudSelectivityFlagsScaledBucketOnly(t *testing.T) {
	eng, q, _ := buildCloudSelectivityFixture(t)

	buckets := eng.ComputeHistogram(context.Background(), q, 2)
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}

	b0, b1 := buckets[0], buckets[1]

	if b0.HasCloudData {
		t.Errorf("bucket 0 (local-only, exact) must not be flagged as cloud-derived")
	}
	if b0.Count != 2 {
		t.Errorf("bucket 0 count = %d, want 2 (exact filtered local count)", b0.Count)
	}

	if !b1.HasCloudData {
		t.Errorf("bucket 1 (scaled via applyCloudSelectivity) must be flagged as cloud-derived")
	}
	// selectivity = 3/4 = 0.75; raw cloud count = 2; estimated = int64(2*0.75) = 1.
	if b1.CloudCount != 1 {
		t.Errorf("bucket 1 CloudCount = %d, want 1 (scaled estimate)", b1.CloudCount)
	}
	if b1.Count != 2 {
		t.Errorf("bucket 1 count = %d, want 2 (1 exact local + 1 estimated cloud)", b1.Count)
	}
}

// TestComputeHistogramCloudSelectivityZeroLocalMatches covers the edge case
// where the filter matches zero local records: selectivity is 0, so the
// estimated cloud contribution rounds down to 0 — but the bucket must still
// carry HasCloudData=true, because it IS cloud-derived (just derived to
// nothing), not because it's an exact count.
func TestComputeHistogramCloudSelectivityZeroLocalMatches(t *testing.T) {
	eng, q, _ := buildCloudSelectivityFixture(t)

	// Filter that matches no local records at all (selectivity = 0/4 = 0).
	expr, err := querylang.Parse("nonexistent-term")
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	q.BoolExpr = expr

	buckets := eng.ComputeHistogram(context.Background(), q, 2)
	if len(buckets) != 2 {
		t.Fatalf("expected 2 buckets, got %d", len(buckets))
	}

	b1 := buckets[1]
	if !b1.HasCloudData {
		t.Errorf("bucket 1 must stay flagged as cloud-derived even when the estimate rounds to 0")
	}
	if b1.CloudCount != 0 {
		t.Errorf("bucket 1 CloudCount = %d, want 0 (selectivity 0 scales cloud contribution to 0)", b1.CloudCount)
	}
	if b1.Count != 0 {
		t.Errorf("bucket 1 count = %d, want 0 (no local matches, no scaled cloud contribution)", b1.Count)
	}
}

// TestComputeHistogramCloudSelectivityNoLocalChunks covers the "no local
// chunks" edge: applyCloudSelectivity's fallback when localTotal == 0 (no
// local data to derive a ratio from) passes the raw, unscaled cloud count
// through as-is. That's an even less precise number than the scaled
// estimate, so the bucket must still carry HasCloudData=true.
func TestComputeHistogramCloudSelectivityNoLocalChunks(t *testing.T) {
	t0 := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	cloud := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	for i := range 5 {
		cloud.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      fmt.Appendf(nil, "cloud-record-%d", i),
		})
	}
	cloud.CM.Seal()

	reg := &testRegistry{
		vaults: make(map[glid.GLID]struct {
			cm chunk.ChunkManager
			im index.IndexManager
		}),
	}
	reg.vaults[glid.New()] = struct {
		cm chunk.ChunkManager
		im index.IndexManager
	}{&cloudBackedCM{cloud.CM}, cloud.IM}

	eng := query.NewWithRegistry(reg, nil)

	expr, err := querylang.Parse("cloud")
	if err != nil {
		t.Fatalf("parse filter: %v", err)
	}
	q := query.Query{
		BoolExpr: expr,
		Start:    t0,
		End:      t0.Add(5 * time.Second),
	}

	buckets := eng.ComputeHistogram(context.Background(), q, 1)
	if len(buckets) != 1 {
		t.Fatalf("expected 1 bucket, got %d", len(buckets))
	}
	b := buckets[0]
	if !b.HasCloudData {
		t.Errorf("bucket with no local chunks at all must still be flagged as cloud-derived")
	}
	if b.Count != 5 {
		t.Errorf("count = %d, want 5 (raw unscaled cloud count, no local ratio available)", b.Count)
	}
}

// TestRunTimechartCloudSelectivitySentinelColumns verifies the `| timechart`
// pipeline operator's TableResult carries the same per-bucket cloud
// provenance as ComputeHistogram, via the TimechartCloudFlagColumn /
// TimechartCloudCountColumn sentinel columns — this is the path that feeds
// the chart's "table" view mode and the CLI, so labeling must be present
// there too, not just on the sidebar histogram.
func TestRunTimechartCloudSelectivitySentinelColumns(t *testing.T) {
	eng, q, _ := buildCloudSelectivityFixture(t)

	pipeline := &querylang.Pipeline{
		Pipes: []querylang.PipeOp{&querylang.TimechartOp{N: 2}},
	}
	result, err := eng.RunPipeline(context.Background(), q, pipeline)
	if err != nil {
		t.Fatalf("RunPipeline: %v", err)
	}
	if result.Table == nil {
		t.Fatal("expected table result")
	}

	countIdx := -1
	cloudFlagIdx := -1
	cloudCountIdx := -1
	for i, c := range result.Table.Columns {
		switch c {
		case "count":
			countIdx = i
		case query.TimechartCloudFlagColumn:
			cloudFlagIdx = i
		case query.TimechartCloudCountColumn:
			cloudCountIdx = i
		}
	}
	if countIdx == -1 || cloudFlagIdx == -1 || cloudCountIdx == -1 {
		t.Fatalf("expected count/%s/%s columns, got %v", query.TimechartCloudFlagColumn, query.TimechartCloudCountColumn, result.Table.Columns)
	}

	if len(result.Table.Rows) != 2 {
		t.Fatalf("expected 2 rows (one per bucket, no group-by), got %d", len(result.Table.Rows))
	}

	// Rows are chronological: row 0 = bucket 0 (local-only, exact), row 1 = bucket 1 (scaled).
	row0, row1 := result.Table.Rows[0], result.Table.Rows[1]

	if row0[cloudFlagIdx] != "false" {
		t.Errorf("bucket 0 %s = %q, want false", query.TimechartCloudFlagColumn, row0[cloudFlagIdx])
	}
	if row0[countIdx] != "2" {
		t.Errorf("bucket 0 count = %q, want 2", row0[countIdx])
	}

	if row1[cloudFlagIdx] != "true" {
		t.Errorf("bucket 1 %s = %q, want true", query.TimechartCloudFlagColumn, row1[cloudFlagIdx])
	}
	if row1[cloudCountIdx] != "1" {
		t.Errorf("bucket 1 %s = %q, want 1", query.TimechartCloudCountColumn, row1[cloudCountIdx])
	}
	if row1[countIdx] != "2" {
		t.Errorf("bucket 1 count = %q, want 2", row1[countIdx])
	}
}
