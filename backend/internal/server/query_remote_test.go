package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

// TestMergeHistogramBucketsPreservesCloudProvenance is the regression test
// for gastrolog-4of7c: mergeHistogramBuckets combines the sidebar
// "Volume" histogram across cluster nodes (query.go/query_pipeline.go
// call it on every fan-out search/pipeline request). Before this fix, a
// remote node's HasCloudData/CloudCount were silently dropped whenever its
// bucket matched an existing timestamp — the merged bucket only kept
// whichever node happened to seed the map first. In a cluster where only
// some nodes host the cloud-backed chunk for a given bucket, that meant the
// cluster-wide histogram could present an applyCloudSelectivity-derived
// count as exact just because the coordinator's own local bucket (with no
// cloud data) was merged first.
func TestMergeHistogramBucketsPreservesCloudProvenance(t *testing.T) {
	// Local node: bucket at t0 has no cloud data at all (exact).
	local := []*apiv1.HistogramBucket{
		{TimestampMs: 1000, Count: 4, HasCloudData: false, CloudCount: 0},
	}
	// Remote node: same bucket, but this node's applyCloudSelectivity pass
	// found cloud-backed chunks and scaled in a contribution.
	remote := []*apiv1.HistogramBucket{
		{TimestampMs: 1000, Count: 3, HasCloudData: true, CloudCount: 2},
	}

	merged := mergeHistogramBuckets(local, remote)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged bucket, got %d", len(merged))
	}

	b := merged[0]
	if b.Count != 7 {
		t.Errorf("Count = %d, want 7 (4+3 summed across nodes)", b.Count)
	}
	if !b.HasCloudData {
		t.Errorf("HasCloudData = false, want true — the remote node's cloud-derived data must not be dropped on merge")
	}
	if b.CloudCount != 2 {
		t.Errorf("CloudCount = %d, want 2 (0+2 summed across nodes)", b.CloudCount)
	}
}

// TestMergeHistogramBucketsCloudFlagSymmetric verifies merge order doesn't
// matter: whichever slice seeds the timestamp index first, a true
// HasCloudData from the other side still wins (OR, not "whichever came
// first").
func TestMergeHistogramBucketsCloudFlagSymmetric(t *testing.T) {
	withCloud := []*apiv1.HistogramBucket{
		{TimestampMs: 1000, Count: 1, HasCloudData: true, CloudCount: 1},
	}
	withoutCloud := []*apiv1.HistogramBucket{
		{TimestampMs: 1000, Count: 1, HasCloudData: false, CloudCount: 0},
	}

	if got := mergeHistogramBuckets(withCloud, withoutCloud); !got[0].HasCloudData {
		t.Errorf("cloud-flagged bucket seeded first: HasCloudData = false, want true")
	}

	// Reset — mergeHistogramBuckets mutates slice `a` in place.
	withCloud = []*apiv1.HistogramBucket{
		{TimestampMs: 1000, Count: 1, HasCloudData: true, CloudCount: 1},
	}
	withoutCloud = []*apiv1.HistogramBucket{
		{TimestampMs: 1000, Count: 1, HasCloudData: false, CloudCount: 0},
	}
	if got := mergeHistogramBuckets(withoutCloud, withCloud); !got[0].HasCloudData {
		t.Errorf("cloud-flagged bucket seeded second: HasCloudData = false, want true")
	}
}
