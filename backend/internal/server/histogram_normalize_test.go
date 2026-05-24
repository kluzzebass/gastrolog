package server

import (
	"testing"

	apiv1 "gastrolog/api/gen/gastrolog/v1"
)

func gcSum(b *apiv1.HistogramBucket) int64 {
	var s int64
	for _, v := range b.GroupCounts {
		s += v
	}
	return s
}

// TestNormalizeHistogramGroupCounts_ScalesOvershootToCount reproduces the
// inspector spike: a bucket whose sampled level breakdown sums to ~2.7x its
// authoritative Count (the volume bar is a stack of GroupCounts, so it renders
// too tall). After normalization the breakdown must sum to exactly Count,
// preserving the relative level proportions.
func TestNormalizeHistogramGroupCounts_ScalesOvershootToCount(t *testing.T) {
	t.Parallel()

	buckets := []*apiv1.HistogramBucket{
		{
			TimestampMs: 1000,
			Count:       1600,
			// Sums to 4329 — the live 2.7x spike shape.
			GroupCounts: map[string]int64{
				"debug": 431, "error": 701, "info": 594, "other": 2184, "warn": 419,
			},
		},
		{
			TimestampMs: 2000,
			Count:       1600,
			GroupCounts: map[string]int64{ // already exact — must be untouched
				"debug": 160, "error": 280, "info": 200, "other": 800, "warn": 160,
			},
		},
	}
	beforeProportionOther := float64(buckets[0].GroupCounts["other"]) / float64(gcSum(buckets[0]))

	normalizeHistogramGroupCounts(buckets)

	if got := gcSum(buckets[0]); got != buckets[0].Count {
		t.Errorf("bucket0 groupSum = %d, want == Count %d", got, buckets[0].Count)
	}
	if got := gcSum(buckets[1]); got != buckets[1].Count {
		t.Errorf("bucket1 groupSum = %d, want == Count %d (exact bucket must stay exact)", got, buckets[1].Count)
	}
	// Proportions preserved (within rounding): "other" was ~50% before.
	afterProportionOther := float64(buckets[0].GroupCounts["other"]) / float64(gcSum(buckets[0]))
	if d := afterProportionOther - beforeProportionOther; d > 0.02 || d < -0.02 {
		t.Errorf("'other' proportion drifted: before=%.3f after=%.3f", beforeProportionOther, afterProportionOther)
	}
}

// TestNormalizeHistogramGroupCounts_LeavesCleanAndEmptyAlone guards the
// common cases: a bucket already summing to Count, a zero-count bucket, and a
// count-bearing bucket with no breakdown at all (cloud "data here, breakdown
// not loaded" ghost) must all pass through unchanged.
func TestNormalizeHistogramGroupCounts_LeavesCleanAndEmptyAlone(t *testing.T) {
	t.Parallel()

	buckets := []*apiv1.HistogramBucket{
		{TimestampMs: 1, Count: 0, GroupCounts: map[string]int64{}},
		{TimestampMs: 2, Count: 500, GroupCounts: nil}, // no breakdown — leave alone
		{TimestampMs: 3, Count: 300, GroupCounts: map[string]int64{"error": 100, "other": 200}},
	}
	normalizeHistogramGroupCounts(buckets)

	if len(buckets[1].GroupCounts) != 0 {
		t.Errorf("count-only bucket gained a breakdown: %v", buckets[1].GroupCounts)
	}
	if got := gcSum(buckets[2]); got != 300 {
		t.Errorf("clean bucket changed: groupSum=%d want 300", got)
	}
}
