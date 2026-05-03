package query

import (
	"context"
	"gastrolog/internal/glid"
	"time"
)

// HistogramBucket holds the count for a single time bucket in the volume histogram.
type HistogramBucket struct {
	TimestampMs  int64            // Bucket start (milliseconds since epoch)
	Count        int64            // Total records in this bucket
	GroupCounts  map[string]int64 // Level → count; records without level → "other"
	HasCloudData bool             // True if cloud chunks cover this bucket
	CloudCount   int64            // Records from cloud chunks (subset of Count)
}

const histogramGroupField = "level"

// ComputeHistogram returns an approximate volume histogram grouped by level
// for the given query's time range.
//
// When the query has no filter expression, uses the fast binary-search path
// for totals and per-bucket attr sampling for the level breakdown.
// When a filter is present, falls back to a record scan so the histogram
// reflects the filtered result set.
func (e *Engine) ComputeHistogram(ctx context.Context, q Query, numBuckets int) []HistogramBucket {
	numBuckets = clampBuckets(numBuckets)
	selectedVaults := e.timechartVaults(q)

	if q.Start.IsZero() || q.End.IsZero() {
		e.deriveTimeRange(&q, selectedVaults)
	}

	start, end := q.Start, q.End
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return nil
	}

	bucketWidth := end.Sub(start) / time.Duration(numBuckets)
	if bucketWidth <= 0 {
		bucketWidth = time.Second
	}

	acc := &histogramAccum{
		counts:      make([]int64, numBuckets),
		cloudFlags:  make([]bool, numBuckets),
		cloudCounts: make([]int64, numBuckets),
		groupCounts: make([]map[string]int64, numBuckets),
	}
	for i := range acc.groupCounts {
		acc.groupCounts[i] = make(map[string]int64)
	}

	hasFilter := q.BoolExpr != nil
	_, _ = e.runTimechartStrategy(ctx, q, nil, selectedVaults,
		start, end, bucketWidth, numBuckets,
		hasFilter, false, true, histogramGroupField,
		acc)

	return buildHistogramBuckets(start, bucketWidth, numBuckets, acc.counts, acc.groupCounts, acc.cloudFlags, acc.cloudCounts)
}

// ComputeHistogramForVaults computes a histogram for specific vaults only.
// Used by the forward search handler to compute a per-node histogram.
func (e *Engine) ComputeHistogramForVaults(ctx context.Context, q Query, numBuckets int, vaultIDs []glid.GLID) []HistogramBucket {
	numBuckets = clampBuckets(numBuckets)

	if q.Start.IsZero() || q.End.IsZero() {
		e.deriveTimeRange(&q, vaultIDs)
	}

	start, end := q.Start, q.End
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return nil
	}

	bucketWidth := end.Sub(start) / time.Duration(numBuckets)
	if bucketWidth <= 0 {
		bucketWidth = time.Second
	}

	acc := &histogramAccum{
		counts:      make([]int64, numBuckets),
		cloudFlags:  make([]bool, numBuckets),
		cloudCounts: make([]int64, numBuckets),
		groupCounts: make([]map[string]int64, numBuckets),
	}
	for i := range acc.groupCounts {
		acc.groupCounts[i] = make(map[string]int64)
	}

	hasFilter := q.BoolExpr != nil
	_, _ = e.runTimechartStrategy(ctx, q, nil, vaultIDs,
		start, end, bucketWidth, numBuckets,
		hasFilter, false, true, histogramGroupField,
		acc)

	return buildHistogramBuckets(start, bucketWidth, numBuckets, acc.counts, acc.groupCounts, acc.cloudFlags, acc.cloudCounts)
}

// buildHistogramBuckets converts raw count arrays into HistogramBucket structs.
//
// For buckets where the per-bucket sample didn't cover every record (typically
// because the chunk's cloud blob isn't locally cached, so the level scan was
// best-effort), the unsampled leftover is distributed across levels using the
// GLOBAL level ratio observed across all chunks we *did* sample. The result
// is an estimate of the level breakdown rather than a giant "other" lump.
// Buckets where every record was sampled get an exact breakdown unchanged.
//
// Buckets where we have NO local samples at all (no chunks at all reachable
// for the level scan) and HAVE cloud data still fall back to a single "other"
// entry — there's no information to estimate from.
func buildHistogramBuckets(start time.Time, bucketWidth time.Duration, numBuckets int, counts []int64, groupCounts []map[string]int64, cloudFlags []bool, cloudCounts []int64) []HistogramBucket {
	// Aggregate the level totals across every bucket so we can derive a
	// global ratio to apply when individual buckets don't have full sample
	// coverage.
	globalTotals := make(map[string]int64)
	var globalSampled int64
	for _, gc := range groupCounts {
		for k, v := range gc {
			globalTotals[k] += v
			globalSampled += v
		}
	}

	buckets := make([]HistogramBucket, numBuckets)
	for i := range numBuckets {
		buckets[i].TimestampMs = start.Add(bucketWidth * time.Duration(i)).UnixMilli()
		buckets[i].HasCloudData = cloudFlags[i]
		if cloudCounts != nil {
			buckets[i].CloudCount = cloudCounts[i]
		}
		if counts[i] == 0 && !cloudFlags[i] {
			continue
		}
		buckets[i].Count = counts[i]

		gc := groupCounts[i]
		var knownSum int64
		for _, v := range gc {
			knownSum += v
		}
		leftover := counts[i] - knownSum
		if leftover <= 0 {
			buckets[i].GroupCounts = gc
			continue
		}

		if globalSampled > 0 {
			// Distribute the unsampled leftover across observed levels in
			// proportion to the global ratio. Use a remainder bucket to
			// keep the rounded sum exactly equal to leftover so the
			// rendered bar matches the bucket count.
			if gc == nil {
				gc = make(map[string]int64)
			}
			var assigned int64
			for k, v := range globalTotals {
				share := leftover * v / globalSampled
				gc[k] += share
				assigned += share
			}
			if rem := leftover - assigned; rem > 0 {
				gc["other"] += rem
			}
		} else {
			// No local samples anywhere → no ratio to apply. Leftover
			// goes to "other" as a single lump.
			if gc == nil {
				gc = make(map[string]int64)
			}
			gc["other"] += leftover
		}
		buckets[i].GroupCounts = gc
	}
	return buckets
}
