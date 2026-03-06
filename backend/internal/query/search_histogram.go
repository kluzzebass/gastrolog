package query

import (
	"time"

	"github.com/google/uuid"
)

// HistogramBucket holds the count for a single time bucket in the volume histogram.
type HistogramBucket struct {
	TimestampMs int64              // Bucket start (milliseconds since epoch)
	Count       int64              // Total records in this bucket
	GroupCounts map[string]int64   // Level → count; records without level → "other"
}

const histogramGroupField = "level"

// ComputeHistogram returns an approximate volume histogram grouped by level
// for the given query's time range. Uses the fast binary-search path for totals
// and per-bucket attr sampling for the level breakdown.
//
// This is intentionally lightweight: it's designed to piggyback on every search
// response as a heatmap signal, not as an exact analytical result.
func (e *Engine) ComputeHistogram(q Query, numBuckets int) []HistogramBucket {
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

	counts := make([]int64, numBuckets)
	groupCounts := make([]map[string]int64, numBuckets)
	for i := range groupCounts {
		groupCounts[i] = make(map[string]int64)
	}

	e.timechartFastPath(selectedVaults, start, end, bucketWidth, numBuckets, counts)
	e.timechartAttrScanGroups(selectedVaults, start, end, bucketWidth, numBuckets, histogramGroupField, groupCounts)

	return buildHistogramBuckets(start, bucketWidth, numBuckets, counts, groupCounts)
}

// ComputeHistogramForVaults computes a histogram for specific vaults only.
// Used by the forward search handler to compute a per-node histogram.
func (e *Engine) ComputeHistogramForVaults(q Query, numBuckets int, vaultIDs []uuid.UUID) []HistogramBucket {
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

	counts := make([]int64, numBuckets)
	groupCounts := make([]map[string]int64, numBuckets)
	for i := range groupCounts {
		groupCounts[i] = make(map[string]int64)
	}

	e.timechartFastPath(vaultIDs, start, end, bucketWidth, numBuckets, counts)
	e.timechartAttrScanGroups(vaultIDs, start, end, bucketWidth, numBuckets, histogramGroupField, groupCounts)

	return buildHistogramBuckets(start, bucketWidth, numBuckets, counts, groupCounts)
}

// buildHistogramBuckets converts raw count arrays into HistogramBucket structs.
// Computes the "other" group as total minus the sum of known groups.
func buildHistogramBuckets(start time.Time, bucketWidth time.Duration, numBuckets int, counts []int64, groupCounts []map[string]int64) []HistogramBucket {
	buckets := make([]HistogramBucket, 0, numBuckets)
	for i := range numBuckets {
		if counts[i] == 0 {
			continue
		}

		gc := groupCounts[i]

		// Compute "other" as total minus sum of known groups.
		var knownSum int64
		for _, v := range gc {
			knownSum += v
		}
		if other := counts[i] - knownSum; other > 0 {
			if gc == nil {
				gc = make(map[string]int64)
			}
			gc["other"] = other
		}

		buckets = append(buckets, HistogramBucket{
			TimestampMs: start.Add(bucketWidth * time.Duration(i)).UnixMilli(),
			Count:       counts[i],
			GroupCounts: gc,
		})
	}
	return buckets
}
