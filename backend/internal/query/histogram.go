package query

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"strconv"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)

// runTimechart executes a timechart pipeline operator. It counts records by
// time bucket with an optional field breakdown.
//
// When By is empty, just counts total records per bucket.
// When By is set, groups by the named record attribute (requires record scan).
//
// Two modes:
//   - No grouping, no filter, no pre-ops: fast path using FindStartPosition
//     binary search. O(buckets * log(n)) per chunk, no record scanning.
//   - Otherwise: falls back to Search() → applyRecordOps() → manual binning.
//     Capped at 1M records.
//
// Returns a TableResult with columns ["_time", "count"] or ["_time", "<field>", "count"].
func (e *Engine) runTimechart(ctx context.Context, q Query, tc *querylang.TimechartOp, preOps []querylang.PipeOp) (*TableResult, error) {
	numBuckets := clampBuckets(tc.N)

	selectedStores := e.timechartStores(q)

	// If no time range, derive from chunk metadata.
	if q.Start.IsZero() || q.End.IsZero() {
		e.deriveTimeRange(&q, selectedStores)
	}

	// Determine group-by field name. Empty = no grouping (just total counts).
	groupField := tc.By

	// Normalize: timechart always needs lower < upper regardless of query direction.
	start, end := q.Start, q.End
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return &TableResult{Columns: timechartColumns(groupField)}, nil
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

	hasFilter := q.BoolExpr != nil
	hasPreOps := len(preOps) > 0
	hasGroupBy := groupField != ""

	if !hasGroupBy && !hasFilter && !hasPreOps {
		e.timechartFastPath(selectedStores, start, end, bucketWidth, numBuckets, counts)
	} else if err := e.timechartScanPath(ctx, q, preOps, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, hasPreOps, counts, groupCounts); err != nil {
		return nil, err
	}

	return timechartToTable(groupField, start, bucketWidth, numBuckets, counts, groupCounts), nil
}

// clampBuckets normalizes the bucket count to the valid range [1, 500], defaulting to 50.
func clampBuckets(n int) int {
	if n <= 0 {
		return 50
	}
	if n > 500 {
		return 500
	}
	return n
}

// timechartStores returns the stores to query for a timechart, applying any store filter.
func (e *Engine) timechartStores(q Query) []uuid.UUID {
	allStores := e.listStores()
	if q.BoolExpr != nil {
		if stores, _ := ExtractStoreFilter(q.BoolExpr, allStores); stores != nil {
			return stores
		}
	}
	return allStores
}

// deriveTimeRange fills in missing Start/End from chunk metadata across the selected stores.
func (e *Engine) deriveTimeRange(q *Query, selectedStores []uuid.UUID) {
	for _, storeID := range selectedStores {
		cm, _ := e.getStoreManagers(storeID)
		if cm == nil {
			continue
		}
		metas, err := cm.List()
		if err != nil {
			continue
		}
		for _, meta := range metas {
			if meta.RecordCount == 0 {
				continue
			}
			if q.Start.IsZero() || meta.StartTS.Before(q.Start) {
				q.Start = meta.StartTS
			}
			if q.End.IsZero() || meta.EndTS.After(q.End) {
				q.End = meta.EndTS
			}
		}
	}
}

// timechartFastPath counts records per bucket using binary search (no record scanning).
func (e *Engine) timechartFastPath(selectedStores []uuid.UUID, start time.Time, end time.Time, bucketWidth time.Duration, numBuckets int, counts []int64) {
	for _, storeID := range selectedStores {
		cm, _ := e.getStoreManagers(storeID)
		if cm == nil {
			continue
		}
		metas, err := cm.List()
		if err != nil {
			continue
		}
		for _, meta := range metas {
			if meta.RecordCount == 0 {
				continue
			}
			if meta.EndTS.Before(start) || !meta.StartTS.Before(end) {
				continue
			}
			timechartChunkFast(cm, meta, start, bucketWidth, numBuckets, counts)
		}
	}
}

// timechartScanPath counts records per bucket via record scanning with optional grouping and pre-ops.
func (e *Engine) timechartScanPath(ctx context.Context, q Query, preOps []querylang.PipeOp, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, hasGroupBy, hasPreOps bool, counts []int64, groupCounts []map[string]int64) error {
	q.Limit = 0
	iter, _ := e.Search(ctx, q, nil)

	if hasPreOps {
		return timechartScanPreOps(iter, preOps, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
	}
	return timechartScanDirect(iter, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
}

// timechartScanPreOps applies pipeline pre-ops then bins the resulting records.
func timechartScanPreOps(iter iter.Seq2[chunk.Record, error], preOps []querylang.PipeOp, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, hasGroupBy bool, counts []int64, groupCounts []map[string]int64) error {
	records, err := applyRecordOps(iter, preOps)
	if err != nil {
		return err
	}
	for _, rec := range records {
		timechartBinRecord(rec.WriteTS, rec.Attrs, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
	}
	return nil
}

// timechartScanDirect iterates records directly and bins them, capped at 1M records.
func timechartScanDirect(records iter.Seq2[chunk.Record, error], start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, hasGroupBy bool, counts []int64, groupCounts []map[string]int64) error {
	const maxScan = 1_000_000
	scanned := 0
	for rec, err := range records {
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return err
		}
		timechartBinRecord(rec.WriteTS, rec.Attrs, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
		scanned++
		if scanned >= maxScan {
			break
		}
	}
	return nil
}

// timechartBinRecord places a single record into the appropriate bucket, updating counts and group counts.
func timechartBinRecord(ts time.Time, attrs chunk.Attributes, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, hasGroupBy bool, counts []int64, groupCounts []map[string]int64) {
	if ts.Before(start) || !ts.Before(end) {
		return
	}
	idx := int(ts.Sub(start) / bucketWidth)
	if idx >= numBuckets {
		idx = numBuckets - 1
	}
	counts[idx]++
	if hasGroupBy {
		if v := attrs[groupField]; v != "" {
			groupCounts[idx][v]++
		}
	}
}

// timechartColumns returns the column list for a timechart result.
// Without grouping: ["_time", "count"]. With grouping: ["_time", "<field>", "count"].
func timechartColumns(groupField string) []string {
	if groupField == "" {
		return []string{"_time", "count"}
	}
	return []string{"_time", groupField, "count"}
}

// timechartToTable converts bucketed counts into a TableResult.
// Without grouping: one row per bucket with columns ["_time", "count"].
// With grouping: one row per bucket × group with columns ["_time", "<field>", "count"].
func timechartToTable(groupField string, start time.Time, bucketWidth time.Duration, numBuckets int, counts []int64, groupCounts []map[string]int64) *TableResult {
	columns := timechartColumns(groupField)
	var rows [][]string

	if groupField == "" {
		// No grouping — one row per bucket.
		for i := range numBuckets {
			ts := start.Add(bucketWidth * time.Duration(i)).Format(time.RFC3339Nano)
			rows = append(rows, []string{ts, strconv.FormatInt(counts[i], 10)})
		}
		return &TableResult{Columns: columns, Rows: rows}
	}

	// Grouped: collect all group values across all buckets for stable ordering.
	groupSet := make(map[string]struct{})
	for _, gc := range groupCounts {
		for k := range gc {
			groupSet[k] = struct{}{}
		}
	}
	groupKeys := slices.Sorted(maps.Keys(groupSet))

	for i := range numBuckets {
		ts := start.Add(bucketWidth * time.Duration(i)).Format(time.RFC3339Nano)
		total := counts[i]

		if len(groupCounts[i]) == 0 {
			// No group breakdown — emit a single row with empty group.
			rows = append(rows, []string{ts, "", strconv.FormatInt(total, 10)})
			continue
		}

		// Emit one row per group value that has counts.
		var groupTotal int64
		for _, key := range groupKeys {
			count, ok := groupCounts[i][key]
			if !ok || count == 0 {
				continue
			}
			rows = append(rows, []string{ts, key, strconv.FormatInt(count, 10)})
			groupTotal += count
		}
		// Emit remainder as empty-group row if total > sum of group counts.
		if remainder := total - groupTotal; remainder > 0 {
			rows = append(rows, []string{ts, "", strconv.FormatInt(remainder, 10)})
		}
	}

	return &TableResult{Columns: columns, Rows: rows}
}

// timechartChunkFast counts records per bucket using binary search on idx.log.
// O(buckets * log(n)) per chunk, no record scanning.
func timechartChunkFast(
	cm chunk.ChunkManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	numBuckets int,
	counts []int64,
) {
	end := start.Add(bucketWidth * time.Duration(numBuckets))

	firstBucket := 0
	if meta.StartTS.After(start) {
		firstBucket = int(meta.StartTS.Sub(start) / bucketWidth)
		if firstBucket >= numBuckets {
			return
		}
	}
	lastBucket := numBuckets - 1
	if meta.EndTS.Before(end) {
		lastBucket = int(meta.EndTS.Sub(start) / bucketWidth)
		if lastBucket >= numBuckets {
			lastBucket = numBuckets - 1
		}
	}

	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))

		var startPos uint64
		if pos, found, err := cm.FindStartPosition(meta.ID, bStart); err == nil && found {
			startPos = pos
		}

		var endPos uint64
		if !bEnd.Before(meta.EndTS) {
			endPos = uint64(meta.RecordCount) //nolint:gosec // G115: RecordCount is always non-negative
		} else if pos, found, err := cm.FindStartPosition(meta.ID, bEnd); err == nil && found {
			endPos = pos
		}

		if endPos > startPos {
			counts[b] += int64(endPos - startPos)
		}
	}
}

