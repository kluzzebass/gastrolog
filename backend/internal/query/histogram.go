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
	"gastrolog/internal/index"
	"gastrolog/internal/lookup"
	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)

// runTimechart executes a timechart pipeline operator. It counts records by
// time bucket with an optional field breakdown.
//
// When By is empty, just counts total records per bucket.
// When By is set, groups by the named record attribute (requires record scan).
//
// Three modes:
//   - No grouping, no filter, no pre-ops: fast path using FindIngestStartPosition
//     binary search. O(buckets * log(n)) per chunk, no record scanning.
//   - Grouping, no filter, no pre-ops: attr-only scan using ScanAttrs.
//     Reads only timestamps + attrs (~88 bytes/record on file vaults), uncapped.
//   - Filter or pre-ops: falls back to Search() → applyRecordOps() → manual binning.
//     Capped at 1M records; sets Truncated when cap is hit.
//
// Returns a TableResult with columns ["_time", "count"] or ["_time", "<field>", "count"].
func (e *Engine) runTimechart(ctx context.Context, q Query, tc *querylang.TimechartOp, preOps []querylang.PipeOp) (*TableResult, error) {
	numBuckets := clampBuckets(tc.N)

	selectedVaults := e.timechartVaults(q)

	// If no time range, derive from chunk metadata.
	if q.Start.IsZero() || q.End.IsZero() {
		e.deriveTimeRange(&q, selectedVaults)
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

	truncated, err := e.runTimechartStrategy(ctx, q, preOps, selectedVaults,
		start, end, bucketWidth, numBuckets,
		hasFilter, hasPreOps, hasGroupBy, groupField,
		counts, groupCounts)
	if err != nil {
		return nil, err
	}

	result := timechartToTable(groupField, start, bucketWidth, numBuckets, counts, groupCounts)
	result.Truncated = truncated
	return result, nil
}

// runTimechartStrategy selects the fastest histogram computation path based on query shape.
// Unfiltered queries use binary search for counts + per-bucket attr sampling for groups.
// Filtered queries fall back to full record scanning with a 1M cap.
func (e *Engine) runTimechartStrategy(
	ctx context.Context, q Query, preOps []querylang.PipeOp, selectedVaults []uuid.UUID,
	start, end time.Time, bucketWidth time.Duration, numBuckets int,
	hasFilter, hasPreOps, hasGroupBy bool, groupField string,
	counts []int64, groupCounts []map[string]int64, cloudFlagsOpt ...[]bool,
) (bool, error) {
	var cloudFlags []bool
	if len(cloudFlagsOpt) > 0 {
		cloudFlags = cloudFlagsOpt[0]
	}
	if hasFilter || hasPreOps {
		return e.timechartScanPath(ctx, q, preOps, start, end, bucketWidth,
			numBuckets, groupField, hasGroupBy, hasPreOps, counts, groupCounts)
	}

	// Exact total counts via IngestTS binary search — O(buckets × log(n)), instant.
	e.timechartFastPath(selectedVaults, start, end, bucketWidth, numBuckets, counts, cloudFlags)

	if !hasGroupBy {
		return false, nil
	}

	// Group breakdown via per-bucket sampling — O(buckets × 1000).
	e.timechartAttrScanGroups(selectedVaults, start, end, bucketWidth, numBuckets, groupField, groupCounts)
	return false, nil
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

// timechartVaults returns the vaults to query for a timechart, applying any vault filter.
func (e *Engine) timechartVaults(q Query) []uuid.UUID {
	allVaults := e.listVaults()
	if q.BoolExpr != nil {
		if vaults, _ := ExtractVaultFilter(q.BoolExpr, allVaults); vaults != nil {
			return vaults
		}
	}
	return allVaults
}

// deriveTimeRange fills in missing Start/End from chunk metadata across the selected vaults.
func (e *Engine) deriveTimeRange(q *Query, selectedVaults []uuid.UUID) {
	for _, vaultID := range selectedVaults {
		cm, _ := e.getVaultManagers(vaultID)
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
			if !meta.IngestStart.IsZero() && (q.Start.IsZero() || meta.IngestStart.Before(q.Start)) {
				q.Start = meta.IngestStart
			}
			if !meta.IngestEnd.IsZero() && (q.End.IsZero() || meta.IngestEnd.After(q.End)) {
				q.End = meta.IngestEnd
			}
		}
	}
}

// timechartFastPath counts records per bucket using IngestTS binary search (no record scanning).
// For the active chunk, uses the in-memory B-tree (FindIngestStartPosition).
// For sealed chunks, uses the persisted IngestTS index (LoadIngestEntries).
func (e *Engine) timechartFastPath(selectedVaults []uuid.UUID, start time.Time, end time.Time, bucketWidth time.Duration, numBuckets int, counts []int64, cloudFlags []bool) {
	for _, vaultID := range selectedVaults {
		cm, im := e.getVaultManagers(vaultID)
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
			if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(start) {
				continue
			}
			if !meta.IngestStart.IsZero() && !meta.IngestStart.Before(end) {
				continue
			}
			timechartChunkByIngestTS(cm, im, meta, start, bucketWidth, numBuckets, counts, cloudFlags)
		}
	}
}

// timechartAttrScanGroups populates group breakdown counts using per-bucket
// sampling. For each bucket, binary search finds the record position range,
// then ScanAttrs reads up to samplePerBucket attrs and scales the proportions
// to the exact count. Total cost: O(buckets × samplePerBucket) regardless of
// dataset size (~50K records for default 50 buckets).
// Does NOT update total counts — those come from timechartFastPath.
func (e *Engine) timechartAttrScanGroups(selectedVaults []uuid.UUID, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, groupCounts []map[string]int64) {
	const samplePerBucket = 1000

	for _, vaultID := range selectedVaults {
		cm, im := e.getVaultManagers(vaultID)
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
			if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(start) {
				continue
			}
			if !meta.IngestStart.IsZero() && !meta.IngestStart.Before(end) {
				continue
			}
			// Skip cloud chunks — ScanAttrs would download the entire blob
			// from S3 just for level breakdown sampling. Cloud chunks get
			// accurate total counts via TS index but no group breakdown.
			if meta.CloudBacked {
				continue
			}
			timechartChunkGroups(cm, im, meta, start, bucketWidth, numBuckets, samplePerBucket, groupField, groupCounts)
		}
	}
}

// timechartChunkGroups samples attrs per bucket within a single chunk.
// For each bucket that overlaps the chunk, it binary-searches for the position
// range via IngestTS, scans up to sampleSize records, and scales the observed
// group proportions to the exact bucket count (from binary search).
func timechartChunkGroups(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	numBuckets int,
	sampleSize int,
	groupField string,
	groupCounts []map[string]int64,
) {
	end := start.Add(bucketWidth * time.Duration(numBuckets))

	firstBucket := 0
	if !meta.IngestStart.IsZero() && meta.IngestStart.After(start) {
		firstBucket = int(meta.IngestStart.Sub(start) / bucketWidth)
		if firstBucket >= numBuckets {
			return
		}
	}
	lastBucket := numBuckets - 1
	if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(end) {
		lastBucket = int(meta.IngestEnd.Sub(start) / bucketWidth)
		if lastBucket >= numBuckets {
			lastBucket = numBuckets - 1
		}
	}

	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))

		startPos, startOK := findIngestPos(cm, im, meta.ID,bStart)
		if !startOK {
			continue
		}

		var endPos uint64
		if !meta.IngestEnd.IsZero() && !bEnd.Before(meta.IngestEnd) {
			endPos = uint64(meta.RecordCount) //nolint:gosec // G115: RecordCount is always non-negative
		} else if pos, ok := findIngestPos(cm, im, meta.ID,bEnd); ok {
			endPos = pos
		}

		bucketRecords := endPos - startPos
		if bucketRecords == 0 {
			continue
		}

		// Sample attrs from this bucket range.
		localCounts := make(map[string]int64)
		sampled := 0
		limit := min(int(bucketRecords), sampleSize)

		_ = cm.ScanAttrs(meta.ID, startPos, func(_ time.Time, attrs chunk.Attributes) bool {
			if v := attrs[groupField]; v != "" {
				localCounts[v]++
			}
			sampled++
			return sampled < limit
		})

		if sampled == 0 {
			continue
		}

		// Scale sample proportions to exact bucket count.
		for k, v := range localCounts {
			groupCounts[b][k] += v * int64(bucketRecords) / int64(sampled)
		}
	}
}

// timechartScanPath counts records per bucket via record scanning with optional grouping and pre-ops.
// Returns (truncated, error) where truncated is true when the 1M scan cap was hit.
func (e *Engine) timechartScanPath(ctx context.Context, q Query, preOps []querylang.PipeOp, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, hasGroupBy, hasPreOps bool, counts []int64, groupCounts []map[string]int64) (bool, error) {
	orderBy := q.OrderBy
	q.Limit = 0
	iter, _ := e.Search(ctx, q, nil)

	if hasPreOps {
		return false, timechartScanPreOps(ctx, iter, preOps, e.lookupResolver, orderBy, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
	}
	return timechartScanDirect(iter, orderBy, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
}

// timechartScanPreOps applies pipeline pre-ops then bins the resulting records.
func timechartScanPreOps(ctx context.Context, iter iter.Seq2[chunk.Record, error], preOps []querylang.PipeOp, resolve lookup.Resolver, orderBy OrderBy, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, hasGroupBy bool, counts []int64, groupCounts []map[string]int64) error {
	records, err := applyRecordOps(ctx, iter, preOps, resolve)
	if err != nil {
		return err
	}
	for _, rec := range records {
		timechartBinRecord(orderBy.RecordTS(rec), rec.Attrs, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
	}
	return nil
}

// timechartScanDirect iterates records directly and bins them, capped at 1M records.
// Returns (truncated, error) where truncated is true when the cap was hit.
func timechartScanDirect(records iter.Seq2[chunk.Record, error], orderBy OrderBy, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, hasGroupBy bool, counts []int64, groupCounts []map[string]int64) (bool, error) {
	const maxScan = 1_000_000
	scanned := 0
	for rec, err := range records {
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return false, err
		}
		timechartBinRecord(orderBy.RecordTS(rec), rec.Attrs, start, end, bucketWidth, numBuckets, groupField, hasGroupBy, counts, groupCounts)
		scanned++
		if scanned >= maxScan {
			return true, nil
		}
	}
	return false, nil
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

// findIngestPos returns the earliest record position with IngestTS >= ts.
// Tries chunk manager first (active chunk B-tree), then index manager
// (sealed chunk on-disk binary search). Both are O(log n).
func findIngestPos(cm chunk.ChunkManager, im index.IndexManager, chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	if pos, found, err := cm.FindIngestStartPosition(chunkID, ts); err == nil && found {
		return pos, true
	}
	if im != nil {
		if pos, found, err := im.FindIngestStartPosition(chunkID, ts); err == nil && found {
			return pos, true
		}
	}
	return 0, false
}

// timechartChunkByIngestTS counts records per bucket using IngestTS binary search.
// Active chunks: chunk manager's FindIngestStartPosition (in-memory B-tree).
// Sealed chunks: index manager's FindIngestStartPosition (on-disk binary search).
// Both are O(buckets × log(n)) with no heap allocation beyond stack buffers.
//
// For chunks without an ingest index (e.g., cloud-backed chunks), falls back to
// linear interpolation: assumes records are evenly distributed across the chunk's
// IngestStart → IngestEnd range.
func timechartChunkByIngestTS(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	numBuckets int,
	counts []int64,
	cloudFlags []bool,
) {
	end := start.Add(bucketWidth * time.Duration(numBuckets))

	firstBucket := 0
	if !meta.IngestStart.IsZero() && meta.IngestStart.After(start) {
		firstBucket = int(meta.IngestStart.Sub(start) / bucketWidth)
		if firstBucket >= numBuckets {
			return
		}
	}
	lastBucket := numBuckets - 1
	if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(end) {
		lastBucket = int(meta.IngestEnd.Sub(start) / bucketWidth)
		if lastBucket >= numBuckets {
			lastBucket = numBuckets - 1
		}
	}

	// Cloud chunks: mark buckets as having cloud data. Don't fetch the
	// TS index from S3 just for histogram bar counts — that latency adds
	// up across many chunks. The TS index is for search-time seeking.
	if meta.CloudBacked {
		for b := firstBucket; b <= lastBucket; b++ {
			if cloudFlags != nil {
				cloudFlags[b] = true
			}
		}
		return
	}

	// Try index-based counting for local chunks.
	if _, ok := findIngestPos(cm, im, meta.ID, start); ok {
		timechartChunkByIndex(cm, im, meta, start, bucketWidth, firstBucket, lastBucket, counts)
		return
	}

	// No ingest index available — mark buckets as having cloud data.
	for b := firstBucket; b <= lastBucket; b++ {
		if cloudFlags != nil {
			cloudFlags[b] = true
		}
	}
}

// timechartChunkByIndex counts records per bucket using binary search on the ingest index.
func timechartChunkByIndex(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	firstBucket, lastBucket int,
	counts []int64,
) {
	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))

		startPos, _ := findIngestPos(cm, im, meta.ID, bStart)

		var endPos uint64
		if !meta.IngestEnd.IsZero() && !bEnd.Before(meta.IngestEnd) {
			endPos = uint64(meta.RecordCount) //nolint:gosec // G115: RecordCount is always non-negative
		} else if pos, ok := findIngestPos(cm, im, meta.ID, bEnd); ok {
			endPos = pos
		}

		if endPos > startPos {
			counts[b] += int64(endPos - startPos)
		}
	}
}

