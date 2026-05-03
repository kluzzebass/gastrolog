package query

import (
	"context"
	"errors"
	"gastrolog/internal/glid"
	"iter"
	"maps"
	"slices"
	"strconv"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/lookup"
	"gastrolog/internal/querylang"
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

	acc := &histogramAccum{
		counts:      counts,
		groupCounts: groupCounts,
		cloudFlags:  make([]bool, numBuckets),
		cloudCounts: make([]int64, numBuckets),
	}
	truncated, err := e.runTimechartStrategy(ctx, q, preOps, selectedVaults,
		start, end, bucketWidth, numBuckets,
		hasFilter, hasPreOps, hasGroupBy, groupField,
		acc)
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
// histogramAccum collects per-bucket counts during histogram computation.
type histogramAccum struct {
	counts      []int64
	groupCounts []map[string]int64
	cloudFlags  []bool
	cloudCounts []int64 // unfiltered cloud contribution per bucket
}

func (e *Engine) runTimechartStrategy(
	ctx context.Context, q Query, preOps []querylang.PipeOp, selectedVaults []glid.GLID,
	start, end time.Time, bucketWidth time.Duration, numBuckets int,
	hasFilter, hasPreOps, hasGroupBy bool, groupField string,
	acc *histogramAccum,
) (bool, error) {
	cloudFlags := acc.cloudFlags
	cloudCounts := acc.cloudCounts
	counts := acc.counts
	groupCounts := acc.groupCounts

	if hasFilter || hasPreOps {
		// Compute unfiltered counts for cloud chunks.
		hasCloud := e.timechartCloudCounts(selectedVaults, start, end, bucketWidth, numBuckets, cloudCounts, cloudFlags)

		// Scan LOCAL chunks only with the filter applied (skip cloud blobs).
		localQ := q
		localQ.SkipCloud = true
		truncated, err := e.timechartScanPath(ctx, localQ, preOps, start, end, bucketWidth,
			numBuckets, groupField, hasGroupBy, hasPreOps, counts, groupCounts)
		if err != nil {
			return truncated, err
		}

		if hasCloud {
			e.applyCloudSelectivity(selectedVaults, start, end, bucketWidth, numBuckets, counts, cloudCounts)
		}

		return truncated, nil
	}

	// Exact total counts via IngestTS binary search — O(buckets × log(n)), instant.
	// Cloud counts are computed separately first so cloudCounts[] captures the split.
	e.timechartCloudCounts(selectedVaults, start, end, bucketWidth, numBuckets, cloudCounts, cloudFlags)
	// Sealed-only counts here: active non-monotonic chunks are handled by
	// the unified pass below (which keeps counts and groupCounts on the same
	// B+ tree snapshot). For !hasGroupBy queries we still need their counts,
	// so the unified pass updates counts even when there's no level breakdown.
	e.timechartLocalCounts(selectedVaults, start, end, bucketWidth, numBuckets, counts, true /*sealedOnly*/)
	for b := range numBuckets {
		counts[b] += cloudCounts[b]
	}

	// Active non-monotonic chunks: single pass that computes both counts
	// and (when grouping) groupCounts from the SAME B+ tree snapshot.
	// Without this, counts and groupCounts come from two separate B+ tree
	// iterations at different times, and on a fast-growing active chunk
	// the second pass sees thousands more records — making groupCounts
	// exceed counts and breaking the "other" remainder. See gastrolog-66b7x.
	e.timechartActiveNonMonotonic(selectedVaults, start, end, bucketWidth, numBuckets, hasGroupBy, groupField, counts, groupCounts)

	if !hasGroupBy {
		return false, nil
	}

	// Group breakdown for sealed local + cached cloud chunks via per-bucket
	// sampling — O(buckets × 1000). Active non-monotonic chunks already
	// contributed via the unified pass above and are skipped here.
	e.timechartAttrScanGroups(selectedVaults, start, end, bucketWidth, numBuckets, groupField, groupCounts)
	return false, nil
}

// applyCloudSelectivity estimates filtered cloud counts using local data selectivity.
// Computes filteredLocal/localTotal ratio and scales cloud counts by that factor.
func (e *Engine) applyCloudSelectivity(selectedVaults []glid.GLID, start, end time.Time, bucketWidth time.Duration, numBuckets int, counts, cloudCounts []int64) {
	localTotals := make([]int64, numBuckets)
	e.timechartLocalCounts(selectedVaults, start, end, bucketWidth, numBuckets, localTotals, false)

	var localTotal, filteredLocal int64
	for b := range numBuckets {
		localTotal += localTotals[b]
		filteredLocal += counts[b]
	}
	if localTotal > 0 {
		selectivity := float64(filteredLocal) / float64(localTotal)
		for b := range numBuckets {
			if cloudCounts[b] > 0 {
				estimated := int64(float64(cloudCounts[b]) * selectivity)
				counts[b] += estimated
				cloudCounts[b] = estimated
			}
		}
	} else {
		for b := range numBuckets {
			counts[b] += cloudCounts[b]
		}
	}
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

// vaultChunkMetas returns every chunk's metadata for the given vault:
// sealed-chunk entries projected from the manifest Reader (FSM truth),
// plus the active chunk taken directly from the chunk manager (the active
// chunk is the documented exception to manifest-as-source-of-truth — its
// running maxima would balloon the Raft log if replicated). Falls back
// to cm.List() in single-vault legacy mode where no registry is wired.
//
// Returns nil if the vault has no chunk manager.
func (e *Engine) vaultChunkMetas(vaultID glid.GLID) []chunk.ChunkMeta {
	cm := e.chunks
	if e.registry != nil {
		cm = e.registry.ChunkManager(vaultID)
	}
	if cm == nil {
		return nil
	}
	if e.registry == nil {
		metas, _ := cm.List()
		return metas
	}
	entries := e.registry.Reader().EntriesForVault(vaultID)
	out := make([]chunk.ChunkMeta, 0, len(entries)+1)
	for i := range entries {
		out = append(out, entries[i].ToChunkMeta())
	}
	if active := cm.Active(); active != nil {
		out = append(out, *active)
	}
	return out
}

// timechartVaults returns the vaults to query for a timechart, applying any vault filter.
func (e *Engine) timechartVaults(q Query) []glid.GLID {
	allVaults := e.listVaults()
	if q.BoolExpr != nil {
		if vaults, _ := ExtractVaultFilter(q.BoolExpr, allVaults); vaults != nil {
			return vaults
		}
	}
	return allVaults
}

// deriveTimeRange fills in missing Start/End from chunk metadata across the selected vaults.
func (e *Engine) deriveTimeRange(q *Query, selectedVaults []glid.GLID) {
	for _, vaultID := range selectedVaults {
		cm, _ := e.getVaultManagers(vaultID)
		if cm == nil {
			continue
		}
		metas := e.vaultChunkMetas(vaultID)
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
func (e *Engine) timechartFastPath(selectedVaults []glid.GLID, start time.Time, end time.Time, bucketWidth time.Duration, numBuckets int, counts []int64, cloudFlags []bool) {
	for _, vaultID := range selectedVaults {
		cm, im := e.getVaultManagers(vaultID)
		if cm == nil {
			continue
		}
		metas := e.vaultChunkMetas(vaultID)
		streamed := e.transitionStreamedChunks(vaultID)
		for _, meta := range metas {
			if streamed[meta.ID] {
				continue
			}
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

// timechartCloudCounts fills cloudCounts with unfiltered record counts from
// cloud-backed chunks only (via TS index binary search). Sets cloudFlags for
// buckets with cloud data. Returns true if any cloud chunks were found.
func (e *Engine) timechartCloudCounts(selectedVaults []glid.GLID, start, end time.Time, bucketWidth time.Duration, numBuckets int, cloudCounts []int64, cloudFlags []bool) bool {
	found := false
	for _, vaultID := range selectedVaults {
		cm, im := e.getVaultManagers(vaultID)
		if cm == nil {
			continue
		}
		metas := e.vaultChunkMetas(vaultID)
		streamed := e.transitionStreamedChunks(vaultID)
		for _, meta := range metas {
			if streamed[meta.ID] {
				continue
			}
			if !meta.CloudBacked || meta.RecordCount == 0 {
				continue
			}
			if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(start) {
				continue
			}
			if !meta.IngestStart.IsZero() && !meta.IngestStart.Before(end) {
				continue
			}
			found = true
			timechartChunkByIngestTS(cm, im, meta, start, bucketWidth, numBuckets, cloudCounts, cloudFlags)
		}
	}
	return found
}

// timechartLocalCounts fills counts with unfiltered record counts from
// local (non-cloud) chunks only. When sealedOnly is true, active
// non-monotonic chunks are skipped — they're handled by the unified
// timechartActiveNonMonotonic pass that keeps counts and groupCounts on
// the same B+ tree snapshot. See gastrolog-66b7x.
func (e *Engine) timechartLocalCounts(selectedVaults []glid.GLID, start, end time.Time, bucketWidth time.Duration, numBuckets int, counts []int64, sealedOnly bool) {
	for _, vaultID := range selectedVaults {
		cm, im := e.getVaultManagers(vaultID)
		if cm == nil {
			continue
		}
		metas := e.vaultChunkMetas(vaultID)
		streamed := e.transitionStreamedChunks(vaultID)
		for _, meta := range metas {
			if streamed[meta.ID] {
				continue
			}
			if meta.CloudBacked || meta.RecordCount == 0 {
				continue
			}
			if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(start) {
				continue
			}
			if !meta.IngestStart.IsZero() && !meta.IngestStart.Before(end) {
				continue
			}
			if sealedOnly && !meta.Sealed && !meta.IngestTSMonotonic {
				continue
			}
			timechartChunkByIngestTS(cm, im, meta, start, bucketWidth, numBuckets, counts, nil)
		}
	}
}

// timechartActiveNonMonotonic iterates each active non-monotonic chunk's
// B+ tree once, populating both counts and (when hasGroupBy) groupCounts
// from the SAME snapshot. Active chunks grow during the histogram compute,
// so a two-pass approach (counts via bucketizeActiveChunk, then groupCounts
// via chunkBucketTotals) sees inconsistent record-count snapshots across
// the passes — making leveled sums exceed bucket totals and zeroing the
// "other" remainder. See gastrolog-66b7x.
func (e *Engine) timechartActiveNonMonotonic(selectedVaults []glid.GLID, start, end time.Time, bucketWidth time.Duration, numBuckets int, hasGroupBy bool, groupField string, counts []int64, groupCounts []map[string]int64) {
	startNanos := start.UnixNano()
	bucketNanos := bucketWidth.Nanoseconds()
	if bucketNanos <= 0 {
		return
	}
	for _, vaultID := range selectedVaults {
		cm, _ := e.getVaultManagers(vaultID)
		if cm == nil {
			continue
		}
		metas := e.vaultChunkMetas(vaultID)
		streamed := e.transitionStreamedChunks(vaultID)
		for _, meta := range metas {
			if !activeNonMonoEligible(meta, streamed, start, end) {
				continue
			}
			scanActiveNonMono(cm, meta, startNanos, bucketNanos, numBuckets, hasGroupBy, groupField, counts, groupCounts)
		}
	}
}

func activeNonMonoEligible(meta chunk.ChunkMeta, streamed map[chunk.ChunkID]bool, start, end time.Time) bool {
	if streamed[meta.ID] {
		return false
	}
	if meta.CloudBacked || meta.RecordCount == 0 {
		return false
	}
	if meta.Sealed || meta.IngestTSMonotonic {
		return false
	}
	if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(start) {
		return false
	}
	if !meta.IngestStart.IsZero() && !meta.IngestStart.Before(end) {
		return false
	}
	return true
}

func scanActiveNonMono(cm chunk.ChunkManager, meta chunk.ChunkMeta, startNanos, bucketNanos int64, numBuckets int, hasGroupBy bool, groupField string, counts []int64, groupCounts []map[string]int64) {
	if hasGroupBy {
		_ = cm.ScanActiveByIngestTS(meta.ID, func(ingestTS time.Time, attrs chunk.Attributes) bool {
			b, ok := bucketForTS(ingestTS.UnixNano(), startNanos, bucketNanos, numBuckets)
			if !ok {
				return b >= 0 // skip-this-record vs stop-iteration
			}
			counts[b]++
			if v := attrs[groupField]; v != "" {
				groupCounts[b][v]++
			}
			return true
		})
		return
	}
	_ = cm.ScanActiveIngestTS(meta.ID, func(tsNanos int64) bool {
		b, ok := bucketForTS(tsNanos, startNanos, bucketNanos, numBuckets)
		if !ok {
			return b >= 0
		}
		counts[b]++
		return true
	})
}

// bucketForTS returns (bucketIdx, true) for an in-range TS. For out-of-range
// TS it returns (sentinel, false): -1 = past-end (caller should stop iteration),
// numBuckets = pre-start (caller should skip this record but keep iterating).
func bucketForTS(tsNanos, startNanos, bucketNanos int64, numBuckets int) (int, bool) {
	if tsNanos < startNanos {
		return numBuckets, false // pre-start: skip but keep going
	}
	b := int((tsNanos - startNanos) / bucketNanos)
	if b >= numBuckets {
		return -1, false // past end: stop
	}
	return b, true
}

// timechartAttrScanGroups populates group breakdown counts using per-bucket
// sampling. For each bucket, binary search finds the record position range,
// then ScanAttrs reads up to samplePerBucket attrs and scales the proportions
// to the exact count. Total cost: O(buckets × samplePerBucket) regardless of
// dataset size (~50K records for default 50 buckets).
// Does NOT update total counts — those come from timechartFastPath.
func (e *Engine) timechartAttrScanGroups(selectedVaults []glid.GLID, start, end time.Time, bucketWidth time.Duration, numBuckets int, groupField string, groupCounts []map[string]int64) {
	const samplePerBucket = 1000

	for _, vaultID := range selectedVaults {
		cm, im := e.getVaultManagers(vaultID)
		if cm == nil {
			continue
		}
		metas := e.vaultChunkMetas(vaultID)
		streamed := e.transitionStreamedChunks(vaultID)
		for _, meta := range metas {
			if streamed[meta.ID] {
				continue
			}
			if meta.RecordCount == 0 {
				continue
			}
			if !meta.IngestEnd.IsZero() && meta.IngestEnd.Before(start) {
				continue
			}
			if !meta.IngestStart.IsZero() && !meta.IngestStart.Before(end) {
				continue
			}
			// Skip chunks whose content isn't locally readable — for cloud
			// chunks that means "blob is not in the warm cache." We never
			// trigger an S3 download just to compute the level breakdown;
			// histogram refreshes that span 30d would otherwise pull
			// hundreds of cloud blobs. Cloud chunks still contribute
			// accurate counts via the TS index, and the bucket renders as
			// a hatched "data here, breakdown not loaded" ghost via the
			// cloudFlags overlay. If the same chunk gets cached later
			// (because a real search needs its records) it'll
			// automatically pick up a real level breakdown on the next
			// histogram refresh. See gastrolog-66b7x and gastrolog-20z6h.
			if !cm.HasLocalContent(meta.ID) {
				continue
			}
			// Active non-monotonic chunks: handled by the unified pass in
			// runTimechartStrategy (timechartActiveNonMonotonic) so counts
			// and groupCounts come from the same B+ tree snapshot. See
			// gastrolog-66b7x.
			if !meta.Sealed && !meta.IngestTSMonotonic {
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

	// Defensive: chunk meta can be transiently inconsistent during a seal /
	// transition (IngestStart vs IngestEnd applied in separate Raft entries).
	// A negative-length bucket range would crash makeslice downstream.
	if firstBucket > lastBucket {
		return
	}

	// Non-monotonic chunks (active or sealed): the per-bucket sampler below
	// reads records in physical (append) order, which is unrelated to the
	// IngestTS bucket they belong to. Running it for every bucket the chunk
	// overlaps just multiplies cost without improving accuracy — the sample
	// is effectively a random slice of the chunk regardless. See
	// gastrolog-66b7x.
	if !meta.IngestTSMonotonic {
		nonMonotonicChunkGroups(cm, im, meta, start, bucketWidth, firstBucket, lastBucket, groupField, groupCounts)
		return
	}

	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))

		startPos, startOK := findIngestPos(cm, im, meta.ID, bStart)
		if !startOK {
			continue
		}

		// Use rank arithmetic for the bucket count (correct on
		// non-monotonic chunks); use position for ScanAttrs offset
		// (the cursor needs a physical record position). See
		// gastrolog-66b7x.
		startRank, rankOK := findIngestRank(cm, im, meta.ID, bStart)
		if !rankOK {
			continue
		}

		var endRank uint64
		if !meta.IngestEnd.IsZero() && !bEnd.Before(meta.IngestEnd) {
			endRank = uint64(meta.RecordCount) //nolint:gosec // G115: RecordCount is always non-negative
		} else if rank, ok := findIngestRank(cm, im, meta.ID, bEnd); ok {
			endRank = rank
		}

		if endRank <= startRank {
			continue
		}
		bucketRecords := endRank - startRank

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
//
// Returns the *physical record position* — correct for cursor positioning
// (e.g. ScanAttrs from this offset). NOT correct for histogram counting on
// non-monotonic chunks; use findIngestRank for bucket counts. See gastrolog-66b7x.
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

// findIngestRank returns the entry index (rank) in the IngestTS-sorted index
// of the first entry with IngestTS >= ts. For active chunks (monotonic
// IngestTS via Append), rank == physical position. For sealed chunks built
// via ImportRecords, rank differs from position because physical layout
// follows source-WriteTS, not IngestTS — histogram bucket counts must use
// rank arithmetic (endRank - startRank), not position arithmetic. See
// gastrolog-66b7x.
func findIngestRank(cm chunk.ChunkManager, im index.IndexManager, chunkID chunk.ChunkID, ts time.Time) (uint64, bool) {
	// Cloud-backed chunks and active monotonic chunks: rank comes from
	// the chunk manager. For active monotonic chunks position == rank.
	// For cloud chunks the entry index in the sorted cache file is the
	// correct rank. See gastrolog-66b7x.
	if rank, found, err := cm.FindIngestEntryIndex(chunkID, ts); err == nil && found {
		return rank, true
	}
	// Sealed local chunks: rank lives on the on-disk TS index file.
	if im != nil {
		if rank, found, err := im.FindIngestEntryIndex(chunkID, ts); err == nil && found {
			return rank, true
		}
	}
	return 0, false
}

// timechartChunkByIngestTS counts records per bucket using IngestTS binary search.
// Active chunks: chunk manager's FindIngestStartPosition (in-memory B-tree).
// Sealed chunks: index manager's FindIngestStartPosition (on-disk binary search).
// Both are O(buckets × log(n)) with no heap allocation beyond stack buffers.
//
// Active non-monotonic chunks fall through to bucketizeActiveChunk, which
// scans the in-memory B+ tree (rank arithmetic isn't available there). All
// other chunks — sealed local, sealed cloud-backed, monotonic active — go
// through timechartChunkByIndex against the IngestTS index. With FSM-as-
// source-of-truth (gastrolog-2pw28) the index is guaranteed to exist for
// any chunk the FSM exposes; transient lookup failures are real errors,
// not histogram artifacts.
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

	// IngestStart / IngestEnd track the FIRST and LAST *appended* records'
	// IngestTS, NOT min/max of IngestTS within the chunk. For non-monotonic
	// chunks (records appended out of TS order — common for cloud-backed
	// chunks built via ImportRecords or for tier-2+ destinations receiving
	// streamed records), IngestEnd can be earlier than IngestStart, and the
	// chunk's true TS range can extend beyond [IngestStart, IngestEnd] in
	// either direction. Clamp by min/max instead of treating Start/End
	// literally, and widen the search range to cover everything the chunk
	// could plausibly hold.
	clampLo, clampHi := meta.IngestStart, meta.IngestEnd
	if !clampLo.IsZero() && !clampHi.IsZero() && clampLo.After(clampHi) {
		clampLo, clampHi = clampHi, clampLo
	}

	firstBucket := 0
	if !clampLo.IsZero() && clampLo.After(start) {
		firstBucket = int(clampLo.Sub(start) / bucketWidth)
		if firstBucket >= numBuckets {
			return
		}
	}
	lastBucket := numBuckets - 1
	if !clampHi.IsZero() && clampHi.Before(end) {
		lastBucket = int(clampHi.Sub(start) / bucketWidth)
		if lastBucket >= numBuckets {
			lastBucket = numBuckets - 1
		}
	}

	// Mark cloud buckets regardless of TS index availability —
	// the flag means "this bucket includes cloud data" (for hatching),
	// not "counts are unknown."
	if meta.CloudBacked && cloudFlags != nil {
		for b := firstBucket; b <= lastBucket; b++ {
			cloudFlags[b] = true
		}
	}

	// Non-monotonic ACTIVE chunks need a full B+ tree iteration: physical
	// position doesn't match IngestTS-sorted rank, and we can't get rank
	// from the in-memory B+ tree without iterating. Sealed non-monotonic
	// chunks have a sorted on-disk TS index — rank arithmetic works there
	// via FindIngestEntryIndex. Monotonic chunks (active or sealed) keep
	// the fast O(buckets × log N) path because position == rank. See
	// gastrolog-66b7x.
	if !meta.Sealed && !meta.IngestTSMonotonic {
		bucketizeActiveChunk(cm, meta, start, bucketWidth, firstBucket, lastBucket, counts)
		return
	}

	// Index-based counting: rank arithmetic on the IngestTS-sorted index
	// (on-disk for sealed chunks, B+ tree for monotonic active chunks,
	// cached local file for cloud-backed sealed chunks). The FSM has
	// already promised an index exists for this chunk.
	timechartChunkByIndex(cm, im, meta, start, bucketWidth, firstBucket, lastBucket, counts)
}

// bucketizeActiveChunk iterates the active chunk's records once and
// increments the corresponding bucket for each record. Required for
// non-monotonic active chunks (tier 2+ destinations receiving streamed
// records out of IngestTS order); the rank-arithmetic path is unsafe
// there because cm.FindIngestStartPosition returns physical position,
// not rank. See gastrolog-66b7x.
func bucketizeActiveChunk(
	cm chunk.ChunkManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	firstBucket, lastBucket int,
	counts []int64,
) {
	startNanos := start.UnixNano()
	bucketNanos := bucketWidth.Nanoseconds()
	if bucketNanos <= 0 {
		return
	}
	_ = cm.ScanActiveIngestTS(meta.ID, func(tsNanos int64) bool {
		if tsNanos < startNanos {
			return true
		}
		b := int((tsNanos - startNanos) / bucketNanos)
		if b < firstBucket {
			return true
		}
		if b > lastBucket {
			return false // entries are sorted, no further match
		}
		counts[b]++
		return true
	})
}

// nonMonotonicChunkGroups computes the level breakdown for a non-monotonic
// chunk by sampling once at the chunk level (capped at sampleCap records),
// computing per-bucket totals from the IngestTS index (B+ tree for active,
// on-disk index for sealed), and applying the chunk-level level
// proportions to each bucket. Cost: O(sampleCap + buckets × log N) instead
// of O(buckets × sampleCap). See gastrolog-66b7x.
func nonMonotonicChunkGroups(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	firstBucket, lastBucket int,
	groupField string,
	groupCounts []map[string]int64,
) {
	const sampleCap = 1000
	levelCounts := make(map[string]int64)
	sampled := 0
	_ = cm.ScanAttrs(meta.ID, 0, func(_ time.Time, attrs chunk.Attributes) bool {
		if v := attrs[groupField]; v != "" {
			levelCounts[v]++
		}
		sampled++
		return sampled < sampleCap
	})
	if sampled == 0 {
		return
	}
	ratios := make(map[string]float64, len(levelCounts))
	for k, v := range levelCounts {
		ratios[k] = float64(v) / float64(sampled)
	}
	bucketTotals := chunkBucketTotals(cm, im, meta, start, bucketWidth, firstBucket, lastBucket)
	for b := firstBucket; b <= lastBucket; b++ {
		total := bucketTotals[b-firstBucket]
		if total == 0 {
			continue
		}
		for k, r := range ratios {
			groupCounts[b][k] += int64(float64(total) * r)
		}
	}
}

// chunkBucketTotals returns per-bucket record counts for a chunk over the
// requested bucket range. Uses B+ tree iteration for active chunks (where
// rank cannot be computed via random access) and rank arithmetic on the
// on-disk TS index for sealed chunks. See gastrolog-66b7x.
func chunkBucketTotals(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	firstBucket, lastBucket int,
) []int64 {
	if lastBucket < firstBucket {
		return nil
	}
	totals := make([]int64, lastBucket-firstBucket+1)
	if !meta.Sealed {
		startNanos := start.UnixNano()
		bucketNanos := bucketWidth.Nanoseconds()
		_ = cm.ScanActiveIngestTS(meta.ID, func(tsNanos int64) bool {
			if tsNanos < startNanos {
				return true
			}
			b := int((tsNanos - startNanos) / bucketNanos)
			if b < firstBucket {
				return true
			}
			if b > lastBucket {
				return false
			}
			totals[b-firstBucket]++
			return true
		})
		return totals
	}
	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))
		startRank, _ := findIngestRank(cm, im, meta.ID, bStart)
		var endRank uint64
		if !meta.IngestEnd.IsZero() && !bEnd.Before(meta.IngestEnd) {
			endRank = uint64(meta.RecordCount) //nolint:gosec // G115: RecordCount is non-negative
		} else if rank, ok := findIngestRank(cm, im, meta.ID, bEnd); ok {
			endRank = rank
		}
		if endRank > startRank {
			totals[b-firstBucket] = int64(endRank - startRank)
		}
	}
	return totals
}

// timechartChunkByIndex counts records per bucket using binary search on the
// ingest index. Counts come from rank arithmetic in the IngestTS-sorted index
// — endRank - startRank — because physical record positions in chunks built
// via ImportRecords are scattered relative to IngestTS order. See
// gastrolog-66b7x.
//
// Cloud chunks whose local IngestTS index isn't cached fall through to a
// proportional FSM-based estimate: distribute meta.RecordCount across the
// buckets the chunk overlaps in proportion to (bucket overlap / chunk span).
// Without this fallback, cloud chunks on follower nodes that haven't pulled
// the index file silently contribute zero to the histogram even though the
// search itself can stream the records — the vault inspector reports N
// records but the histogram shows N/2 because every cloud chunk drops out.
//
// We can't probe the index up front: findIngestRank at the chunk's
// IngestStart returns (0, true) for a healthy index AND for a missing
// index (rank zero is the natural answer at the chunk's earliest record),
// so a single probe can't distinguish the two. Instead, run rank arithmetic
// across all buckets first; if the total contribution is zero despite the
// chunk having records, the index isn't actually serving lookups and we
// fall back to overlap-based distribution.
func timechartChunkByIndex(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	firstBucket, lastBucket int,
	counts []int64,
) {
	if firstBucket > lastBucket {
		return
	}
	// IngestStart/IngestEnd track first/last appended records, not
	// min/max IngestTS. For non-monotonic chunks the IngestEnd-fallthrough
	// shortcut needs to compare against max(IngestStart, IngestEnd) to fire
	// at the chunk's actual upper TS bound.
	clampHi := meta.IngestEnd
	if !meta.IngestStart.IsZero() && (clampHi.IsZero() || meta.IngestStart.After(clampHi)) {
		clampHi = meta.IngestStart
	}

	// Fast path: probe once before the per-bucket loop. If neither cm nor
	// im can resolve the chunk's index at any TS, the rank-arithmetic loop
	// would do 50×2 failed lookups per chunk — at ~50µs per failed
	// loadIngestTSMmap open() syscall, that's ~5ms per chunk × ~1900 chunks
	// = ~10s of pure syscall overhead on a `last=12h` query. The probe
	// distinguishes "working index that returns rank 0" (ok=true) from
	// "no index reachable" (ok=false) cleanly via the boolean — only the
	// rank value is ambiguous, not ok. On a miss we skip straight to
	// FSM-based distribution.
	probeTS := meta.IngestStart
	if probeTS.IsZero() {
		probeTS = clampHi
	}
	if !probeTS.IsZero() {
		if _, ok := findIngestRank(cm, im, meta.ID, probeTS); !ok {
			distributeChunkRecordsByOverlap(meta, start, bucketWidth, firstBucket, lastBucket, counts)
			return
		}
	}

	rankCounts := make([]int64, lastBucket-firstBucket+1)
	var rankTotal int64
	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))

		startRank, _ := findIngestRank(cm, im, meta.ID, bStart)

		var endRank uint64
		if !clampHi.IsZero() && !bEnd.Before(clampHi) {
			endRank = uint64(meta.RecordCount) //nolint:gosec // G115: RecordCount is always non-negative
		} else if rank, ok := findIngestRank(cm, im, meta.ID, bEnd); ok {
			endRank = rank
		}

		if endRank > startRank {
			delta := int64(endRank - startRank)
			rankCounts[b-firstBucket] = delta
			rankTotal += delta
		}
	}
	if rankTotal < meta.RecordCount {
		// Rank arithmetic under-counted the chunk. Three known causes:
		//   1. Local index unreachable (cloud chunk whose index file
		//      isn't cached on this node) — every per-bucket lookup
		//      returns (0, false).
		//   2. lastBucket was clamped at numBuckets-1 because the chunk
		//      extends past the histogram window — the upper-bound
		//      fallthrough gate fails for every bucket.
		//   3. Non-monotonic chunk where IngestStart/IngestEnd are
		//      first/last *appended* records' TS rather than min/max,
		//      so the bucket clamping in timechartChunkByIngestTS lands
		//      a range that doesn't fully cover the chunk's records.
		// In all three the FSM still tells us how many records the
		// chunk holds — fall back to overlap-based distribution so they
		// show up in the histogram. Was the production gap "vault
		// inspector reports N, histogram shows ~N/2".
		distributeChunkRecordsByOverlap(meta, start, bucketWidth, firstBucket, lastBucket, counts)
		return
	}
	for i, c := range rankCounts {
		counts[firstBucket+i] += c
	}
}

// distributeChunkRecordsByOverlap spreads meta.RecordCount across the
// histogram buckets the chunk overlaps, in proportion to the time overlap
// between each bucket and [meta.IngestStart, meta.IngestEnd]. Used when the
// IngestTS rank index isn't locally resolvable (typically cloud chunks on
// followers without a cached index file) — without this, the chunk silently
// contributes zero, breaking the invariant "histogram total ≈ vault total".
func distributeChunkRecordsByOverlap(
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	firstBucket, lastBucket int,
	counts []int64,
) {
	if meta.RecordCount == 0 || meta.IngestStart.IsZero() || meta.IngestEnd.IsZero() {
		return
	}
	span := meta.IngestEnd.Sub(meta.IngestStart)
	if span <= 0 {
		// Degenerate single-instant chunk: dump the count into the bucket
		// containing IngestStart, if it falls in range.
		offset := meta.IngestStart.Sub(start)
		if offset < 0 {
			return
		}
		b := int(offset / bucketWidth)
		if b < firstBucket || b > lastBucket {
			return
		}
		counts[b] += meta.RecordCount
		return
	}
	for b := firstBucket; b <= lastBucket; b++ {
		bStart := start.Add(bucketWidth * time.Duration(b))
		bEnd := start.Add(bucketWidth * time.Duration(b+1))
		ovStart := bStart
		if meta.IngestStart.After(ovStart) {
			ovStart = meta.IngestStart
		}
		ovEnd := bEnd
		if meta.IngestEnd.Before(ovEnd) {
			ovEnd = meta.IngestEnd
		}
		overlap := ovEnd.Sub(ovStart)
		if overlap <= 0 {
			continue
		}
		// int64 arithmetic preserves rounding sanity; floor is fine because
		// any rounding loss is bounded by O(buckets).
		counts[b] += meta.RecordCount * int64(overlap) / int64(span)
	}
}
