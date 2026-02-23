package query

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/querylang"
)

// severityLevels are the canonical severity names we track in timecharts.
var severityLevels = []string{"error", "warn", "info", "debug", "trace"}

// classifySeverity maps a raw value to a canonical severity level.
// Handles values set by the level digester (already normalized) and
// raw syslog severity_name values that bypass the digester.
func classifySeverity(v string) string {
	switch strings.ToLower(v) {
	case "error", "err", "fatal", "critical", "emerg", "emergency", "alert", "crit":
		return "error"
	case "warn", "warning":
		return "warn"
	case "info", "notice", "informational":
		return "info"
	case "debug":
		return "debug"
	case "trace":
		return "trace"
	}
	return ""
}

// severityFromRecord returns the canonical severity level for a record
// based on its attributes. The level digester sets a normalized "level"
// attribute at ingest time; syslog sets "severity_name" directly.
func severityFromRecord(rec chunk.Record) string {
	for _, key := range []string{"level", "severity_name"} {
		if v, ok := rec.Attrs[key]; ok {
			if level := classifySeverity(v); level != "" {
				return level
			}
		}
	}
	return ""
}

// severityKVLookups maps canonical severity names to the key=value pairs to look up
// in the KV index (extracted from message text, e.g. level=error).
var severityKVLookups = map[string][][2]string{
	"error": {{"level", "error"}, {"level", "err"}},
	"warn":  {{"level", "warn"}, {"level", "warning"}},
	"info":  {{"level", "info"}},
	"debug": {{"level", "debug"}},
	"trace": {{"level", "trace"}},
}

// severityAttrKVLookups maps canonical severity names to the key=value pairs to look up
// in the attr KV index (from record attributes, e.g. severity_name=error).
var severityAttrKVLookups = map[string][][2]string{
	"error": {{"severity_name", "err"}, {"severity_name", "error"}},
	"warn":  {{"severity_name", "warning"}, {"severity_name", "warn"}},
	"info":  {{"severity_name", "info"}},
	"debug": {{"severity_name", "debug"}},
	"trace": {{"severity_name", "trace"}},
}

// runTimechart executes a timechart pipeline operator. It counts records by
// time bucket with severity breakdown, using index binary search (no record
// scanning) for unfiltered queries.
//
// Two modes:
//   - No pre-ops and no filter: fast path using FindStartPosition binary search.
//     O(buckets * log(n)) per chunk. Severity uses KV index lookups.
//   - With pre-ops or filter: falls back to Search() → applyRecordOps() →
//     manual binning. Capped at 1M records.
//
// Returns a TableResult with columns ["_time", "level", "count"].
func (e *Engine) runTimechart(ctx context.Context, q Query, tc *querylang.TimechartOp, preOps []querylang.PipeOp) (*TableResult, error) {
	numBuckets := tc.N
	if numBuckets <= 0 {
		numBuckets = 50
	}
	if numBuckets > 500 {
		numBuckets = 500
	}

	// Determine which stores to query.
	allStores := e.listStores()
	selectedStores := allStores
	if q.BoolExpr != nil {
		stores, _ := ExtractStoreFilter(q.BoolExpr, allStores)
		if stores != nil {
			selectedStores = stores
		}
	}

	// If no time range, derive from chunk metadata.
	if q.Start.IsZero() || q.End.IsZero() {
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

	// Normalize: timechart always needs lower < upper regardless of query direction.
	start, end := q.Start, q.End
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return &TableResult{Columns: []string{"_time", "level", "count"}}, nil
	}

	bucketWidth := end.Sub(start) / time.Duration(numBuckets)
	if bucketWidth <= 0 {
		bucketWidth = time.Second
	}

	counts := make([]int64, numBuckets)
	levelCounts := make([]map[string]int64, numBuckets)
	for i := range levelCounts {
		levelCounts[i] = make(map[string]int64)
	}

	hasFilter := q.BoolExpr != nil
	hasPreOps := len(preOps) > 0

	if hasFilter || hasPreOps {
		// Filtered/pre-ops path: scan matching records and bucket them.
		q.Limit = 0
		iter, _ := e.Search(ctx, q, nil)

		if hasPreOps {
			records, err := applyRecordOps(iter, preOps)
			if err != nil {
				return nil, err
			}
			for _, rec := range records {
				ts := rec.WriteTS
				if ts.Before(start) || !ts.Before(end) {
					continue
				}
				idx := int(ts.Sub(start) / bucketWidth)
				if idx >= numBuckets {
					idx = numBuckets - 1
				}
				counts[idx]++
				if level := severityFromRecord(rec); level != "" {
					levelCounts[idx][level]++
				}
			}
		} else {
			const maxScan = 1_000_000
			scanned := 0
			for rec, err := range iter {
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						break
					}
					return nil, err
				}
				ts := rec.WriteTS
				if ts.Before(start) || !ts.Before(end) {
					continue
				}
				idx := int(ts.Sub(start) / bucketWidth)
				if idx >= numBuckets {
					idx = numBuckets - 1
				}
				counts[idx]++
				if level := severityFromRecord(rec); level != "" {
					levelCounts[idx][level]++
				}
				scanned++
				if scanned >= maxScan {
					break
				}
			}
		}
	} else {
		// Unfiltered fast path: use FindStartPosition for O(buckets * log(n)).
		for _, storeID := range selectedStores {
			cm, im := e.getStoreManagers(storeID)
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
				if meta.Sealed {
					timechartChunkSeverity(cm, im, meta, start, bucketWidth, numBuckets, levelCounts)
				} else {
					timechartChunkSeverityScan(cm, meta, start, bucketWidth, numBuckets, levelCounts)
				}
			}
		}
	}

	return timechartToTable(start, bucketWidth, numBuckets, counts, levelCounts), nil
}

// timechartToTable converts bucketed counts into a TableResult with columns
// ["_time", "level", "count"]. Each bucket × level combination is a row.
func timechartToTable(start time.Time, bucketWidth time.Duration, numBuckets int, counts []int64, levelCounts []map[string]int64) *TableResult {
	columns := []string{"_time", "level", "count"}
	var rows [][]string

	for i := range numBuckets {
		ts := start.Add(bucketWidth * time.Duration(i)).Format(time.RFC3339Nano)
		total := counts[i]

		if len(levelCounts[i]) == 0 {
			// No severity breakdown — emit a single row with empty level.
			rows = append(rows, []string{ts, "", fmt.Sprintf("%d", total)})
			continue
		}

		// Emit one row per level that has counts.
		var levelTotal int64
		for _, level := range severityLevels {
			count, ok := levelCounts[i][level]
			if !ok || count == 0 {
				continue
			}
			rows = append(rows, []string{ts, level, fmt.Sprintf("%d", count)})
			levelTotal += count
		}
		// Emit remainder as empty-level row if total > sum of level counts.
		if remainder := total - levelTotal; remainder > 0 {
			rows = append(rows, []string{ts, "", fmt.Sprintf("%d", remainder)})
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
			endPos = uint64(meta.RecordCount)
		} else if pos, found, err := cm.FindStartPosition(meta.ID, bEnd); err == nil && found {
			endPos = pos
		}

		if endPos > startPos {
			counts[b] += int64(endPos - startPos)
		}
	}
}

// timechartChunkSeverity populates per-level counts for a chunk using KV indexes.
// Only works on sealed, indexed chunks.
func timechartChunkSeverity(
	cm chunk.ChunkManager,
	im index.IndexManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	numBuckets int,
	levelCounts []map[string]int64,
) {
	if im == nil {
		return
	}

	// Open KV index (from message text).
	kvIdx, _, err := im.OpenKVIndex(meta.ID)
	if err != nil {
		return
	}
	kvReader := index.NewKVIndexReader(meta.ID, kvIdx.Entries())

	// Open attr KV index (from record attributes).
	attrKVIdx, err := im.OpenAttrKVIndex(meta.ID)
	var attrKVReader *index.KVIndexReader
	if err == nil {
		attrEntries := attrKVIdx.Entries()
		kvEntries := make([]index.KVIndexEntry, len(attrEntries))
		for i, e := range attrEntries {
			kvEntries[i] = index.KVIndexEntry(e)
		}
		attrKVReader = index.NewKVIndexReader(meta.ID, kvEntries)
	}

	for _, level := range severityLevels {
		var allPositions []uint64

		for _, kv := range severityKVLookups[level] {
			if positions, found := kvReader.Lookup(kv[0], kv[1]); found {
				allPositions = append(allPositions, positions...)
			}
		}

		if attrKVReader != nil {
			for _, kv := range severityAttrKVLookups[level] {
				if positions, found := attrKVReader.Lookup(kv[0], kv[1]); found {
					allPositions = append(allPositions, positions...)
				}
			}
		}

		if len(allPositions) == 0 {
			continue
		}

		slices.Sort(allPositions)
		allPositions = slices.Compact(allPositions)

		timestamps, err := cm.ReadWriteTimestamps(meta.ID, allPositions)
		if err != nil {
			continue
		}

		for _, ts := range timestamps {
			if ts.Before(start) || !ts.Before(start.Add(bucketWidth*time.Duration(numBuckets))) {
				continue
			}
			idx := int(ts.Sub(start) / bucketWidth)
			if idx >= numBuckets {
				idx = numBuckets - 1
			}
			levelCounts[idx][level]++
		}
	}
}

// timechartChunkSeverityScan populates per-level counts for an unsealed chunk
// by scanning records and checking attrs.
func timechartChunkSeverityScan(
	cm chunk.ChunkManager,
	meta chunk.ChunkMeta,
	start time.Time,
	bucketWidth time.Duration,
	numBuckets int,
	levelCounts []map[string]int64,
) {
	cursor, err := cm.OpenCursor(meta.ID)
	if err != nil {
		return
	}
	defer cursor.Close()

	end := start.Add(bucketWidth * time.Duration(numBuckets))
	for {
		rec, _, err := cursor.Next()
		if err != nil {
			break
		}
		ts := rec.WriteTS
		if ts.Before(start) || !ts.Before(end) {
			continue
		}
		level := severityFromRecord(rec)
		if level == "" {
			continue
		}
		idx := int(ts.Sub(start) / bucketWidth)
		if idx >= numBuckets {
			idx = numBuckets - 1
		}
		levelCounts[idx][level]++
	}
}
