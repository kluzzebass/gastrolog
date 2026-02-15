package query

import (
	"context"
	"errors"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// HistogramQuery describes what to compute.
type HistogramQuery struct {
	Query      Query // Time bounds, filters, store filter
	NumBuckets int
}

// HistogramResult holds bucketed counts.
type HistogramResult struct {
	Start       time.Time
	End         time.Time
	Counts      []int64
	LevelCounts []map[string]int64
}

// severityLevels are the canonical severity names we track in histograms.
var severityLevels = []string{"error", "warn", "info", "debug", "trace"}

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

// Histogram returns record counts bucketed by time.
//
// Two modes:
//   - Unfiltered (no tokens, kv, severity): uses FindStartPosition binary search
//     on idx.log for O(buckets * log(n)) per chunk. Very fast.
//   - Filtered: runs the full query engine search (unlimited) and buckets each
//     matching record. Capped at 1M records to prevent runaway scans.
func (e *Engine) Histogram(ctx context.Context, hq HistogramQuery) (*HistogramResult, error) {
	q := hq.Query
	numBuckets := hq.NumBuckets
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

	// Normalize: histogram always needs lower < upper regardless of query direction.
	start, end := q.Start, q.End
	if !start.IsZero() && !end.IsZero() && end.Before(start) {
		start, end = end, start
	}
	if start.IsZero() || end.IsZero() || !start.Before(end) {
		return &HistogramResult{}, nil
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

	if hasFilter {
		// Filtered path: scan matching records and bucket them.
		q.Limit = 0
		iter, _ := e.Search(ctx, q, nil)
		const maxScan = 1_000_000
		scanned := 0
		for rec, err := range iter {
			if err != nil {
				if errors.Is(err, context.Canceled) {
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
			scanned++
			if scanned >= maxScan {
				break
			}
		}
	} else {
		// Unfiltered path: use FindStartPosition for O(buckets * log(n)).
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
				histogramChunkFast(cm, meta, start, bucketWidth, numBuckets, counts)
				if meta.Sealed {
					histogramChunkSeverity(cm, im, meta, start, bucketWidth, numBuckets, levelCounts)
				}
			}
		}
	}

	return &HistogramResult{
		Start:       start,
		End:         end,
		Counts:      counts,
		LevelCounts: levelCounts,
	}, nil
}

// histogramChunkFast counts records per bucket using binary search on idx.log.
// O(buckets * log(n)) per chunk, no record scanning.
func histogramChunkFast(
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

// histogramChunkSeverity populates per-level counts for a chunk using KV indexes.
// Only works on sealed, indexed chunks.
func histogramChunkSeverity(
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
		// AttrKVIndex uses AttrKVIndexEntry, but we need KVIndexReader.
		// Convert entries.
		attrEntries := attrKVIdx.Entries()
		kvEntries := make([]index.KVIndexEntry, len(attrEntries))
		for i, e := range attrEntries {
			kvEntries[i] = index.KVIndexEntry(e)
		}
		attrKVReader = index.NewKVIndexReader(meta.ID, kvEntries)
	}

	for _, level := range severityLevels {
		// Collect positions from both KV and attr KV indexes.
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

		// Deduplicate positions (a record might match both level=error and severity_name=error).
		slices.Sort(allPositions)
		allPositions = slices.Compact(allPositions)

		// Read WriteTS for each position and bucket them.
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
