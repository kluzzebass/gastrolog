// Package query provides a query engine that sits above chunk and index
// managers. It owns query semantics: selecting chunks, using indexes,
// driving cursors, merging results, and enforcing limits.
package query

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
)

// Query describes what records to search for.
type Query struct {
	// Time bounds
	Start time.Time // inclusive lower bound (zero = no lower bound)
	End   time.Time // exclusive upper bound (zero = no upper bound)

	// Optional filters
	Source *chunk.SourceID // filter by source (nil = no filter)

	// Result control
	Limit int // max results (0 = unlimited)
}

// Engine executes queries against chunk and index managers.
type Engine struct {
	chunks  chunk.ChunkManager
	indexes index.IndexManager
}

// New creates a query engine backed by the given chunk and index managers.
func New(chunks chunk.ChunkManager, indexes index.IndexManager) *Engine {
	return &Engine{chunks: chunks, indexes: indexes}
}

// Search returns records matching the query, ordered by ingest timestamp.
func (e *Engine) Search(ctx context.Context, q Query) ([]chunk.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	metas, err := e.chunks.List()
	if err != nil {
		return nil, err
	}

	candidates := e.selectChunks(metas, q)

	var results []chunk.Record
	for _, meta := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		remaining := 0
		if q.Limit > 0 {
			remaining = q.Limit - len(results)
			if remaining <= 0 {
				break
			}
		}

		found, err := e.searchChunk(q, meta, remaining)
		if err != nil {
			return nil, err
		}
		results = append(results, found...)
	}

	return results, nil
}

// selectChunks filters to chunks that overlap the query time range,
// sorted by StartTS ascending. Unsealed chunks are always included
// (their EndTS is not final, so time filtering is not reliable).
func (e *Engine) selectChunks(metas []chunk.ChunkMeta, q Query) []chunk.ChunkMeta {
	var out []chunk.ChunkMeta
	for _, m := range metas {
		if m.Sealed {
			if !q.Start.IsZero() && m.EndTS.Before(q.Start) {
				continue
			}
			if !q.End.IsZero() && !m.StartTS.Before(q.End) {
				continue
			}
		}
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
		return a.StartTS.Compare(b.StartTS)
	})
	return out
}

// searchChunk scans a single chunk using indexes and a cursor.
// limit is the max records to return (0 = unlimited).
// Unsealed chunks are scanned sequentially without indexes.
func (e *Engine) searchChunk(q Query, meta chunk.ChunkMeta, limit int) ([]chunk.Record, error) {
	cursor, err := e.chunks.OpenCursor(meta.ID)
	if err != nil {
		return nil, err
	}
	defer cursor.Close()

	if !meta.Sealed {
		return e.scanSequential(cursor, q, limit)
	}

	// Source filter: if set, get positions from source index.
	var sourcePositions []uint64
	if q.Source != nil {
		srcIdx, err := e.indexes.OpenSourceIndex(meta.ID)
		if err != nil {
			return nil, err
		}
		reader := index.NewSourceIndexReader(meta.ID, srcIdx.Entries())
		positions, found := reader.Lookup(*q.Source)
		if !found {
			return nil, nil // source not in this chunk
		}
		sourcePositions = positions
	}

	// Time index: find start position.
	var seekRef chunk.RecordRef
	var hasSeek bool
	if !q.Start.IsZero() {
		timeIdx, err := e.indexes.OpenTimeIndex(meta.ID)
		if err != nil {
			return nil, err
		}
		reader := index.NewTimeIndexReader(meta.ID, timeIdx.Entries())
		seekRef, hasSeek = reader.FindStart(q.Start)
	}

	if hasSeek {
		if err := cursor.Seek(seekRef); err != nil {
			return nil, err
		}
	}

	if q.Source != nil {
		return e.scanByPositions(cursor, q, meta.ID, seekRef, hasSeek, sourcePositions, limit)
	}
	return e.scanSequential(cursor, q, limit)
}

// scanSequential reads records sequentially from the cursor, applying all filters.
func (e *Engine) scanSequential(cursor chunk.RecordCursor, q Query, limit int) ([]chunk.Record, error) {
	var results []chunk.Record
	for {
		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return nil, err
		}

		if !q.Start.IsZero() && rec.IngestTS.Before(q.Start) {
			continue
		}
		if !q.End.IsZero() && !rec.IngestTS.Before(q.End) {
			break
		}
		if q.Source != nil && rec.SourceID != *q.Source {
			continue
		}

		results = append(results, rec)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	return results, nil
}

// scanByPositions seeks to specific positions from the source index,
// applying time filters to each record.
func (e *Engine) scanByPositions(cursor chunk.RecordCursor, q Query, chunkID chunk.ChunkID, seekRef chunk.RecordRef, hasSeek bool, positions []uint64, limit int) ([]chunk.Record, error) {
	// Filter positions to those at or after the time index start position.
	startIdx := 0
	if hasSeek {
		startIdx = sort.Search(len(positions), func(i int) bool {
			return positions[i] >= seekRef.Pos
		})
	}

	var results []chunk.Record
	for _, pos := range positions[startIdx:] {
		ref := chunk.RecordRef{ChunkID: chunkID, Pos: pos}
		if err := cursor.Seek(ref); err != nil {
			return nil, err
		}

		rec, _, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			break
		}
		if err != nil {
			return nil, err
		}

		if !q.Start.IsZero() && rec.IngestTS.Before(q.Start) {
			continue
		}
		if !q.End.IsZero() && !rec.IngestTS.Before(q.End) {
			break
		}

		results = append(results, rec)
		if limit > 0 && len(results) >= limit {
			break
		}
	}
	return results, nil
}
