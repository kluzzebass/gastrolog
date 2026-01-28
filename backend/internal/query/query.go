// Package query provides a query engine that sits above chunk and index
// managers. It owns query semantics: selecting chunks, using indexes,
// driving cursors, merging results, and enforcing limits.
package query

import (
	"context"
	"errors"
	"iter"
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

// Search returns an iterator over records matching the query, ordered by ingest timestamp.
// The iterator yields (record, nil) for each match, or (zero, err) on error.
// After yielding an error, iteration stops.
func (e *Engine) Search(ctx context.Context, q Query) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		metas, err := e.chunks.List()
		if err != nil {
			yield(chunk.Record{}, err)
			return
		}

		candidates := e.selectChunks(metas, q)

		count := 0
		for _, meta := range candidates {
			if err := ctx.Err(); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			for rec, err := range e.searchChunk(ctx, q, meta) {
				if err != nil {
					yield(chunk.Record{}, err)
					return
				}

				if !yield(rec, nil) {
					return
				}

				count++
				if q.Limit > 0 && count >= q.Limit {
					return
				}
			}
		}
	}
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

// searchChunk returns an iterator over records in a single chunk.
// Unsealed chunks are scanned sequentially without indexes.
func (e *Engine) searchChunk(ctx context.Context, q Query, meta chunk.ChunkMeta) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		cursor, err := e.chunks.OpenCursor(meta.ID)
		if err != nil {
			yield(chunk.Record{}, err)
			return
		}
		defer cursor.Close()

		if !meta.Sealed {
			for rec, err := range e.scanSequential(cursor, q) {
				if err != nil {
					yield(chunk.Record{}, err)
					return
				}
				if !yield(rec, nil) {
					return
				}
			}
			return
		}

		// Source filter: if set, get positions from source index.
		var sourcePositions []uint64
		if q.Source != nil {
			srcIdx, err := e.indexes.OpenSourceIndex(meta.ID)
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}
			reader := index.NewSourceIndexReader(meta.ID, srcIdx.Entries())
			positions, found := reader.Lookup(*q.Source)
			if !found {
				return // source not in this chunk
			}
			sourcePositions = positions
		}

		// Time index: find start position.
		var seekRef chunk.RecordRef
		var hasSeek bool
		if !q.Start.IsZero() {
			timeIdx, err := e.indexes.OpenTimeIndex(meta.ID)
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}
			reader := index.NewTimeIndexReader(meta.ID, timeIdx.Entries())
			seekRef, hasSeek = reader.FindStart(q.Start)
		}

		if hasSeek {
			if err := cursor.Seek(seekRef); err != nil {
				yield(chunk.Record{}, err)
				return
			}
		}

		var scanner iter.Seq2[chunk.Record, error]
		if q.Source != nil {
			scanner = e.scanByPositions(cursor, q, meta.ID, seekRef, hasSeek, sourcePositions)
		} else {
			scanner = e.scanSequential(cursor, q)
		}

		for rec, err := range scanner {
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
		}
	}
}

// scanSequential reads records sequentially from the cursor, applying all filters.
func (e *Engine) scanSequential(cursor chunk.RecordCursor, q Query) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		for {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}

			if !q.Start.IsZero() && rec.IngestTS.Before(q.Start) {
				continue
			}
			if !q.End.IsZero() && !rec.IngestTS.Before(q.End) {
				return
			}
			if q.Source != nil && rec.SourceID != *q.Source {
				continue
			}

			if !yield(rec, nil) {
				return
			}
		}
	}
}

// scanByPositions seeks to specific positions from the source index,
// applying time filters to each record.
func (e *Engine) scanByPositions(cursor chunk.RecordCursor, q Query, chunkID chunk.ChunkID, seekRef chunk.RecordRef, hasSeek bool, positions []uint64) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		// Filter positions to those at or after the time index start position.
		startIdx := 0
		if hasSeek {
			startIdx = sort.Search(len(positions), func(i int) bool {
				return positions[i] >= seekRef.Pos
			})
		}

		for _, pos := range positions[startIdx:] {
			ref := chunk.RecordRef{ChunkID: chunkID, Pos: pos}
			if err := cursor.Seek(ref); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}

			if !q.Start.IsZero() && rec.IngestTS.Before(q.Start) {
				continue
			}
			if !q.End.IsZero() && !rec.IngestTS.Before(q.End) {
				return
			}

			if !yield(rec, nil) {
				return
			}
		}
	}
}
