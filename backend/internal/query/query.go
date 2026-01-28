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
	"github.com/kluzzebass/gastrolog/internal/index/token"
)

// Query describes what records to search for.
type Query struct {
	// Time bounds
	Start time.Time // inclusive lower bound (zero = no lower bound)
	End   time.Time // exclusive upper bound (zero = no upper bound)

	// Optional filters
	Sources []chunk.SourceID // filter by sources (nil = no filter, OR semantics)
	Tokens  []string         // filter by tokens (nil = no filter, AND semantics)

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

		// Time index: find start position first to prune posting lists.
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

		// Token filter: if set, get positions from token index and intersect.
		var tokenPositions []uint64
		var hasTokenFilter bool
		if len(q.Tokens) > 0 {
			hasTokenFilter = true
			tokIdx, err := e.indexes.OpenTokenIndex(meta.ID)
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}
			reader := index.NewTokenIndexReader(meta.ID, tokIdx.Entries())

			// Look up each token and intersect positions.
			for i, tok := range q.Tokens {
				positions, found := reader.Lookup(tok)
				if !found {
					return // token not in this chunk, no matches possible
				}
				// Prune positions before time start.
				if hasSeek {
					positions = prunePositions(positions, seekRef.Pos)
					if len(positions) == 0 {
						return // no positions after time start
					}
				}
				if i == 0 {
					tokenPositions = positions
				} else {
					tokenPositions = intersectPositions(tokenPositions, positions)
					if len(tokenPositions) == 0 {
						return // no records contain all tokens
					}
				}
			}
		}

		// Source filter: if set, get positions from source index (OR semantics).
		var sourcePositions []uint64
		var hasSourceFilter bool
		if len(q.Sources) > 0 {
			hasSourceFilter = true
			srcIdx, err := e.indexes.OpenSourceIndex(meta.ID)
			if err != nil {
				yield(chunk.Record{}, err)
				return
			}
			reader := index.NewSourceIndexReader(meta.ID, srcIdx.Entries())

			// Union positions from all requested sources.
			for _, src := range q.Sources {
				positions, found := reader.Lookup(src)
				if found {
					// Prune positions before time start.
					if hasSeek {
						positions = prunePositions(positions, seekRef.Pos)
					}
					sourcePositions = unionPositions(sourcePositions, positions)
				}
			}
			if len(sourcePositions) == 0 {
				return // no requested sources in this chunk
			}
		}

		// Combine token and source positions if both are set.
		var finalPositions []uint64
		hasPositionFilter := hasTokenFilter || hasSourceFilter
		if hasTokenFilter && hasSourceFilter {
			finalPositions = intersectPositions(tokenPositions, sourcePositions)
			if len(finalPositions) == 0 {
				return // no records match both filters
			}
		} else if hasTokenFilter {
			finalPositions = tokenPositions
		} else if hasSourceFilter {
			finalPositions = sourcePositions
		}

		if hasSeek {
			if err := cursor.Seek(seekRef); err != nil {
				yield(chunk.Record{}, err)
				return
			}
		}

		var scanner iter.Seq2[chunk.Record, error]
		if hasPositionFilter {
			scanner = e.scanByPositions(cursor, q, meta.ID, finalPositions)
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

// prunePositions returns positions >= minPos from a sorted slice.
func prunePositions(positions []uint64, minPos uint64) []uint64 {
	idx := sort.Search(len(positions), func(i int) bool {
		return positions[i] >= minPos
	})
	return positions[idx:]
}

// intersectPositions returns positions present in both sorted slices.
func intersectPositions(a, b []uint64) []uint64 {
	var result []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			i++
		} else {
			j++
		}
	}
	return result
}

// unionPositions returns all unique positions from both sorted slices, in sorted order.
func unionPositions(a, b []uint64) []uint64 {
	result := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		if a[i] == b[j] {
			result = append(result, a[i])
			i++
			j++
		} else if a[i] < b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)
	return result
}

// matchesTokens checks if the record's raw data contains all query tokens.
func matchesTokens(raw []byte, queryTokens []string) bool {
	if len(queryTokens) == 0 {
		return true
	}
	recordTokens := token.Simple(raw)
	tokenSet := make(map[string]bool, len(recordTokens))
	for _, t := range recordTokens {
		tokenSet[t] = true
	}
	for _, qt := range queryTokens {
		if !tokenSet[qt] {
			return false
		}
	}
	return true
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
			if len(q.Sources) > 0 && !slices.Contains(q.Sources, rec.SourceID) {
				continue
			}
			if !matchesTokens(rec.Raw, q.Tokens) {
				continue
			}

			if !yield(rec, nil) {
				return
			}
		}
	}
}

// scanByPositions seeks to specific positions, applying time filters to each record.
// Positions are assumed to already be pruned to >= time start.
func (e *Engine) scanByPositions(cursor chunk.RecordCursor, q Query, chunkID chunk.ChunkID, positions []uint64) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		for _, pos := range positions {
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
