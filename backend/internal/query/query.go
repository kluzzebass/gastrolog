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

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/token"
)

// Query describes what records to search for.
type Query struct {
	// Time bounds (if End < Start, results are returned in reverse/newest-first order)
	Start time.Time // inclusive bound (lower for forward, upper for reverse)
	End   time.Time // exclusive bound (upper for forward, lower for reverse)

	// Optional filters
	Sources []chunk.SourceID // filter by sources (nil = no filter, OR semantics)
	Tokens  []string         // filter by tokens (nil = no filter, AND semantics)

	// Result control
	Limit int // max results (0 = unlimited)

	// Context windows (for SearchWithContext)
	ContextBefore int // number of records to include before each match
	ContextAfter  int // number of records to include after each match
}

// Reverse returns true if this query should return results in reverse (newest-first) order.
func (q Query) Reverse() bool {
	return !q.Start.IsZero() && !q.End.IsZero() && q.End.Before(q.Start)
}

// TimeBounds returns the effective lower and upper time bounds, accounting for reverse order.
// For forward: lower=Start, upper=End
// For reverse: lower=End, upper=Start
func (q Query) TimeBounds() (lower, upper time.Time) {
	if q.Reverse() {
		return q.End, q.Start
	}
	return q.Start, q.End
}

// ResumeToken allows resuming a query from where it left off.
// Next refers to the first record that has NOT yet been returned.
// Tokens are valid as long as the referenced chunk exists.
type ResumeToken struct {
	Next chunk.RecordRef
}

// ErrInvalidResumeToken is returned when a resume token references a chunk that no longer exists.
var ErrInvalidResumeToken = errors.New("invalid resume token: chunk no longer exists")

// recordWithRef combines a record with its reference for internal iteration.
type recordWithRef struct {
	Record chunk.Record
	Ref    chunk.RecordRef
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
//
// The resume parameter allows continuing from a previous search. Pass nil to start fresh.
// The returned nextToken function returns a ResumeToken if iteration stopped early
// (limit reached, caller break, error, or context cancellation), or nil if all
// matching records were returned.
func (e *Engine) Search(ctx context.Context, q Query, resume *ResumeToken) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	// Track state for resume token generation.
	var nextRef *chunk.RecordRef
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
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

		// Skip chunks before resume position.
		startChunkIdx := 0
		if resume != nil {
			found := false
			for i, meta := range candidates {
				if meta.ID == resume.Next.ChunkID {
					startChunkIdx = i
					found = true
					break
				}
			}
			if !found {
				yield(chunk.Record{}, ErrInvalidResumeToken)
				return
			}
		}

		count := 0
		for i := startChunkIdx; i < len(candidates); i++ {
			meta := candidates[i]

			if err := ctx.Err(); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			// Determine start position within this chunk.
			var startPos *uint64
			if resume != nil && i == startChunkIdx {
				startPos = &resume.Next.Pos
			}

			for rr, err := range e.searchChunkWithRef(ctx, q, meta, startPos) {
				if err != nil {
					// Track position for potential retry.
					nextRef = &rr.Ref
					yield(chunk.Record{}, err)
					return
				}

				// Track next position before yielding.
				nextRef = &rr.Ref

				if !yield(rr.Record, nil) {
					return
				}

				count++
				if q.Limit > 0 && count >= q.Limit {
					return
				}
			}
		}

		// Iteration completed fully.
		completed = true
	}

	nextToken := func() *ResumeToken {
		if completed || nextRef == nil {
			return nil
		}
		return &ResumeToken{Next: *nextRef}
	}

	return seq, nextToken
}

// SearchThenFollow finds the first record matching the query, then yields all
// subsequent records (ignoring source and token filters) until End, limit, or EOF.
//
// This is useful for "find error, then show everything after" use cases.
// The source and token filters only apply to finding the first match.
// Time bounds and limit still apply to all yielded records.
func (e *Engine) SearchThenFollow(ctx context.Context, q Query, resume *ResumeToken) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	var nextRef *chunk.RecordRef
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
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

		startChunkIdx := 0
		if resume != nil {
			found := false
			for i, meta := range candidates {
				if meta.ID == resume.Next.ChunkID {
					startChunkIdx = i
					found = true
					break
				}
			}
			if !found {
				yield(chunk.Record{}, ErrInvalidResumeToken)
				return
			}
		}

		// Create a follow query that removes source/token filters.
		followQuery := q
		followQuery.Sources = nil
		followQuery.Tokens = nil

		count := 0
		inFollowMode := false
		var followFromRef *chunk.RecordRef

		// Phase 1: Find the first match using the filtered query.
		for i := startChunkIdx; i < len(candidates) && !inFollowMode; i++ {
			meta := candidates[i]

			if err := ctx.Err(); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			var startPos *uint64
			if resume != nil && i == startChunkIdx {
				startPos = &resume.Next.Pos
			}

			for rr, err := range e.searchChunkWithRef(ctx, q, meta, startPos) {
				if err != nil {
					nextRef = &rr.Ref
					yield(chunk.Record{}, err)
					return
				}

				nextRef = &rr.Ref

				if !yield(rr.Record, nil) {
					return
				}

				count++
				if q.Limit > 0 && count >= q.Limit {
					return
				}

				// Found first match - switch to follow mode.
				inFollowMode = true
				followFromRef = &rr.Ref
				break
			}
		}

		if !inFollowMode {
			// No match found.
			completed = true
			return
		}

		// Phase 2: Follow mode - continue from where we left off without filters.
		// Find which chunk contains the follow point.
		followChunkIdx := -1
		for i, meta := range candidates {
			if meta.ID == followFromRef.ChunkID {
				followChunkIdx = i
				break
			}
		}
		if followChunkIdx < 0 {
			completed = true
			return
		}

		// Continue from the follow position.
		for i := followChunkIdx; i < len(candidates); i++ {
			meta := candidates[i]

			if err := ctx.Err(); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			var startPos *uint64
			if i == followChunkIdx {
				startPos = &followFromRef.Pos
			}

			for rr, err := range e.searchChunkWithRef(ctx, followQuery, meta, startPos) {
				if err != nil {
					nextRef = &rr.Ref
					yield(chunk.Record{}, err)
					return
				}

				nextRef = &rr.Ref

				if !yield(rr.Record, nil) {
					return
				}

				count++
				if q.Limit > 0 && count >= q.Limit {
					return
				}
			}
		}

		completed = true
	}

	nextToken := func() *ResumeToken {
		if completed || nextRef == nil {
			return nil
		}
		return &ResumeToken{Next: *nextRef}
	}

	return seq, nextToken
}

// SearchWithContext finds records matching the query and includes surrounding
// context records. For each match, it yields ContextBefore records before the
// match, the match itself, and ContextAfter records after the match.
//
// Context records are yielded in timestamp order (oldest first for forward,
// newest first for reverse). Context gathering may cross chunk boundaries.
//
// Note: This method buffers context records in memory. For large context windows,
// consider using SearchThenFollow or manual cursor operations instead.
func (e *Engine) SearchWithContext(ctx context.Context, q Query) (iter.Seq2[chunk.Record, error], func() *ResumeToken) {
	var nextRef *chunk.RecordRef
	completed := false

	seq := func(yield func(chunk.Record, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(chunk.Record{}, err)
			return
		}

		metas, err := e.chunks.List()
		if err != nil {
			yield(chunk.Record{}, err)
			return
		}

		// Sort all chunks by StartTS for consistent ordering.
		allChunks := e.selectChunks(metas, q)

		// Also need all chunks sorted ascending for context gathering.
		allChunksAsc := make([]chunk.ChunkMeta, len(metas))
		copy(allChunksAsc, metas)
		slices.SortFunc(allChunksAsc, func(a, b chunk.ChunkMeta) int {
			return a.StartTS.Compare(b.StartTS)
		})

		count := 0
		for i := 0; i < len(allChunks); i++ {
			meta := allChunks[i]

			if err := ctx.Err(); err != nil {
				yield(chunk.Record{}, err)
				return
			}

			for rr, err := range e.searchChunkWithRef(ctx, q, meta, nil) {
				if err != nil {
					nextRef = &rr.Ref
					yield(chunk.Record{}, err)
					return
				}

				// Gather context before (in iteration order).
				// For forward: before = chronologically older records
				// For reverse: before = chronologically newer records
				if q.ContextBefore > 0 {
					var beforeRecs []chunk.Record
					var err error
					if q.Reverse() {
						// In reverse mode, "before" in iteration order means chronologically after.
						beforeRecs, err = e.gatherContextAfter(ctx, allChunksAsc, rr.Ref, q.ContextBefore, true)
					} else {
						beforeRecs, err = e.gatherContextBefore(ctx, allChunksAsc, rr.Ref, q.ContextBefore, false)
					}
					if err != nil {
						yield(chunk.Record{}, err)
						return
					}
					for _, rec := range beforeRecs {
						if !yield(rec, nil) {
							nextRef = &rr.Ref
							return
						}
						count++
						if q.Limit > 0 && count >= q.Limit {
							nextRef = &rr.Ref
							return
						}
					}
				}

				// Yield the match.
				nextRef = &rr.Ref
				if !yield(rr.Record, nil) {
					return
				}
				count++
				if q.Limit > 0 && count >= q.Limit {
					return
				}

				// Gather context after (in iteration order).
				// For forward: after = chronologically newer records
				// For reverse: after = chronologically older records
				if q.ContextAfter > 0 {
					var afterRecs []chunk.Record
					var err error
					if q.Reverse() {
						// In reverse mode, "after" in iteration order means chronologically before.
						afterRecs, err = e.gatherContextBefore(ctx, allChunksAsc, rr.Ref, q.ContextAfter, true)
					} else {
						afterRecs, err = e.gatherContextAfter(ctx, allChunksAsc, rr.Ref, q.ContextAfter, false)
					}
					if err != nil {
						yield(chunk.Record{}, err)
						return
					}
					for _, rec := range afterRecs {
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

		completed = true
	}

	nextToken := func() *ResumeToken {
		if completed || nextRef == nil {
			return nil
		}
		return &ResumeToken{Next: *nextRef}
	}

	return seq, nextToken
}

// gatherContextBefore collects up to n records before the anchor position.
// Returns records in the order they should be yielded (oldest first for forward,
// newest first for reverse).
func (e *Engine) gatherContextBefore(ctx context.Context, chunksAsc []chunk.ChunkMeta, anchor chunk.RecordRef, n int, reverse bool) ([]chunk.Record, error) {
	if n <= 0 {
		return nil, nil
	}

	// Find the chunk containing the anchor.
	chunkIdx := -1
	for i, m := range chunksAsc {
		if m.ID == anchor.ChunkID {
			chunkIdx = i
			break
		}
	}
	if chunkIdx < 0 {
		return nil, nil // chunk not found, no context
	}

	var collected []chunk.Record

	// Start from anchor chunk, walk backward.
	for ci := chunkIdx; ci >= 0 && len(collected) < n; ci-- {
		meta := chunksAsc[ci]

		cursor, err := e.chunks.OpenCursor(meta.ID)
		if err != nil {
			return nil, err
		}

		if ci == chunkIdx {
			// Seek to anchor position.
			if err := cursor.Seek(anchor); err != nil {
				cursor.Close()
				return nil, err
			}
		} else {
			// Seek to end of previous chunk.
			if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: uint64(meta.Size)}); err != nil {
				cursor.Close()
				return nil, err
			}
		}

		// Walk backward collecting records.
		for len(collected) < n {
			rec, _, err := cursor.Prev()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				cursor.Close()
				return nil, err
			}
			collected = append(collected, rec)
		}

		cursor.Close()
	}

	// Reverse so oldest is first (for forward mode).
	// For reverse mode, we want newest first, so reverse again.
	slices.Reverse(collected)
	if reverse {
		slices.Reverse(collected)
	}

	return collected, nil
}

// gatherContextAfter collects up to n records after the anchor position.
// Returns records in the order they should be yielded (oldest first for forward,
// newest first for reverse).
func (e *Engine) gatherContextAfter(ctx context.Context, chunksAsc []chunk.ChunkMeta, anchor chunk.RecordRef, n int, reverse bool) ([]chunk.Record, error) {
	if n <= 0 {
		return nil, nil
	}

	// Find the chunk containing the anchor.
	chunkIdx := -1
	for i, m := range chunksAsc {
		if m.ID == anchor.ChunkID {
			chunkIdx = i
			break
		}
	}
	if chunkIdx < 0 {
		return nil, nil // chunk not found, no context
	}

	var collected []chunk.Record

	// Start from anchor chunk, walk forward.
	for ci := chunkIdx; ci < len(chunksAsc) && len(collected) < n; ci++ {
		meta := chunksAsc[ci]

		cursor, err := e.chunks.OpenCursor(meta.ID)
		if err != nil {
			return nil, err
		}

		if ci == chunkIdx {
			// Seek to anchor position and skip it.
			if err := cursor.Seek(anchor); err != nil {
				cursor.Close()
				return nil, err
			}
			// Skip the anchor record itself.
			if _, _, err := cursor.Next(); err != nil && !errors.Is(err, chunk.ErrNoMoreRecords) {
				cursor.Close()
				return nil, err
			}
		}
		// For subsequent chunks, cursor starts at beginning by default.

		// Walk forward collecting records.
		for len(collected) < n {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				cursor.Close()
				return nil, err
			}
			collected = append(collected, rec)
		}

		cursor.Close()
	}

	// For reverse mode, we want newest first.
	if reverse {
		slices.Reverse(collected)
	}

	return collected, nil
}

// selectChunks filters to chunks that overlap the query time range,
// sorted by StartTS (ascending for forward, descending for reverse).
// Unsealed chunks are always included (their EndTS is not final).
func (e *Engine) selectChunks(metas []chunk.ChunkMeta, q Query) []chunk.ChunkMeta {
	lower, upper := q.TimeBounds()

	var out []chunk.ChunkMeta
	for _, m := range metas {
		if m.Sealed {
			// Chunk must overlap [lower, upper)
			if !lower.IsZero() && m.EndTS.Before(lower) {
				continue
			}
			if !upper.IsZero() && !m.StartTS.Before(upper) {
				continue
			}
		}
		out = append(out, m)
	}
	if q.Reverse() {
		slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
			return b.StartTS.Compare(a.StartTS) // descending
		})
	} else {
		slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
			return a.StartTS.Compare(b.StartTS) // ascending
		})
	}
	return out
}

// searchChunkWithRef returns an iterator over records in a single chunk, including their refs.
// startPos allows resuming from a specific position within the chunk.
// Unsealed chunks are scanned sequentially without indexes.
func (e *Engine) searchChunkWithRef(ctx context.Context, q Query, meta chunk.ChunkMeta, startPos *uint64) iter.Seq2[recordWithRef, error] {
	return func(yield func(recordWithRef, error) bool) {
		cursor, err := e.chunks.OpenCursor(meta.ID)
		if err != nil {
			yield(recordWithRef{}, err)
			return
		}
		defer cursor.Close()

		// Handle resume position.
		if startPos != nil {
			if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: *startPos}); err != nil {
				yield(recordWithRef{}, err)
				return
			}
			// Skip the record at startPos - it was already returned before the break.
			// For forward: call Next() to move past resume position.
			// For reverse: cursor.Prev() decrements before returning, so seeking to
			// the resume position is sufficient - the first Prev() will skip it.
			if !q.Reverse() {
				if _, _, err := cursor.Next(); err != nil && !errors.Is(err, chunk.ErrNoMoreRecords) {
					yield(recordWithRef{}, err)
					return
				}
			}
		} else if q.Reverse() {
			// For reverse without resume, seek to end of chunk.
			if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: uint64(meta.Size)}); err != nil {
				yield(recordWithRef{}, err)
				return
			}
		}

		if !meta.Sealed {
			var scanner iter.Seq2[recordWithRef, error]
			if q.Reverse() {
				scanner = e.scanSequentialReverseWithRef(cursor, q, meta.ID)
			} else {
				scanner = e.scanSequentialWithRef(cursor, q, meta.ID)
			}
			for rr, err := range scanner {
				if err != nil {
					yield(rr, err)
					return
				}
				if !yield(rr, nil) {
					return
				}
			}
			return
		}

		// Time index: find start position first to prune posting lists.
		// For forward queries, we seek to Start (lower bound).
		// For reverse queries, we seek to End (which is the lower bound).
		lower, _ := q.TimeBounds()
		var seekRef chunk.RecordRef
		var hasSeek bool
		if !lower.IsZero() {
			timeIdx, err := e.indexes.OpenTimeIndex(meta.ID)
			if err != nil {
				yield(recordWithRef{}, err)
				return
			}
			reader := index.NewTimeIndexReader(meta.ID, timeIdx.Entries())
			seekRef, hasSeek = reader.FindStart(lower)
		}

		// If we have a resume position, use the larger of time index and resume.
		if startPos != nil {
			if !hasSeek || *startPos > seekRef.Pos {
				seekRef = chunk.RecordRef{ChunkID: meta.ID, Pos: *startPos}
				hasSeek = true
			}
		}

		// Token filter: if set, get positions from token index and intersect.
		var tokenPositions []uint64
		var hasTokenFilter bool
		if len(q.Tokens) > 0 {
			hasTokenFilter = true
			tokIdx, err := e.indexes.OpenTokenIndex(meta.ID)
			if err != nil {
				yield(recordWithRef{}, err)
				return
			}
			reader := index.NewTokenIndexReader(meta.ID, tokIdx.Entries())

			// Look up each token and intersect positions.
			for i, tok := range q.Tokens {
				positions, found := reader.Lookup(tok)
				if !found {
					return // token not in this chunk, no matches possible
				}
				// Prune positions before start.
				if hasSeek {
					positions = prunePositions(positions, seekRef.Pos)
					if len(positions) == 0 {
						return // no positions after start
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
				yield(recordWithRef{}, err)
				return
			}
			reader := index.NewSourceIndexReader(meta.ID, srcIdx.Entries())

			// Union positions from all requested sources.
			for _, src := range q.Sources {
				positions, found := reader.Lookup(src)
				if found {
					// Prune positions before start.
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

		// If resuming, exclude the exact resume position (already returned).
		if startPos != nil && len(finalPositions) > 0 {
			if q.Reverse() {
				// In reverse, resume position is at the end of the list we care about.
				if finalPositions[len(finalPositions)-1] == *startPos {
					finalPositions = finalPositions[:len(finalPositions)-1]
					if len(finalPositions) == 0 {
						return // no more positions before resume point
					}
				}
			} else {
				// In forward, resume position is at the start.
				if finalPositions[0] == *startPos {
					finalPositions = finalPositions[1:]
					if len(finalPositions) == 0 {
						return // no more positions after resume point
					}
				}
			}
		}

		if hasSeek && startPos == nil && !q.Reverse() {
			// Only seek if we haven't already seeked for resume (forward only).
			if err := cursor.Seek(seekRef); err != nil {
				yield(recordWithRef{}, err)
				return
			}
		}

		var scanner iter.Seq2[recordWithRef, error]
		if hasPositionFilter {
			if q.Reverse() {
				scanner = e.scanByPositionsReverseWithRef(cursor, q, meta.ID, finalPositions)
			} else {
				scanner = e.scanByPositionsWithRef(cursor, q, meta.ID, finalPositions)
			}
		} else {
			if q.Reverse() {
				scanner = e.scanSequentialReverseWithRef(cursor, q, meta.ID)
			} else {
				scanner = e.scanSequentialWithRef(cursor, q, meta.ID)
			}
		}

		for rr, err := range scanner {
			if err != nil {
				yield(rr, err)
				return
			}
			if !yield(rr, nil) {
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

// scanSequentialWithRef reads records sequentially from the cursor, applying all filters.
func (e *Engine) scanSequentialWithRef(cursor chunk.RecordCursor, q Query, chunkID chunk.ChunkID) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()

	return func(yield func(recordWithRef, error) bool) {
		for {
			rec, ref, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(recordWithRef{Ref: ref}, err)
				return
			}

			if !lower.IsZero() && rec.IngestTS.Before(lower) {
				continue
			}
			if !upper.IsZero() && !rec.IngestTS.Before(upper) {
				return
			}
			if len(q.Sources) > 0 && !slices.Contains(q.Sources, rec.SourceID) {
				continue
			}
			if !matchesTokens(rec.Raw, q.Tokens) {
				continue
			}

			if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
				return
			}
		}
	}
}

// scanByPositionsWithRef seeks to specific positions, applying time filters to each record.
// Positions are assumed to already be pruned to >= time start.
func (e *Engine) scanByPositionsWithRef(cursor chunk.RecordCursor, q Query, chunkID chunk.ChunkID, positions []uint64) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()

	return func(yield func(recordWithRef, error) bool) {
		for _, pos := range positions {
			ref := chunk.RecordRef{ChunkID: chunkID, Pos: pos}
			if err := cursor.Seek(ref); err != nil {
				yield(recordWithRef{Ref: ref}, err)
				return
			}

			rec, ref, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(recordWithRef{Ref: ref}, err)
				return
			}

			if !lower.IsZero() && rec.IngestTS.Before(lower) {
				continue
			}
			if !upper.IsZero() && !rec.IngestTS.Before(upper) {
				return
			}

			if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
				return
			}
		}
	}
}

// scanSequentialReverseWithRef reads records in reverse from the cursor, applying all filters.
func (e *Engine) scanSequentialReverseWithRef(cursor chunk.RecordCursor, q Query, chunkID chunk.ChunkID) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()

	return func(yield func(recordWithRef, error) bool) {
		for {
			rec, ref, err := cursor.Prev()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(recordWithRef{Ref: ref}, err)
				return
			}

			// In reverse, we stop when we go below lower bound (too old).
			if !lower.IsZero() && rec.IngestTS.Before(lower) {
				return
			}
			// In reverse, we skip records at or after upper bound (too new).
			if !upper.IsZero() && !rec.IngestTS.Before(upper) {
				continue
			}
			if len(q.Sources) > 0 && !slices.Contains(q.Sources, rec.SourceID) {
				continue
			}
			if !matchesTokens(rec.Raw, q.Tokens) {
				continue
			}

			if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
				return
			}
		}
	}
}

// scanByPositionsReverseWithRef seeks to specific positions in reverse order.
func (e *Engine) scanByPositionsReverseWithRef(cursor chunk.RecordCursor, q Query, chunkID chunk.ChunkID, positions []uint64) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()

	return func(yield func(recordWithRef, error) bool) {
		// Iterate positions in reverse order.
		for i := len(positions) - 1; i >= 0; i-- {
			pos := positions[i]
			ref := chunk.RecordRef{ChunkID: chunkID, Pos: pos}
			if err := cursor.Seek(ref); err != nil {
				yield(recordWithRef{Ref: ref}, err)
				return
			}

			rec, ref, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(recordWithRef{Ref: ref}, err)
				return
			}

			// In reverse, we stop when we go below lower bound.
			if !lower.IsZero() && rec.IngestTS.Before(lower) {
				return
			}
			// In reverse, we skip records at or after upper bound.
			if !upper.IsZero() && !rec.IngestTS.Before(upper) {
				continue
			}

			if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
				return
			}
		}
	}
}
