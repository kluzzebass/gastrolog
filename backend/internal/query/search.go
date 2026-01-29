package query

import (
	"context"
	"iter"
	"slices"

	"gastrolog/internal/chunk"
)

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
