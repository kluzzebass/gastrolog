package query

import (
	"errors"
	"iter"
	"slices"
	"sort"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/token"
)

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

// recordFilter returns true if the record should be included in results.
type recordFilter func(chunk.Record) bool

// scannerBuilder constructs a scanner by composing position sources and filters.
// It handles the complexity of combining indexed lookups with runtime filtering,
// and gracefully falls back to sequential scanning when indexes aren't available.
//
// Position semantics:
//   - nil: no index narrowing, scan sequentially
//   - empty (len==0): index says no matches, skip chunk entirely
//   - non-empty: seek to these positions only
type scannerBuilder struct {
	chunkID   chunk.ChunkID
	positions []uint64       // nil = sequential, empty = no matches, non-empty = seek positions
	filters   []recordFilter // applied in order; cheap filters should be added first
	minPos    uint64         // prune positions below this (from time index or resume)
	hasMinPos bool
}

// newScannerBuilder creates a builder for the given chunk.
func newScannerBuilder(chunkID chunk.ChunkID) *scannerBuilder {
	return &scannerBuilder{chunkID: chunkID}
}

// setMinPosition sets the minimum position for pruning posting lists.
// Positions below this are excluded. Used for time-based start bounds and resume.
func (b *scannerBuilder) setMinPosition(pos uint64) {
	if !b.hasMinPos || pos > b.minPos {
		b.minPos = pos
		b.hasMinPos = true
	}
}

// addPositions intersects the given positions with existing positions.
// If this is the first position source, it sets positions directly.
// Returns false if the intersection is empty (no matches possible).
func (b *scannerBuilder) addPositions(positions []uint64) bool {
	// Prune positions below minPos.
	if b.hasMinPos {
		positions = prunePositions(positions, b.minPos)
	}
	if len(positions) == 0 {
		b.positions = []uint64{} // empty, not nil
		return false
	}

	if b.positions == nil {
		b.positions = positions
	} else {
		b.positions = intersectPositions(b.positions, positions)
	}
	return len(b.positions) > 0
}

// unionPositionsInto unions positions into an accumulator, then adds the result.
// Used for OR semantics (e.g., multiple sources).
func (b *scannerBuilder) unionPositionsInto(accumulator *[]uint64, positions []uint64) {
	if b.hasMinPos {
		positions = prunePositions(positions, b.minPos)
	}
	*accumulator = unionPositions(*accumulator, positions)
}

// addFilter adds a runtime filter that will be applied to each record.
// Filters are applied in the order they are added, so callers should add
// cheap filters (e.g., source ID check) before expensive ones (e.g., tokenization).
func (b *scannerBuilder) addFilter(f recordFilter) {
	b.filters = append(b.filters, f)
}

// excludePosition removes a specific position (used for resume to skip already-returned record).
func (b *scannerBuilder) excludePosition(pos uint64, reverse bool) {
	if b.positions == nil || len(b.positions) == 0 {
		return
	}
	if reverse {
		// In reverse, the position would be at the end.
		if b.positions[len(b.positions)-1] == pos {
			b.positions = b.positions[:len(b.positions)-1]
		}
	} else {
		// In forward, the position would be at the start.
		if b.positions[0] == pos {
			b.positions = b.positions[1:]
		}
	}
}

// hasNoMatches returns true if the index determined there are no matches.
// This is distinct from isSequential (no index narrowing).
func (b *scannerBuilder) hasNoMatches() bool {
	return b.positions != nil && len(b.positions) == 0
}

// isSequential returns true if we should do a sequential scan (no position list).
// This means no index contributed positions, so we must scan all records.
func (b *scannerBuilder) isSequential() bool {
	return b.positions == nil
}

// build creates the final scanner iterator.
func (b *scannerBuilder) build(cursor chunk.RecordCursor, q Query) iter.Seq2[recordWithRef, error] {
	if b.hasNoMatches() {
		return emptyScanner()
	}

	if b.isSequential() {
		return b.buildSequentialScanner(cursor, q)
	}

	return b.buildPositionScanner(cursor, q)
}

// buildSequentialScanner creates a scanner that reads records sequentially.
func (b *scannerBuilder) buildSequentialScanner(cursor chunk.RecordCursor, q Query) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()
	filters := b.filters

	if q.Reverse() {
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

				// Time bounds.
				if !lower.IsZero() && rec.IngestTS.Before(lower) {
					return // too old, stop
				}
				if !upper.IsZero() && !rec.IngestTS.Before(upper) {
					continue // too new, skip
				}

				// Apply filters.
				if !applyFilters(rec, filters) {
					continue
				}

				if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
					return
				}
			}
		}
	}

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

			// Time bounds.
			if !lower.IsZero() && rec.IngestTS.Before(lower) {
				continue // too old, skip
			}
			if !upper.IsZero() && !rec.IngestTS.Before(upper) {
				return // too new, stop
			}

			// Apply filters.
			if !applyFilters(rec, filters) {
				continue
			}

			if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
				return
			}
		}
	}
}

// buildPositionScanner creates a scanner that seeks to specific positions.
func (b *scannerBuilder) buildPositionScanner(cursor chunk.RecordCursor, q Query) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()
	positions := b.positions
	chunkID := b.chunkID
	filters := b.filters

	if q.Reverse() {
		return func(yield func(recordWithRef, error) bool) {
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

				// Time bounds.
				if !lower.IsZero() && rec.IngestTS.Before(lower) {
					return // too old, stop
				}
				if !upper.IsZero() && !rec.IngestTS.Before(upper) {
					continue // too new, skip
				}

				// Apply filters.
				if !applyFilters(rec, filters) {
					continue
				}

				if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
					return
				}
			}
		}
	}

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

			// Time bounds.
			if !lower.IsZero() && rec.IngestTS.Before(lower) {
				continue // too old, skip
			}
			if !upper.IsZero() && !rec.IngestTS.Before(upper) {
				return // too new, stop
			}

			// Apply filters.
			if !applyFilters(rec, filters) {
				continue
			}

			if !yield(recordWithRef{Record: rec, Ref: ref}, nil) {
				return
			}
		}
	}
}

// applyFilters returns true if the record passes all filters.
func applyFilters(rec chunk.Record, filters []recordFilter) bool {
	for _, f := range filters {
		if !f(rec) {
			return false
		}
	}
	return true
}

// Filter functions for common filter types.

// sourceFilter returns a filter that matches records from any of the given sources.
func sourceFilter(sources []chunk.SourceID) recordFilter {
	return func(rec chunk.Record) bool {
		return slices.Contains(sources, rec.SourceID)
	}
}

// tokenFilter returns a filter that matches records containing all given tokens.
func tokenFilter(tokens []string) recordFilter {
	return func(rec chunk.Record) bool {
		return matchesTokens(rec.Raw, tokens)
	}
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

// Index application functions. Each returns true if it contributed positions,
// false if the index wasn't available (caller should add a runtime filter).

// applyTimeIndex uses the time index to set the minimum position.
func applyTimeIndex(b *scannerBuilder, indexes index.IndexManager, chunkID chunk.ChunkID, lower, upper interface{ IsZero() bool }) bool {
	// Time index is only used for seeking, not for position filtering.
	// The actual time filtering is done in the scanner.
	return true
}

// applySourceIndex tries to use the source index for position filtering.
// Returns true if successful, false if index not available.
func applySourceIndex(b *scannerBuilder, indexes index.IndexManager, chunkID chunk.ChunkID, sources []chunk.SourceID) (ok bool, empty bool) {
	if len(sources) == 0 {
		return true, false
	}

	srcIdx, err := indexes.OpenSourceIndex(chunkID)
	if errors.Is(err, index.ErrIndexNotFound) {
		return false, false
	}
	if err != nil {
		return false, false // treat other errors as index not available
	}

	reader := index.NewSourceIndexReader(chunkID, srcIdx.Entries())
	var accumulated []uint64
	for _, src := range sources {
		if positions, found := reader.Lookup(src); found {
			b.unionPositionsInto(&accumulated, positions)
		}
	}

	if len(accumulated) == 0 {
		return true, true // index available but no matches
	}

	return b.addPositions(accumulated), false
}

// applyTokenIndex tries to use the token index for position filtering.
// Returns true if successful, false if index not available.
func applyTokenIndex(b *scannerBuilder, indexes index.IndexManager, chunkID chunk.ChunkID, tokens []string) (ok bool, empty bool) {
	if len(tokens) == 0 {
		return true, false
	}

	tokIdx, err := indexes.OpenTokenIndex(chunkID)
	if errors.Is(err, index.ErrIndexNotFound) {
		return false, false
	}
	if err != nil {
		return false, false
	}

	reader := index.NewTokenIndexReader(chunkID, tokIdx.Entries())

	// All tokens must be present (AND semantics).
	for i, tok := range tokens {
		positions, found := reader.Lookup(tok)
		if !found {
			return true, true // token not in chunk, no matches
		}
		if i == 0 {
			if !b.addPositions(positions) {
				return true, true
			}
		} else {
			// Intersect with existing positions.
			if !b.addPositions(positions) {
				return true, true
			}
		}
	}

	return true, false
}
