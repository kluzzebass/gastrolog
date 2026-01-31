// Package query provides a query engine that sits above chunk and index
// managers. It owns query semantics: selecting chunks, using indexes,
// driving cursors, merging results, and enforcing limits.
package query

import (
	"context"
	"errors"
	"iter"
	"log/slog"
	"slices"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
)

// Query describes what records to search for.
type Query struct {
	// Time bounds (if End < Start, results are returned in reverse/newest-first order)
	Start time.Time // inclusive bound (lower for forward, upper for reverse)
	End   time.Time // exclusive bound (upper for forward, lower for reverse)

	// Optional filters
	Tokens []string // filter by tokens (nil = no filter, AND semantics)

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
//
// Logging:
//   - Logger is dependency-injected via the constructor
//   - Engine owns its scoped logger (component="query-engine")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (search iteration, filtering)
type Engine struct {
	chunks  chunk.ChunkManager
	indexes index.IndexManager

	// Logger for this engine instance.
	// Scoped with component="query-engine" at construction time.
	logger *slog.Logger
}

// New creates a query engine backed by the given chunk and index managers.
// If logger is nil, logging is disabled.
func New(chunks chunk.ChunkManager, indexes index.IndexManager, logger *slog.Logger) *Engine {
	return &Engine{
		chunks:  chunks,
		indexes: indexes,
		logger:  logging.Default(logger).With("component", "query-engine"),
	}
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
			if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: uint64(meta.RecordCount)}); err != nil {
				yield(recordWithRef{}, err)
				return
			}
		}

		// Try to use indexes for sealed chunks, fall back to sequential scan
		// if indexes aren't available yet (chunk sealed but not yet indexed).
		scanner, err := e.buildScanner(cursor, q, meta, startPos)
		if err != nil {
			yield(recordWithRef{}, err)
			return
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

// buildScanner creates a scanner for a chunk using the composable filter pipeline.
// It tries to use indexes when available, falling back to runtime filters when not.
func (e *Engine) buildScanner(cursor chunk.RecordCursor, q Query, meta chunk.ChunkMeta, startPos *uint64) (iter.Seq2[recordWithRef, error], error) {
	b := newScannerBuilder(meta.ID)

	// Set minimum position from time index or binary search on idx.log.
	lower, _ := q.TimeBounds()
	if !lower.IsZero() {
		var foundPos bool
		var pos uint64

		// Try time index first (only available for sealed chunks).
		if meta.Sealed {
			timeIdx, err := e.indexes.OpenTimeIndex(meta.ID)
			if err == nil {
				reader := index.NewTimeIndexReader(meta.ID, timeIdx.Entries())
				if seekRef, found := reader.FindStart(lower); found {
					pos = seekRef.Pos
					foundPos = true
				}
			}
		}

		// Fall back to binary search on idx.log (works for both sealed and unsealed).
		if !foundPos {
			if p, found, err := e.chunks.FindStartPosition(meta.ID, lower); err == nil && found {
				pos = p
				foundPos = true
			}
		}

		if foundPos {
			b.setMinPosition(pos)
		}
	}

	// Resume position takes precedence over time-based start.
	if startPos != nil {
		b.setMinPosition(*startPos)
	}

	// Apply token filter: try index first, fall back to runtime filter.
	if len(q.Tokens) > 0 {
		if meta.Sealed {
			ok, empty := applyTokenIndex(b, e.indexes, meta.ID, q.Tokens)
			if empty {
				return emptyScanner(), nil
			}
			if !ok {
				// Index not available, use runtime filter.
				b.addFilter(tokenFilter(q.Tokens))
			}
		} else {
			// Unsealed chunks don't have indexes.
			b.addFilter(tokenFilter(q.Tokens))
		}
	}

	// Exclude resume position (already returned).
	if startPos != nil {
		b.excludePosition(*startPos, q.Reverse())
	}

	// Seek cursor to start position if we have one and not using positions.
	if b.isSequential() && b.hasMinPos && startPos == nil && !q.Reverse() {
		if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: b.minPos}); err != nil {
			return nil, err
		}
	}

	return b.build(cursor, q), nil
}

// emptyScanner returns a scanner that yields no records.
func emptyScanner() iter.Seq2[recordWithRef, error] {
	return func(yield func(recordWithRef, error) bool) {}
}
