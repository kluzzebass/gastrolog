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
	"gastrolog/internal/querylang"
)

// KeyValueFilter represents a key=value filter that searches both
// record attributes and key=value pairs extracted from the message body.
// The filter matches if the key=value pair is found in either location.
//
// Wildcard patterns:
//   - Key="foo", Value="bar" - exact match for foo=bar
//   - Key="foo", Value=""    - match any record with key "foo" (any value)
//   - Key="", Value="bar"    - match any record with value "bar" (any key)
type KeyValueFilter struct {
	Key   string // empty string means "any key"
	Value string // empty string means "any value"
}

// Query describes what records to search for.
type Query struct {
	// Time bounds (if End < Start, results are returned in reverse/newest-first order)
	Start time.Time // inclusive bound (lower for forward, upper for reverse)
	End   time.Time // exclusive bound (upper for forward, lower for reverse)

	// Optional filters (legacy API, ignored if BoolExpr is set)
	Tokens []string         // filter by tokens (nil = no filter, AND semantics)
	KV     []KeyValueFilter // filter by key=value in attrs OR message (nil = no filter, AND semantics)

	// BoolExpr is an optional boolean expression filter.
	// If set, Tokens and KV are ignored; filtering is driven by this expression.
	// This enables complex queries like "(error OR warn) AND NOT debug".
	BoolExpr querylang.Expr

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

// Normalize converts legacy Tokens/KV fields to BoolExpr if BoolExpr is not set.
// This ensures all filtering goes through the unified BoolExpr path.
func (q Query) Normalize() Query {
	if q.BoolExpr != nil {
		return q
	}
	if len(q.Tokens) == 0 && len(q.KV) == 0 {
		return q
	}

	// Build predicates from legacy fields
	var predicates []querylang.Expr

	for _, tok := range q.Tokens {
		predicates = append(predicates, &querylang.PredicateExpr{
			Kind:  querylang.PredToken,
			Value: tok,
		})
	}

	for _, f := range q.KV {
		var pred *querylang.PredicateExpr
		if f.Key == "" && f.Value != "" {
			pred = &querylang.PredicateExpr{Kind: querylang.PredValueExists, Value: f.Value}
		} else if f.Key != "" && f.Value == "" {
			pred = &querylang.PredicateExpr{Kind: querylang.PredKeyExists, Key: f.Key}
		} else if f.Key != "" && f.Value != "" {
			pred = &querylang.PredicateExpr{Kind: querylang.PredKV, Key: f.Key, Value: f.Value}
		}
		if pred != nil {
			predicates = append(predicates, pred)
		}
	}

	if len(predicates) == 0 {
		return q
	}

	// Combine with AND semantics
	var expr querylang.Expr
	if len(predicates) == 1 {
		expr = predicates[0]
	} else {
		expr = querylang.FlattenAnd(predicates...)
	}

	result := q
	result.BoolExpr = expr
	return result
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

	// Set minimum position from binary search on idx.log.
	lower, _ := q.TimeBounds()
	if !lower.IsZero() {
		if pos, found, err := e.chunks.FindStartPosition(meta.ID, lower); err == nil && found {
			b.setMinPosition(pos)
		}
	}

	// Resume position takes precedence over time-based start.
	if startPos != nil {
		b.setMinPosition(*startPos)
	}

	// Convert BoolExpr to DNF and process.
	// For single-branch DNF: use index acceleration for positive predicates, runtime filter for negatives.
	// For multi-branch DNF: union positions from branches, apply DNF filter for correctness.
	if q.BoolExpr == nil {
		// No filter expression - return all records (subject to time bounds).
		// This is handled by the scanner builder with no filters.
	} else {
		dnf := querylang.ToDNF(q.BoolExpr)

		if len(dnf.Branches) == 0 {
			// Empty DNF - no matches
			return emptyScanner(), nil
		}

		if len(dnf.Branches) == 1 {
			// Single branch: use index acceleration
			tokens, kv, negFilter := ConjunctionToFilters(&dnf.Branches[0])

			// Apply token filter for positive predicates
			if len(tokens) > 0 {
				if meta.Sealed {
					ok, empty := applyTokenIndex(b, e.indexes, meta.ID, tokens)
					if empty {
						return emptyScanner(), nil
					}
					if !ok {
						b.addFilter(tokenFilter(tokens))
					}
				} else {
					b.addFilter(tokenFilter(tokens))
				}
			}

			// Apply KV filter for positive predicates
			if len(kv) > 0 {
				if meta.Sealed {
					ok, empty := applyKeyValueIndex(b, e.indexes, meta.ID, kv)
					if empty {
						return emptyScanner(), nil
					}
					if !ok {
						b.addFilter(keyValueFilter(kv))
					}
				} else {
					b.addFilter(keyValueFilter(kv))
				}
			}

			// Apply negation filter for NOT predicates
			if negFilter != nil {
				b.addFilter(negFilter)
			}
		} else {
			// Multi-branch DNF: execute each branch and union positions.
			// Each branch can use indexes independently.
			var allPositions []uint64
			anyBranchHasPositions := false

			for _, branch := range dnf.Branches {
				branchBuilder := newScannerBuilder(meta.ID)
				if b.hasMinPos {
					branchBuilder.setMinPosition(b.minPos)
				}

				tokens, kv, _ := ConjunctionToFilters(&branch)
				branchEmpty := false

				// Try to get positions from token index
				if len(tokens) > 0 && meta.Sealed {
					ok, empty := applyTokenIndex(branchBuilder, e.indexes, meta.ID, tokens)
					if empty {
						branchEmpty = true
					}
					_ = ok
				}

				// Try to get positions from KV index
				if !branchEmpty && len(kv) > 0 && meta.Sealed {
					ok, empty := applyKeyValueIndex(branchBuilder, e.indexes, meta.ID, kv)
					if empty {
						branchEmpty = true
					}
					_ = ok
				}

				if branchEmpty {
					continue // this branch has no matches, skip
				}

				if branchBuilder.positions != nil {
					// This branch contributed positions - union them
					allPositions = unionPositions(allPositions, branchBuilder.positions)
					anyBranchHasPositions = true
				} else {
					// Branch requires sequential scan (no index narrowing)
					// Fall back to full runtime filter
					anyBranchHasPositions = false
					break
				}
			}

			if anyBranchHasPositions && len(allPositions) > 0 {
				// Use unioned positions from all branches
				b.positions = allPositions
			}
			// Note: if all branches were empty, positions stays nil (sequential scan)

			// Apply DNF filter for correctness.
			// This evaluates primitive predicates per-branch, not recursive AST evaluation.
			b.addFilter(dnfFilter(&dnf))
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
