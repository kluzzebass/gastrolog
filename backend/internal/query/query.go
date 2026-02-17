// Package query provides a query engine that sits above chunk and index
// managers. It owns query semantics: selecting chunks, using indexes,
// driving cursors, merging results, and enforcing limits.
package query

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"log/slog"
	"regexp"
	"slices"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
	"gastrolog/internal/querylang"

	"github.com/google/uuid"
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

	// Glob patterns for key/value positions. When non-nil, matching uses regex
	// instead of exact string comparison.
	KeyPat   *regexp.Regexp // compiled glob for key (e.g., err*=value)
	ValuePat *regexp.Regexp // compiled glob for value (e.g., key=err*)
}

// Query describes what records to search for.
type Query struct {
	// Time bounds on WriteTS.
	Start time.Time // inclusive lower bound
	End   time.Time // exclusive upper bound

	// Time bounds on SourceTS (optional runtime filters)
	SourceStart time.Time // inclusive lower bound on SourceTS
	SourceEnd   time.Time // exclusive upper bound on SourceTS

	// Time bounds on IngestTS (optional runtime filters)
	IngestStart time.Time // inclusive lower bound on IngestTS
	IngestEnd   time.Time // exclusive upper bound on IngestTS

	// Optional filters (legacy API, ignored if BoolExpr is set)
	Tokens []string         // filter by tokens (nil = no filter, AND semantics)
	KV     []KeyValueFilter // filter by key=value in attrs OR message (nil = no filter, AND semantics)

	// BoolExpr is an optional boolean expression filter.
	// If set, Tokens and KV are ignored; filtering is driven by this expression.
	// This enables complex queries like "(error OR warn) AND NOT debug".
	BoolExpr querylang.Expr

	// RawExpression is the original query string before parsing.
	// Used for serialization over gRPC (the server re-parses it).
	// Set by callers that parse from a string (e.g., REPL).
	RawExpression string

	// Result control
	IsReverse bool    // return results newest-first
	Limit     int     // max results (0 = unlimited)
	Pos       *uint64 // exact record position within chunk (nil = no filter)

	// Context windows (for SearchWithContext)
	ContextBefore int // number of records to include before each match
	ContextAfter  int // number of records to include after each match
}

// String returns a human-readable representation of the query including all parameters.
func (q Query) String() string {
	var parts []string
	if q.BoolExpr != nil {
		parts = append(parts, q.BoolExpr.String())
	}
	if !q.Start.IsZero() {
		parts = append(parts, "start="+q.Start.Format(time.RFC3339))
	}
	if !q.End.IsZero() {
		parts = append(parts, "end="+q.End.Format(time.RFC3339))
	}
	if q.IsReverse {
		parts = append(parts, "reverse=true")
	}
	if q.Limit > 0 {
		parts = append(parts, fmt.Sprintf("limit=%d", q.Limit))
	}
	return strings.Join(parts, " ")
}

// Reverse returns true if this query should return results in reverse (newest-first) order.
func (q Query) Reverse() bool {
	if q.IsReverse {
		return true
	}
	// Legacy convention: End < Start means reverse.
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
// Always returns lower <= upper regardless of query direction.
func (q Query) TimeBounds() (lower, upper time.Time) {
	if q.IsReverse {
		// New-style: Start/End are always lower/upper.
		return q.Start, q.End
	}
	// Legacy convention: End < Start means reverse, swap them.
	if q.Reverse() {
		return q.End, q.Start
	}
	return q.Start, q.End
}

// WithStorePredicate returns a copy of the query with a store=X predicate added.
// The predicate is ANDed with any existing BoolExpr.
func (q Query) WithStorePredicate(storeID string) Query {
	storePred := &querylang.PredicateExpr{
		Kind:  querylang.PredKV,
		Key:   "store",
		Value: storeID,
	}

	result := q
	if q.BoolExpr == nil {
		result.BoolExpr = storePred
	} else {
		result.BoolExpr = querylang.FlattenAnd(q.BoolExpr, storePred)
	}
	return result
}

// MultiStorePosition represents a position within a specific store's chunk.
type MultiStorePosition struct {
	StoreID  uuid.UUID
	ChunkID  chunk.ChunkID
	Position uint64
}

// ResumeToken allows resuming a query from where it left off.
// For multi-store queries, Positions contains the last position in each active chunk.
// Tokens are valid as long as the referenced chunks exist.
type ResumeToken struct {
	// Positions contains the last yielded position for each store/chunk combination.
	// For single-store queries with one chunk, this will have one entry.
	// For multi-store queries, this may have multiple entries (one per active chunk).
	Positions []MultiStorePosition

	// Legacy field for backward compatibility with single-store resume tokens.
	// Deprecated: use Positions instead.
	Next chunk.RecordRef
}

// Normalize converts a legacy resume token (using Next) to the new format (using Positions).
// If Positions is already populated, returns the token unchanged.
// The storeID parameter is used for legacy tokens that don't include store information.
func (t *ResumeToken) Normalize(defaultStoreID uuid.UUID) *ResumeToken {
	if t == nil {
		return nil
	}
	// If Positions is already populated, use it as-is.
	if len(t.Positions) > 0 {
		return t
	}
	// Convert legacy Next field to Positions.
	var zeroChunkID chunk.ChunkID
	if t.Next.ChunkID == zeroChunkID {
		return t
	}
	return &ResumeToken{
		Positions: []MultiStorePosition{{
			StoreID:  defaultStoreID,
			ChunkID:  t.Next.ChunkID,
			Position: t.Next.Pos,
		}},
	}
}

// ErrInvalidResumeToken is returned when a resume token references a chunk that no longer exists.
var ErrInvalidResumeToken = errors.New("invalid resume token: chunk no longer exists")

// ErrMultiStoreNotSupported is returned when an operation doesn't support multi-store mode.
var ErrMultiStoreNotSupported = errors.New("operation not supported in multi-store mode")

// recordWithRef combines a record with its reference for internal iteration.
// StoreID is included for multi-store queries.
type recordWithRef struct {
	StoreID uuid.UUID
	Record  chunk.Record
	Ref     chunk.RecordRef
}

// record returns the Record with Ref and StoreID populated.
func (rr recordWithRef) record() chunk.Record {
	rr.Record.Ref = rr.Ref
	rr.Record.StoreID = rr.StoreID
	return rr.Record
}

// Engine executes queries against chunk and index managers.
//
// The engine can operate in two modes:
//   - Single-store mode: created with New(), queries one store
//   - Multi-store mode: created with NewWithRegistry(), queries across stores
//
// Logging:
//   - Logger is dependency-injected via the constructor
//   - Engine owns its scoped logger (component="query-engine")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (search iteration, filtering)
type Engine struct {
	// Single-store mode (legacy)
	chunks  chunk.ChunkManager
	indexes index.IndexManager

	// Multi-store mode
	registry StoreRegistry

	// Logger for this engine instance.
	// Scoped with component="query-engine" at construction time.
	logger *slog.Logger
}

// New creates a query engine backed by the given chunk and index managers.
// This creates a single-store engine for backward compatibility.
// If logger is nil, logging is disabled.
func New(chunks chunk.ChunkManager, indexes index.IndexManager, logger *slog.Logger) *Engine {
	return &Engine{
		chunks:  chunks,
		indexes: indexes,
		logger:  logging.Default(logger).With("component", "query-engine"),
	}
}

// NewWithRegistry creates a query engine that can search across multiple stores.
// Store predicates in queries (e.g., "store=prod") filter which stores are searched.
// If no store predicate is present, all stores are searched.
// If logger is nil, logging is disabled.
func NewWithRegistry(registry StoreRegistry, logger *slog.Logger) *Engine {
	return &Engine{
		registry: registry,
		logger:   logging.Default(logger).With("component", "query-engine"),
	}
}

// isMultiStore returns true if this engine operates in multi-store mode.
func (e *Engine) isMultiStore() bool {
	return e.registry != nil
}

// getStoreManagers returns the chunk and index managers for a store.
// For single-store mode, storeID is ignored.
func (e *Engine) getStoreManagers(storeID uuid.UUID) (chunk.ChunkManager, index.IndexManager) {
	if e.registry != nil {
		return e.registry.ChunkManager(storeID), e.registry.IndexManager(storeID)
	}
	return e.chunks, e.indexes
}

// listStores returns all store IDs this engine can query.
func (e *Engine) listStores() []uuid.UUID {
	if e.registry != nil {
		return e.registry.ListStores()
	}
	return []uuid.UUID{uuid.Nil}
}

// selectChunks filters to chunks that overlap the query time range,
// sorted by StartTS (ascending for forward, descending for reverse).
// Unsealed chunks are always included (their EndTS is not final).
// If chunkIDs is non-nil, only chunks with matching IDs are included.
func (e *Engine) selectChunks(metas []chunk.ChunkMeta, q Query, chunkIDs []chunk.ChunkID) []chunk.ChunkMeta {
	lower, upper := q.TimeBounds()

	// Build lookup set for chunk ID filtering.
	var chunkSet map[chunk.ChunkID]struct{}
	if chunkIDs != nil {
		chunkSet = make(map[chunk.ChunkID]struct{}, len(chunkIDs))
		for _, id := range chunkIDs {
			chunkSet[id] = struct{}{}
		}
	}

	var out []chunk.ChunkMeta
	for _, m := range metas {
		// Chunk ID filter (if specified).
		if chunkSet != nil {
			if _, ok := chunkSet[m.ID]; !ok {
				continue
			}
		}
		if m.Sealed {
			// Chunk must overlap [lower, upper)
			if !lower.IsZero() && m.EndTS.Before(lower) {
				continue
			}
			if !upper.IsZero() && !m.StartTS.Before(upper) {
				continue
			}
		}

		// Filter by IngestTS bounds if query specifies them.
		if !q.IngestStart.IsZero() && !m.IngestEnd.IsZero() && m.IngestEnd.Before(q.IngestStart) {
			continue
		}
		if !q.IngestEnd.IsZero() && !m.IngestStart.IsZero() && !m.IngestStart.Before(q.IngestEnd) {
			continue
		}

		// Filter by SourceTS bounds if query specifies them.
		// Chunk with no SourceTS data (both zero) is included.
		if !q.SourceStart.IsZero() && !m.SourceEnd.IsZero() && m.SourceEnd.Before(q.SourceStart) {
			continue
		}
		if !q.SourceEnd.IsZero() && !m.SourceStart.IsZero() && !m.SourceStart.Before(q.SourceEnd) {
			continue
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
// storeID identifies which store the chunk belongs to (for multi-store queries).
// startPos allows resuming from a specific position within the chunk.
// Unsealed chunks are scanned sequentially without indexes.
func (e *Engine) searchChunkWithRef(ctx context.Context, q Query, storeID uuid.UUID, meta chunk.ChunkMeta, startPos *uint64) iter.Seq2[recordWithRef, error] {
	return func(yield func(recordWithRef, error) bool) {
		cm, im := e.getStoreManagers(storeID)
		if cm == nil {
			yield(recordWithRef{}, errors.New("store not found: "+storeID.String()))
			return
		}

		cursor, err := cm.OpenCursor(meta.ID)
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
		scanner, err := e.buildScannerWithManagers(ctx, cursor, q, storeID, meta, startPos, cm, im)
		if err != nil {
			yield(recordWithRef{}, err)
			return
		}

		for rr, err := range scanner {
			if err != nil {
				yield(rr, err)
				return
			}
			rr.Record.Ref = rr.Ref
			rr.Record.StoreID = rr.StoreID
			if !yield(rr, nil) {
				return
			}
		}
	}
}

// buildScannerWithManagers creates a scanner for a chunk using the composable filter pipeline.
// It tries to use indexes when available, falling back to runtime filters when not.
// storeID is included in the returned recordWithRef for multi-store queries.
func (e *Engine) buildScannerWithManagers(ctx context.Context, cursor chunk.RecordCursor, q Query, storeID uuid.UUID, meta chunk.ChunkMeta, startPos *uint64, cm chunk.ChunkManager, im index.IndexManager) (iter.Seq2[recordWithRef, error], error) {
	b := newScannerBuilder(meta.ID)
	b.storeID = storeID

	// Set minimum position from time bounds.
	// WriteTS: binary search on idx.log.
	lower, _ := q.TimeBounds()
	if !lower.IsZero() {
		if pos, found, err := cm.FindStartPosition(meta.ID, lower); err == nil && found {
			b.setMinPosition(pos)
		}
	}
	// IngestTS: use ingest index when available.
	if !q.IngestStart.IsZero() && meta.Sealed {
		if pos, found, err := im.FindIngestStartPosition(meta.ID, q.IngestStart); err == nil && found {
			b.setMinPosition(pos)
		}
	}
	// SourceTS: use source index when available.
	if !q.SourceStart.IsZero() && meta.Sealed {
		if pos, found, err := im.FindSourceStartPosition(meta.ID, q.SourceStart); err == nil && found {
			b.setMinPosition(pos)
		}
	}

	// Resume position takes precedence over time-based start.
	if startPos != nil {
		b.setMinPosition(*startPos)
	}

	// Exact position filter: seek directly to one record.
	if q.Pos != nil {
		b.addPositions([]uint64{*q.Pos})
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
			tokens, kv, globs, negFilter := ConjunctionToFilters(&dnf.Branches[0])

			// Apply token filter for positive predicates
			if len(tokens) > 0 {
				if meta.Sealed {
					ok, empty := applyTokenIndex(b, im, meta.ID, tokens)
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

			// Apply glob filter for positive predicates
			if len(globs) > 0 {
				if meta.Sealed {
					ok, empty := applyGlobIndex(b, im, meta.ID, globs)
					if empty {
						return emptyScanner(), nil
					}
					if !ok {
						b.addFilter(globTokenFilter(globs))
					} else {
						// Index gave us prefix-based positions, but we still need a runtime
						// filter to verify the full glob pattern matches a token.
						b.addFilter(globTokenFilter(globs))
					}
				} else {
					b.addFilter(globTokenFilter(globs))
				}
			}

			// Apply KV filter for positive predicates
			if len(kv) > 0 {
				if meta.Sealed {
					ok, empty := applyKeyValueIndex(b, im, meta.ID, kv)
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

				tokens, kv, globs, _ := ConjunctionToFilters(&branch)
				branchEmpty := false

				// Try to get positions from token index
				if len(tokens) > 0 && meta.Sealed {
					ok, empty := applyTokenIndex(branchBuilder, im, meta.ID, tokens)
					if empty {
						branchEmpty = true
					}
					_ = ok
				}

				// Try to get positions from glob index
				if !branchEmpty && len(globs) > 0 && meta.Sealed {
					ok, empty := applyGlobIndex(branchBuilder, im, meta.ID, globs)
					if empty {
						branchEmpty = true
					}
					_ = ok
				}

				// Try to get positions from KV index
				if !branchEmpty && len(kv) > 0 && meta.Sealed {
					ok, empty := applyKeyValueIndex(branchBuilder, im, meta.ID, kv)
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

	// Add SourceTS filter if bounds are set.
	if !q.SourceStart.IsZero() || !q.SourceEnd.IsZero() {
		b.addFilter(sourceTimeFilter(q.SourceStart, q.SourceEnd))
	}

	// Add IngestTS filter if bounds are set.
	if !q.IngestStart.IsZero() || !q.IngestEnd.IsZero() {
		b.addFilter(ingestTimeFilter(q.IngestStart, q.IngestEnd))
	}

	// Seek cursor to start position if we have one and not using positions.
	if b.isSequential() && b.hasMinPos && startPos == nil && !q.Reverse() {
		if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: b.minPos}); err != nil {
			return nil, err
		}
	}

	return b.build(ctx, cursor, q), nil
}

// emptyScanner returns a scanner that yields no records.
func emptyScanner() iter.Seq2[recordWithRef, error] {
	return func(yield func(recordWithRef, error) bool) {}
}
