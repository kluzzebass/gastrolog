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
	"gastrolog/internal/lookup"
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

	// Op is the comparison operator (default OpEq). Non-eq ops use key-only
	// index acceleration with runtime value comparison.
	Op querylang.CompareOp
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
		switch {
		case f.Key == "" && f.Value != "":
			pred = &querylang.PredicateExpr{Kind: querylang.PredValueExists, Value: f.Value}
		case f.Key != "" && f.Value == "":
			pred = &querylang.PredicateExpr{Kind: querylang.PredKeyExists, Key: f.Key}
		case f.Key != "" && f.Value != "":
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
	//
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

	// Lookup enrichment resolver (optional). Set via SetLookupResolver.
	lookupResolver lookup.Resolver

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

// SetLookupResolver sets the lookup resolver used by pipeline lookup operators.
func (e *Engine) SetLookupResolver(r lookup.Resolver) {
	e.lookupResolver = r
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
	chunkSet := buildChunkIDSet(chunkIDs)

	var out []chunk.ChunkMeta
	for _, m := range metas {
		if chunkMatchesQuery(m, q, lower, upper, chunkSet) {
			out = append(out, m)
		}
	}
	sortChunksByStartTS(out, q.Reverse())
	return out
}

// buildChunkIDSet creates a lookup set from chunk IDs, or returns nil if chunkIDs is nil.
func buildChunkIDSet(chunkIDs []chunk.ChunkID) map[chunk.ChunkID]struct{} {
	if chunkIDs == nil {
		return nil
	}
	set := make(map[chunk.ChunkID]struct{}, len(chunkIDs))
	for _, id := range chunkIDs {
		set[id] = struct{}{}
	}
	return set
}

// chunkMatchesQuery returns true if a chunk should be included in the query results.
func chunkMatchesQuery(m chunk.ChunkMeta, q Query, lower, upper time.Time, chunkSet map[chunk.ChunkID]struct{}) bool {
	// Chunk ID filter (if specified).
	if chunkSet != nil {
		if _, ok := chunkSet[m.ID]; !ok {
			return false
		}
	}
	if m.Sealed {
		// Chunk must overlap [lower, upper)
		if !lower.IsZero() && m.EndTS.Before(lower) {
			return false
		}
		if !upper.IsZero() && !m.StartTS.Before(upper) {
			return false
		}
	}

	// Filter by IngestTS bounds if query specifies them.
	if !q.IngestStart.IsZero() && !m.IngestEnd.IsZero() && m.IngestEnd.Before(q.IngestStart) {
		return false
	}
	if !q.IngestEnd.IsZero() && !m.IngestStart.IsZero() && !m.IngestStart.Before(q.IngestEnd) {
		return false
	}

	// Filter by SourceTS bounds if query specifies them.
	// Chunk with no SourceTS data (both zero) is included.
	if !q.SourceStart.IsZero() && !m.SourceEnd.IsZero() && m.SourceEnd.Before(q.SourceStart) {
		return false
	}
	if !q.SourceEnd.IsZero() && !m.SourceStart.IsZero() && !m.SourceStart.Before(q.SourceEnd) {
		return false
	}

	return true
}

// sortChunksByStartTS sorts chunks by StartTS in ascending or descending order.
func sortChunksByStartTS(out []chunk.ChunkMeta, reverse bool) {
	if reverse {
		slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
			return b.StartTS.Compare(a.StartTS) // descending
		})
	} else {
		slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
			return a.StartTS.Compare(b.StartTS) // ascending
		})
	}
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
		defer func() { _ = cursor.Close() }()

		if err := positionCursor(cursor, q, meta, startPos); err != nil {
			yield(recordWithRef{}, err)
			return
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

// positionCursor sets the cursor to the correct starting position for the query.
// For resume: seek to startPos and skip past it (forward) or leave at it (reverse).
// For reverse without resume: seek to end of chunk.
func positionCursor(cursor chunk.RecordCursor, q Query, meta chunk.ChunkMeta, startPos *uint64) error {
	if startPos != nil {
		if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: *startPos}); err != nil {
			return err
		}
		// Skip the record at startPos - it was already returned before the break.
		// For forward: call Next() to move past resume position.
		// For reverse: cursor.Prev() decrements before returning, so seeking to
		// the resume position is sufficient - the first Prev() will skip it.
		if !q.Reverse() {
			if _, _, err := cursor.Next(); err != nil && !errors.Is(err, chunk.ErrNoMoreRecords) {
				return err
			}
		}
		return nil
	}
	if q.Reverse() {
		// For reverse without resume, seek to end of chunk.
		return cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: uint64(meta.RecordCount)}) //nolint:gosec // G115: RecordCount is always non-negative
	}
	return nil
}

// buildScannerWithManagers creates a scanner for a chunk using the composable filter pipeline.
// It tries to use indexes when available, falling back to runtime filters when not.
// storeID is included in the returned recordWithRef for multi-store queries.
func (e *Engine) buildScannerWithManagers(ctx context.Context, cursor chunk.RecordCursor, q Query, storeID uuid.UUID, meta chunk.ChunkMeta, startPos *uint64, cm chunk.ChunkManager, im index.IndexManager) (iter.Seq2[recordWithRef, error], error) {
	b := newScannerBuilder(meta.ID)
	b.storeID = storeID

	setMinPositionsFromBounds(b, q, meta, cm, im)

	// Resume position takes precedence over time-based start.
	if startPos != nil {
		b.setMinPosition(*startPos)
	}

	// Exact position filter: seek directly to one record.
	if q.Pos != nil {
		b.addPositions([]uint64{*q.Pos})
	}

	// Convert BoolExpr to DNF and apply index acceleration + runtime filters.
	if q.BoolExpr != nil {
		if empty, err := applyBoolExpr(b, q.BoolExpr, meta, im); err != nil {
			return nil, err
		} else if empty {
			return emptyScanner(), nil
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

// setMinPositionsFromBounds sets the scanner builder's minimum position from time bounds
// using binary search on WriteTS, IngestTS, and SourceTS indexes.
func setMinPositionsFromBounds(b *scannerBuilder, q Query, meta chunk.ChunkMeta, cm chunk.ChunkManager, im index.IndexManager) {
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
}

// applyBoolExpr converts a BoolExpr to DNF and applies index acceleration and runtime filters
// to the scanner builder. Returns (true, nil) if the chunk is definitely empty.
func applyBoolExpr(b *scannerBuilder, expr querylang.Expr, meta chunk.ChunkMeta, im index.IndexManager) (empty bool, err error) {
	dnf := querylang.ToDNF(expr)

	if len(dnf.Branches) == 0 {
		return true, nil
	}

	if len(dnf.Branches) == 1 {
		return applySingleBranchDNF(b, &dnf.Branches[0], meta, im), nil
	}

	applyMultiBranchDNF(b, &dnf, meta, im)
	return false, nil
}

// applySingleBranchDNF applies index acceleration and runtime filters for a single DNF branch.
// Returns true if the chunk is definitely empty (no matches).
func applySingleBranchDNF(b *scannerBuilder, branch *querylang.Conjunction, meta chunk.ChunkMeta, im index.IndexManager) (empty bool) {
	tokens, kv, globs, negFilter := ConjunctionToFilters(branch)

	if applySingleBranchTokens(b, tokens, meta, im) {
		return true
	}
	if applySingleBranchGlobs(b, globs, meta, im) {
		return true
	}
	if applySingleBranchKV(b, kv, meta, im) {
		return true
	}

	if negFilter != nil {
		b.addFilter(negFilter)
	}
	return false
}

// applySingleBranchTokens applies token index acceleration for a single DNF branch.
// Returns true if the chunk is definitely empty.
func applySingleBranchTokens(b *scannerBuilder, tokens []string, meta chunk.ChunkMeta, im index.IndexManager) bool {
	if len(tokens) == 0 {
		return false
	}
	if !meta.Sealed {
		b.addFilter(tokenFilter(tokens))
		return false
	}
	ok, empty := applyTokenIndex(b, im, meta.ID, tokens)
	if empty {
		return true
	}
	if !ok {
		b.addFilter(tokenFilter(tokens))
	}
	return false
}

// applySingleBranchGlobs applies glob index acceleration for a single DNF branch.
// Returns true if the chunk is definitely empty.
func applySingleBranchGlobs(b *scannerBuilder, globs []GlobFilter, meta chunk.ChunkMeta, im index.IndexManager) bool {
	if len(globs) == 0 {
		return false
	}
	if !meta.Sealed {
		b.addFilter(globTokenFilter(globs))
		return false
	}
	_, empty := applyGlobIndex(b, im, meta.ID, globs)
	if empty {
		return true
	}
	// Always add runtime filter: prefix-based positions still need full glob verification.
	b.addFilter(globTokenFilter(globs))
	return false
}

// applySingleBranchKV applies key-value index acceleration for a single DNF branch.
// Returns true if the chunk is definitely empty.
func applySingleBranchKV(b *scannerBuilder, kv []KeyValueFilter, meta chunk.ChunkMeta, im index.IndexManager) bool {
	if len(kv) == 0 {
		return false
	}
	if meta.Sealed {
		if _, empty := applyKeyValueIndex(b, im, meta.ID, kv); empty {
			return true
		}
	}
	// Always add runtime filter: the index narrows positions by
	// key existence, but comparison operators (>=, <=, etc.) still
	// need runtime value verification. For OpEq this is redundant
	// but harmless — matches the pattern used by glob filters.
	b.addFilter(keyValueFilter(kv))
	return false
}

// applyMultiBranchDNF handles multi-branch DNF: unions positions from branches
// and adds a DNF filter for correctness.
func applyMultiBranchDNF(b *scannerBuilder, dnf *querylang.DNF, meta chunk.ChunkMeta, im index.IndexManager) {
	var allPositions []uint64
	anyBranchHasPositions := false

	for i := range dnf.Branches {
		positions, hasPositions, branchEmpty := collectBranchPositions(b, &dnf.Branches[i], meta, im)
		if branchEmpty {
			continue
		}
		if !hasPositions {
			// Branch requires sequential scan — fall back to full runtime filter.
			anyBranchHasPositions = false
			break
		}
		allPositions = unionPositions(allPositions, positions)
		anyBranchHasPositions = true
	}

	if anyBranchHasPositions && len(allPositions) > 0 {
		b.positions = allPositions
	}

	// Apply DNF filter for correctness.
	// This evaluates primitive predicates per-branch, not recursive AST evaluation.
	b.addFilter(dnfFilter(dnf))
}

// collectBranchPositions tries index acceleration on a single DNF branch.
// Returns the collected positions, whether the branch contributed positions,
// and whether the branch is definitely empty.
func collectBranchPositions(parent *scannerBuilder, branch *querylang.Conjunction, meta chunk.ChunkMeta, im index.IndexManager) (positions []uint64, hasPositions bool, branchEmpty bool) {
	bb := newScannerBuilder(meta.ID)
	if parent.hasMinPos {
		bb.setMinPosition(parent.minPos)
	}

	tokens, kv, globs, _ := ConjunctionToFilters(branch)

	if len(tokens) > 0 && meta.Sealed {
		if _, empty := applyTokenIndex(bb, im, meta.ID, tokens); empty {
			return nil, false, true
		}
	}
	if len(globs) > 0 && meta.Sealed {
		if _, empty := applyGlobIndex(bb, im, meta.ID, globs); empty {
			return nil, false, true
		}
	}
	if len(kv) > 0 && meta.Sealed {
		if _, empty := applyKeyValueIndex(bb, im, meta.ID, kv); empty {
			return nil, false, true
		}
	}

	if bb.positions == nil {
		return nil, false, false
	}
	return bb.positions, true, false
}

// emptyScanner returns a scanner that yields no records.
func emptyScanner() iter.Seq2[recordWithRef, error] {
	return func(yield func(recordWithRef, error) bool) {}
}
