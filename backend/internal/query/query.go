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
	"sort"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
	"gastrolog/internal/lookup"
	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)

// OrderBy specifies which timestamp field to use for ordering search results.
type OrderBy int

const (
	OrderByIngestTS OrderBy = iota // default: order by IngestTS (primary timestamp)
	OrderBySourceTS                // order by SourceTS
)

// String returns a human-readable name for the OrderBy value.
func (o OrderBy) String() string {
	switch o {
	case OrderByIngestTS:
		return "ingest_ts"
	case OrderBySourceTS:
		return "source_ts"
	}
	return "ingest_ts"
}

// RecordTS extracts the relevant timestamp from a record based on the ordering.
func (o OrderBy) RecordTS(rec chunk.Record) time.Time {
	switch o {
	case OrderByIngestTS:
		return rec.IngestTS
	case OrderBySourceTS:
		return rec.SourceTS
	}
	return rec.IngestTS
}

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
	// Primary time bounds on IngestTS.
	// Set via start=/end=/last=/ingest_start=/ingest_end= query directives.
	Start time.Time // inclusive lower bound on IngestTS
	End   time.Time // exclusive upper bound on IngestTS

	// Time bounds on SourceTS (optional runtime filters).
	SourceStart time.Time // inclusive lower bound on SourceTS
	SourceEnd   time.Time // exclusive upper bound on SourceTS

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
	OrderBy   OrderBy // timestamp field for ordering (default: IngestTS)
	IsReverse bool    // return results newest-first
	Limit     int     // max results (0 = unlimited)
	Pos       *uint64 // exact record position within chunk (nil = no filter)

	// Context windows (for SearchWithContext)
	ContextBefore int // number of records to include before each match
	ContextAfter  int // number of records to include after each match

	// ResumeTS is set internally when resuming a reordered chunk.
	// The reorder scanner skips records already past this timestamp.
	ResumeTS time.Time

	// SkipCloud skips cloud-backed chunks during search. Used by the
	// histogram to compute filtered counts from local data only.
	SkipCloud bool
}

// String returns a human-readable representation of the query including all parameters.
func (q Query) String() string {
	var parts []string
	if q.BoolExpr != nil {
		parts = append(parts, q.BoolExpr.String())
	}
	if !q.Start.IsZero() {
		parts = append(parts, "start="+q.Start.Format(time.RFC3339Nano))
	}
	if !q.End.IsZero() {
		parts = append(parts, "end="+q.End.Format(time.RFC3339Nano))
	}
	if q.IsReverse {
		parts = append(parts, "reverse=true")
	}
	if q.Limit > 0 {
		parts = append(parts, fmt.Sprintf("limit=%d", q.Limit))
	}
	if q.OrderBy != OrderByIngestTS {
		parts = append(parts, "order="+q.OrderBy.String())
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

// TimeBounds returns the effective lower and upper IngestTS bounds, accounting for reverse order.
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

// WithVaultPredicate returns a copy of the query with a vault_id=X predicate added.
// The predicate is ANDed with any existing BoolExpr.
func (q Query) WithVaultPredicate(vaultID string) Query {
	vaultPred := &querylang.PredicateExpr{
		Kind:  querylang.PredKV,
		Key:   "vault_id",
		Value: vaultID,
	}

	result := q
	if q.BoolExpr == nil {
		result.BoolExpr = vaultPred
	} else {
		result.BoolExpr = querylang.FlattenAnd(q.BoolExpr, vaultPred)
	}
	return result
}

// MultiVaultPosition represents a position within a specific vault's chunk.
type MultiVaultPosition struct {
	VaultID  uuid.UUID
	ChunkID  chunk.ChunkID
	Position uint64
	ResumeTS time.Time // non-zero for reordered chunks (no TS index)
}

// ResumeToken allows resuming a query from where it left off.
// VaultTokens maps vault IDs to opaque per-vault tokens. Each vault — local
// or remote — serializes its own resume state. The API node routes each
// token to wherever the vault lives.
type ResumeToken struct {
	// VaultTokens maps vault IDs to their opaque resume tokens.
	// For local vaults, these are deserialized into Positions by the search engine.
	// For remote vaults, they are forwarded as-is to the owning node.
	VaultTokens map[uuid.UUID][]byte

	// FrozenStart and FrozenEnd preserve the original query time bounds from
	// the first page. On subsequent pages, these override the re-parsed time
	// range (e.g., "last-5m" would shift between pages without this).
	FrozenStart time.Time
	FrozenEnd   time.Time

	// Positions contains the last yielded position for each vault/chunk combination.
	// This is the internal representation used by eng.Search() for local vaults.
	// Populated by deserializing the relevant VaultTokens entry.
	Positions []MultiVaultPosition

	// Legacy field for backward compatibility with single-vault resume tokens.
	//
	// Deprecated: use Positions instead.
	Next chunk.RecordRef
}

// Normalize converts a legacy resume token (using Next) to the new format (using Positions).
// If Positions is already populated, returns the token unchanged.
// The vaultID parameter is used for legacy tokens that don't include vault information.
func (t *ResumeToken) Normalize(defaultVaultID uuid.UUID) *ResumeToken {
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
		Positions: []MultiVaultPosition{{
			VaultID:  defaultVaultID,
			ChunkID:  t.Next.ChunkID,
			Position: t.Next.Pos,
		}},
	}
}

// ErrInvalidResumeToken is returned when a resume token references a chunk that no longer exists.
var ErrInvalidResumeToken = errors.New("invalid resume token: chunk no longer exists")

// ErrMultiVaultNotSupported is returned when an operation doesn't support multi-vault mode.
var ErrMultiVaultNotSupported = errors.New("operation not supported in multi-vault mode")

// recordWithRef combines a record with its reference for internal iteration.
// VaultID is included for multi-vault queries.
type recordWithRef struct {
	VaultID   uuid.UUID
	Record    chunk.Record
	Ref       chunk.RecordRef
	Reordered bool // true when yielded from reorder fallback (resume by IngestTS, not position)
}

// record returns the Record with Ref and VaultID populated.
func (rr recordWithRef) record() chunk.Record {
	rr.Record.Ref = rr.Ref
	rr.Record.VaultID = rr.VaultID
	return rr.Record
}

// Engine executes queries against chunk and index managers.
//
// The engine can operate in two modes:
//   - Single-vault mode: created with New(), queries one vault
//   - Multi-vault mode: created with NewWithRegistry(), queries across vaults
//
// Logging:
//   - Logger is dependency-injected via the constructor
//   - Engine owns its scoped logger (component="query-engine")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (search iteration, filtering)
type Engine struct {
	// Single-vault mode (legacy)
	chunks  chunk.ChunkManager
	indexes index.IndexManager

	// Multi-vault mode
	registry VaultRegistry

	// Lookup enrichment resolver (optional). Set via SetLookupResolver.
	lookupResolver lookup.Resolver

	// Logger for this engine instance.
	// Scoped with component="query-engine" at construction time.
	logger *slog.Logger
}

// New creates a query engine backed by the given chunk and index managers.
// This creates a single-vault engine for backward compatibility.
// If logger is nil, logging is disabled.
func New(chunks chunk.ChunkManager, indexes index.IndexManager, logger *slog.Logger) *Engine {
	return &Engine{
		chunks:  chunks,
		indexes: indexes,
		logger:  logging.Default(logger).With("component", "query-engine"),
	}
}

// NewWithRegistry creates a query engine that can search across multiple vaults.
// Vault predicates in queries (e.g., "vault_id=<uuid>") filter which vaults are searched.
// If no vault predicate is present, all vaults are searched.
// If logger is nil, logging is disabled.
func NewWithRegistry(registry VaultRegistry, logger *slog.Logger) *Engine {
	return &Engine{
		registry: registry,
		logger:   logging.Default(logger).With("component", "query-engine"),
	}
}

// SetLookupResolver sets the lookup resolver used by pipeline lookup operators.
func (e *Engine) SetLookupResolver(r lookup.Resolver) {
	e.lookupResolver = r
}

// isMultiVault returns true if this engine operates in multi-vault mode.
func (e *Engine) isMultiVault() bool {
	return e.registry != nil
}

// getVaultManagers returns the chunk and index managers for a vault.
// For single-vault mode, vaultID is ignored.
func (e *Engine) getVaultManagers(vaultID uuid.UUID) (chunk.ChunkManager, index.IndexManager) {
	if e.registry != nil {
		return e.registry.ChunkManager(vaultID), e.registry.IndexManager(vaultID)
	}
	return e.chunks, e.indexes
}

// listVaults returns all vault IDs this engine can query.
func (e *Engine) listVaults() []uuid.UUID {
	if e.registry != nil {
		return e.registry.ListVaults()
	}
	return []uuid.UUID{uuid.Nil}
}

// selectChunks filters to chunks that overlap the query time range,
// sorted by WriteStart (ascending for forward, descending for reverse).
// Unsealed chunks are always included (their WriteEnd is not final).
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
	sortChunks(out, q.OrderBy, q.Reverse())
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
	// Skip cloud chunks if requested (histogram local-only scan).
	if q.SkipCloud && m.CloudBacked {
		return false
	}
	// Skip archived chunks — they can't be read without a restore operation.
	if m.Archived {
		return false
	}
	// Primary filter: IngestTS overlap.
	// lower/upper come from Query.Start/End which are IngestTS bounds.
	if !lower.IsZero() && !m.IngestEnd.IsZero() && m.IngestEnd.Before(lower) {
		return false
	}
	if !upper.IsZero() && !m.IngestStart.IsZero() && !m.IngestStart.Before(upper) {
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

// sortChunks sorts chunks by the appropriate timestamp bounds based on OrderBy.
func sortChunks(out []chunk.ChunkMeta, orderBy OrderBy, reverse bool) {
	startTS := func(m chunk.ChunkMeta) time.Time {
		switch orderBy { //nolint:exhaustive // IngestTS is the default
		case OrderBySourceTS:
			return m.SourceStart
		default:
			return m.IngestStart
		}
	}
	if reverse {
		slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
			return startTS(b).Compare(startTS(a)) // descending
		})
	} else {
		slices.SortFunc(out, func(a, b chunk.ChunkMeta) int {
			return startTS(a).Compare(startTS(b)) // ascending
		})
	}
}

// searchChunkWithRef returns an iterator over records in a single chunk, including their refs.
// vaultID identifies which vault the chunk belongs to (for multi-vault queries).
// startPos allows resuming from a specific position within the chunk.
// Unsealed chunks are scanned sequentially without indexes.
func (e *Engine) searchChunkWithRef(ctx context.Context, q Query, vaultID uuid.UUID, meta chunk.ChunkMeta, startPos *uint64) iter.Seq2[recordWithRef, error] {
	return func(yield func(recordWithRef, error) bool) {
		cm, im := e.getVaultManagers(vaultID)
		if cm == nil {
			yield(recordWithRef{}, errors.New("vault not found: "+vaultID.String()))
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
		scanner, err := e.buildScannerWithManagers(ctx, cursor, q, vaultID, meta, startPos, cm, im)
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
			rr.Record.VaultID = rr.VaultID
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
// vaultID is included in the returned recordWithRef for multi-vault queries.
//
// When OrderBy != OrderByWriteTS, sealed chunks use TS-index-ordered scanning:
// the TS index is walked in timestamp order, producing positions in TS order
// rather than physical order. For active chunks, results are buffered and sorted.
func (e *Engine) buildScannerWithManagers(ctx context.Context, cursor chunk.RecordCursor, q Query, vaultID uuid.UUID, meta chunk.ChunkMeta, startPos *uint64, cm chunk.ChunkManager, im index.IndexManager) (iter.Seq2[recordWithRef, error], error) {
	b := newScannerBuilder(meta.ID)
	b.vaultID = vaultID

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

	// Records are stored in physical write order but must be yielded in
	// IngestTS or SourceTS order. Always go through TS-ordered scanning.
	return e.buildTSOrderedScanner(ctx, cursor, q, b, meta, startPos, cm, im)
}

// buildTSOrderedScanner creates a scanner that yields records in TS-index order.
// For sealed chunks with a TS index, it walks the index to produce positions in
// timestamp order. For active chunks (or when the index is unavailable), it falls
// back to buffering and sorting.
func (e *Engine) buildTSOrderedScanner(ctx context.Context, cursor chunk.RecordCursor, q Query, b *scannerBuilder, meta chunk.ChunkMeta, startPos *uint64, cm chunk.ChunkManager, im index.IndexManager) (iter.Seq2[recordWithRef, error], error) {
	if meta.Sealed {
		// Try to load the TS index for this ordering.
		// The index manager handles local sealed chunks; the chunk manager
		// handles cloud chunks (via locally-cached TS index files).
		tsEntries, err := loadTSEntries(im, meta.ID, q.OrderBy)
		if err != nil && meta.CloudBacked {
			tsEntries, err = loadCloudTSEntries(cm, meta.ID, q.OrderBy)
		}
		if err == nil {
			e.logger.Debug("✅ TS index scanner activated", "chunk", meta.ID, "entries", len(tsEntries), "cloud", meta.CloudBacked)
			return buildTSIndexScanner(ctx, cursor, q, b, meta, tsEntries)
		}
		e.logger.Debug("❌ TS index unavailable, falling back to reorder buffer", "chunk", meta.ID, "cloud", meta.CloudBacked, "error", err, "isNotFound", errors.Is(err, index.ErrIndexNotFound), "isNoTS", errors.Is(err, chunk.ErrNoTSIndex))
		// Fall through to buffer-and-sort if index unavailable.
	}

	// Active chunk or index unavailable: build normal scanner then buffer & sort.
	innerQ := q
	if meta.Sealed {
		// For sealed chunks without a TS index, the sequential scanner reads
		// in WriteTS order and applies IngestTS time bounds — stopping early
		// when it hits a record with IngestTS before the lower bound. But
		// WriteTS and IngestTS can differ (forwarded records), so the scanner
		// misses records that are within the time range but out of WriteTS
		// order. Strip time bounds from the inner scanner and apply them
		// after sorting by the correct TS field.
		// Preserve the reverse flag explicitly since Reverse() has a legacy
		// fallback that inspects Start/End ordering.
		innerQ.IsReverse = q.Reverse()
		innerQ.Start = time.Time{}
		innerQ.End = time.Time{}
	}

	if b.isSequential() && b.hasMinPos && startPos == nil && !q.Reverse() {
		if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: b.minPos}); err != nil {
			return nil, err
		}
	}
	inner := b.build(ctx, cursor, innerQ)
	if meta.Sealed {
		lower, upper := q.TimeBounds()
		return reorderByTSWithBounds(inner, q.OrderBy, q.Reverse(), lower, upper, q.ResumeTS), nil
	}
	return reorderByTS(inner, q.OrderBy, q.Reverse()), nil
}

// loadCloudTSEntries loads TS index entries from the chunk manager's cloud
// TS cache via the TSIndexLoader interface. Returns index.ErrIndexNotFound
// if the chunk manager doesn't support it.
func loadCloudTSEntries(cm chunk.ChunkManager, chunkID chunk.ChunkID, orderBy OrderBy) ([]index.TSEntry, error) {
	loader, ok := cm.(chunk.TSIndexLoader)
	if !ok {
		return nil, index.ErrIndexNotFound
	}
	var entries []chunk.TSEntry
	var err error
	switch orderBy { //nolint:exhaustive // IngestTS is the default
	case OrderBySourceTS:
		entries, err = loader.LoadSourceEntries(chunkID)
	default:
		entries, err = loader.LoadIngestEntries(chunkID)
	}
	if err != nil {
		return nil, err
	}
	// Convert chunk.TSEntry → index.TSEntry (same layout, different packages).
	out := make([]index.TSEntry, len(entries))
	for i, e := range entries {
		out[i] = index.TSEntry{TS: e.TS, Pos: e.Pos}
	}
	return out, nil
}

// loadTSEntries loads the appropriate TS index entries based on OrderBy.
func loadTSEntries(im index.IndexManager, chunkID chunk.ChunkID, orderBy OrderBy) ([]index.TSEntry, error) {
	switch orderBy { //nolint:exhaustive // IngestTS is the default
	case OrderBySourceTS:
		return im.LoadSourceEntries(chunkID)
	default:
		return im.LoadIngestEntries(chunkID)
	}
}

// buildTSIndexScanner creates a position scanner from TS-index-ordered entries.
// It prunes entries by time bounds, intersects with any existing index positions
// (using a set to preserve TS order), and builds a position scanner with time
// bounds checking disabled (pruning already handled).
func buildTSIndexScanner(ctx context.Context, cursor chunk.RecordCursor, q Query, b *scannerBuilder, meta chunk.ChunkMeta, tsEntries []index.TSEntry) (iter.Seq2[recordWithRef, error], error) {
	// Prune by time bounds (entries are sorted by TS, use binary search).
	tsEntries = pruneTSEntriesByBounds(tsEntries, q)

	if len(tsEntries) == 0 {
		return emptyScanner(), nil
	}

	// If we have index-narrowed positions from token/KV lookups, intersect
	// using a set (not sorted merge) to preserve TS order.
	var tsPositions []uint64
	if b.positions != nil {
		posSet := make(map[uint64]struct{}, len(b.positions))
		for _, p := range b.positions {
			posSet[p] = struct{}{}
		}
		for _, e := range tsEntries {
			if _, ok := posSet[uint64(e.Pos)]; ok {
				tsPositions = append(tsPositions, uint64(e.Pos))
			}
		}
	} else {
		tsPositions = make([]uint64, len(tsEntries))
		for i, e := range tsEntries {
			tsPositions[i] = uint64(e.Pos)
		}
	}

	if len(tsPositions) == 0 {
		return emptyScanner(), nil
	}

	// Replace positions with TS-ordered ones and disable time bounds checking
	// (already handled by pruneTSEntriesByBounds).
	b.positions = tsPositions
	b.skipTimeBounds = true
	return b.build(ctx, cursor, q), nil
}

// pruneTSEntriesByBounds filters TS index entries to those within the query time bounds.
// The entries are sorted by TS, so we use binary search for lower and upper bounds.
func pruneTSEntriesByBounds(entries []index.TSEntry, q Query) []index.TSEntry {
	lower, upper := q.TimeBounds()

	// Binary search for lower bound.
	if !lower.IsZero() {
		lowerNano := lower.UnixNano()
		lo := sort.Search(len(entries), func(i int) bool {
			return entries[i].TS >= lowerNano
		})
		entries = entries[lo:]
	}

	// Binary search for upper bound (exclusive).
	if !upper.IsZero() {
		upperNano := upper.UnixNano()
		hi := sort.Search(len(entries), func(i int) bool {
			return entries[i].TS >= upperNano
		})
		entries = entries[:hi]
	}

	return entries
}

// setMinPositionsFromBounds sets the scanner builder's minimum position from time bounds
// using IngestTS and SourceTS indexes (sealed flat index or active chunk B+ tree).
func setMinPositionsFromBounds(b *scannerBuilder, q Query, meta chunk.ChunkMeta, cm chunk.ChunkManager, im index.IndexManager) {
	// IngestTS: use sealed flat index or active chunk B+ tree.
	lower, _ := q.TimeBounds()
	seekIngestTS(b, lower, meta, cm, im)
	seekSourceTS(b, q.SourceStart, meta, cm, im)
}

func seekIngestTS(b *scannerBuilder, lower time.Time, meta chunk.ChunkMeta, cm chunk.ChunkManager, im index.IndexManager) {
	if lower.IsZero() {
		return
	}
	if meta.Sealed {
		if pos, found, err := im.FindIngestStartPosition(meta.ID, lower); err == nil && found {
			b.setMinPosition(pos)
			return
		}
		// Fallback: chunk manager handles cloud chunks with locally-cached TS index.
		if pos, found, err := cm.FindIngestStartPosition(meta.ID, lower); err == nil && found {
			b.setMinPosition(pos)
		}
		return
	}
	if pos, found, err := cm.FindIngestStartPosition(meta.ID, lower); err == nil && found {
		b.setMinPosition(pos)
	}
}

func seekSourceTS(b *scannerBuilder, sourceStart time.Time, meta chunk.ChunkMeta, cm chunk.ChunkManager, im index.IndexManager) {
	if sourceStart.IsZero() {
		return
	}
	if meta.Sealed {
		if pos, found, err := im.FindSourceStartPosition(meta.ID, sourceStart); err == nil && found {
			b.setMinPosition(pos)
			return
		}
		// Fallback: chunk manager handles cloud chunks with locally-cached TS index.
		if pos, found, err := cm.FindSourceStartPosition(meta.ID, sourceStart); err == nil && found {
			b.setMinPosition(pos)
		}
		return
	}
	if pos, found, err := cm.FindSourceStartPosition(meta.ID, sourceStart); err == nil && found {
		b.setMinPosition(pos)
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
