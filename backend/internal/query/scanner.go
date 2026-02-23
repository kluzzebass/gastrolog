package query

import (
	"bytes"
	"context"
	"errors"
	"iter"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/querylang"
	"gastrolog/internal/tokenizer"

	"github.com/google/uuid"
)

// kvExtractors is the default set of KV extractors used by runtime filters.
// Must match the extractors registered in the KV indexer factories.
var kvExtractors = tokenizer.DefaultExtractors()

// pipeEval is a shared, stateless evaluator for expression predicates.
// It's goroutine-safe because Eval only reads from the function registry.
var pipeEval = querylang.NewEvaluator()

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
	storeID   uuid.UUID // store ID for multi-store queries
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

// addFilter adds a runtime filter that will be applied to each record.
// Filters are applied in the order they are added, so callers should add
// cheap filters (e.g., source ID check) before expensive ones (e.g., tokenization).
func (b *scannerBuilder) addFilter(f recordFilter) {
	b.filters = append(b.filters, f)
}

// excludePosition removes a specific position (used for resume to skip already-returned record).
func (b *scannerBuilder) excludePosition(pos uint64, reverse bool) {
	if len(b.positions) == 0 {
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
func (b *scannerBuilder) build(ctx context.Context, cursor chunk.RecordCursor, q Query) iter.Seq2[recordWithRef, error] {
	if b.hasNoMatches() {
		return emptyScanner()
	}

	if b.isSequential() {
		return b.buildSequentialScanner(ctx, cursor, q)
	}

	return b.buildPositionScanner(ctx, cursor, q)
}

// buildSequentialScanner creates a scanner that reads records sequentially.
func (b *scannerBuilder) buildSequentialScanner(ctx context.Context, cursor chunk.RecordCursor, q Query) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()
	filters := b.filters
	storeID := b.storeID

	if q.Reverse() {
		return func(yield func(recordWithRef, error) bool) {
			n := 0
			for {
				if n&1023 == 0 {
					if err := ctx.Err(); err != nil {
						yield(recordWithRef{StoreID: storeID}, err)
						return
					}
				}
				n++

				rec, ref, err := cursor.Prev()
				if errors.Is(err, chunk.ErrNoMoreRecords) {
					return
				}
				if err != nil {
					yield(recordWithRef{StoreID: storeID, Ref: ref}, err)
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

				if !yield(recordWithRef{StoreID: storeID, Record: rec, Ref: ref}, nil) {
					return
				}
			}
		}
	}

	return func(yield func(recordWithRef, error) bool) {
		n := 0
		for {
			if n&1023 == 0 {
				if err := ctx.Err(); err != nil {
					yield(recordWithRef{StoreID: storeID}, err)
					return
				}
			}
			n++

			rec, ref, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(recordWithRef{StoreID: storeID, Ref: ref}, err)
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

			if !yield(recordWithRef{StoreID: storeID, Record: rec, Ref: ref}, nil) {
				return
			}
		}
	}
}

// buildPositionScanner creates a scanner that seeks to specific positions.
func (b *scannerBuilder) buildPositionScanner(ctx context.Context, cursor chunk.RecordCursor, q Query) iter.Seq2[recordWithRef, error] {
	lower, upper := q.TimeBounds()
	positions := b.positions
	chunkID := b.chunkID
	storeID := b.storeID
	filters := b.filters

	if q.Reverse() {
		return func(yield func(recordWithRef, error) bool) {
			for i := len(positions) - 1; i >= 0; i-- {
				if i&1023 == 0 {
					if err := ctx.Err(); err != nil {
						yield(recordWithRef{StoreID: storeID}, err)
						return
					}
				}

				pos := positions[i]
				ref := chunk.RecordRef{ChunkID: chunkID, Pos: pos}
				if err := cursor.Seek(ref); err != nil {
					yield(recordWithRef{StoreID: storeID, Ref: ref}, err)
					return
				}

				rec, ref, err := cursor.Next()
				if errors.Is(err, chunk.ErrNoMoreRecords) {
					return
				}
				if err != nil {
					yield(recordWithRef{StoreID: storeID, Ref: ref}, err)
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

				if !yield(recordWithRef{StoreID: storeID, Record: rec, Ref: ref}, nil) {
					return
				}
			}
		}
	}

	return func(yield func(recordWithRef, error) bool) {
		for i, pos := range positions {
			if i&1023 == 0 {
				if err := ctx.Err(); err != nil {
					yield(recordWithRef{StoreID: storeID}, err)
					return
				}
			}

			ref := chunk.RecordRef{ChunkID: chunkID, Pos: pos}
			if err := cursor.Seek(ref); err != nil {
				yield(recordWithRef{StoreID: storeID, Ref: ref}, err)
				return
			}

			rec, ref, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				return
			}
			if err != nil {
				yield(recordWithRef{StoreID: storeID, Ref: ref}, err)
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

			if !yield(recordWithRef{StoreID: storeID, Record: rec, Ref: ref}, nil) {
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

// tokenFilter returns a filter that matches records containing all given tokens.
func tokenFilter(tokens []string) recordFilter {
	return func(rec chunk.Record) bool {
		return matchesTokens(rec.Raw, tokens)
	}
}

// matchesTokens checks if the record's raw data contains all query tokens.
// Indexable tokens are matched against the tokenized record; non-indexable tokens
// (containing dots, etc.) fall back to case-insensitive substring search.
func matchesTokens(raw []byte, queryTokens []string) bool {
	if len(queryTokens) == 0 {
		return true
	}

	var recordTokens []string
	var rawLower []byte

	for _, qt := range queryTokens {
		qtLower := strings.ToLower(qt)
		if tokenizer.IsIndexable(qtLower) {
			if recordTokens == nil {
				recordTokens = tokenizer.Tokens(raw)
			}
			if !slices.Contains(recordTokens, qtLower) {
				return false
			}
		} else {
			if rawLower == nil {
				rawLower = bytes.ToLower(raw)
			}
			if !bytes.Contains(rawLower, []byte(qtLower)) {
				return false
			}
		}
	}
	return true
}

// keyValueFilter returns a filter that matches records where all key=value pairs
// are found in either the record's attributes OR the message body.
func keyValueFilter(filters []KeyValueFilter) recordFilter {
	return func(rec chunk.Record) bool {
		return matchesKeyValue(rec.Attrs, rec.Raw, filters)
	}
}

// regexFilter returns a filter that matches records whose raw line matches the pattern.
func regexFilter(pattern *regexp.Regexp) recordFilter {
	return func(rec chunk.Record) bool {
		return pattern.Match(rec.Raw)
	}
}

// matchesKeyValue checks if all query key=value pairs are found in either
// the record's attributes or the message body (OR semantics per pair, AND across pairs).
//
// Supports wildcard patterns:
//   - Key="foo", Value="bar" - exact match for foo=bar
//   - Key="foo", Value=""    - match if key "foo" exists (any value)
//   - Key="", Value="bar"    - match if any key has value "bar"
func matchesKeyValue(recAttrs chunk.Attributes, raw []byte, queryFilters []KeyValueFilter) bool {
	if len(queryFilters) == 0 {
		return true
	}

	// Lazily extract key=value pairs from message body only if needed.
	var msgPairs map[string]map[string]struct{}
	var msgValues map[string]struct{} // all values (for *=value pattern)
	getMsgPairs := func() map[string]map[string]struct{} {
		if msgPairs == nil {
			pairs := tokenizer.CombinedExtract(raw, kvExtractors)
			msgPairs = make(map[string]map[string]struct{})
			msgValues = make(map[string]struct{})
			for _, kv := range pairs {
				if msgPairs[kv.Key] == nil {
					msgPairs[kv.Key] = make(map[string]struct{})
				}
				// Keys are already lowercase from extractor, values are preserved.
				// For matching, we lowercase both.
				valLower := strings.ToLower(kv.Value)
				msgPairs[kv.Key][valLower] = struct{}{}
				msgValues[valLower] = struct{}{}
			}
		}
		return msgPairs
	}
	getMsgValues := func() map[string]struct{} {
		getMsgPairs() // ensure msgValues is populated
		return msgValues
	}

	// Check all filters (AND semantics across filters).
	for _, f := range queryFilters {
		keyLower := strings.ToLower(f.Key)
		valLower := strings.ToLower(f.Value)

		if f.Key == "" && f.Value == "" {
			// Both empty - matches everything, skip this filter
			continue
		} else if f.Value == "" {
			// Key only: key=* pattern (key exists with any value)
			// Check attrs
			found := false
			for k := range recAttrs {
				if matchStringOrPat(k, f.Key, f.KeyPat) {
					found = true
					break
				}
			}
			if found {
				continue
			}
			// Check message body
			pairs := getMsgPairs()
			if f.KeyPat != nil {
				// Glob key: scan all keys
				for k := range pairs {
					if f.KeyPat.MatchString(k) {
						found = true
						break
					}
				}
			} else if _, ok := pairs[keyLower]; ok {
				found = true
			}
			if found {
				continue
			}
			// Check structural JSON paths.
			if f.KeyPat == nil && matchJSONPathExists(raw, f.Key) {
				continue
			}
			return false // Key not found in either location
		} else if f.Key == "" {
			// Value only: *=value pattern (any key has this value)
			// Check attrs
			found := false
			for _, v := range recAttrs {
				if matchStringOrPat(v, f.Value, f.ValuePat) {
					found = true
					break
				}
			}
			if found {
				continue
			}
			// Check message body
			if f.ValuePat != nil {
				values := getMsgValues()
				for v := range values {
					if f.ValuePat.MatchString(v) {
						found = true
						break
					}
				}
			} else {
				values := getMsgValues()
				if _, ok := values[valLower]; ok {
					found = true
				}
			}
			if found {
				continue
			}
			return false // Value not found in either location
		} else if f.Op != querylang.OpEq {
			// Comparison operator (!=, >, >=, <, <=): use compareValues.
			found := false
			for k, v := range recAttrs {
				if strings.EqualFold(k, f.Key) && compareValues(v, f.Value, f.Op) {
					found = true
					break
				}
			}
			if !found {
				pairs := getMsgPairs()
				for k, values := range pairs {
					if k != keyLower {
						continue
					}
					for v := range values {
						if compareValues(v, f.Value, f.Op) {
							found = true
							break
						}
					}
					if found {
						break
					}
				}
			}
			if !found {
				found = matchJSONPathCompare(raw, f.Key, f.Value, f.Op)
			}
			if !found {
				return false
			}
		} else {
			// Both key and value: exact or glob key=value match
			// Check attributes first (cheaper).
			found := false
			for k, v := range recAttrs {
				if matchStringOrPat(k, f.Key, f.KeyPat) && matchStringOrPat(v, f.Value, f.ValuePat) {
					found = true
					break
				}
			}
			if found {
				continue
			}

			// Check message body.
			pairs := getMsgPairs()
			for k, values := range pairs {
				if !matchStringOrPatLower(k, keyLower, f.KeyPat) {
					continue
				}
				for v := range values {
					if matchStringOrPatLower(v, valLower, f.ValuePat) {
						found = true
						break
					}
				}
				if found {
					break
				}
			}

			if found {
				continue
			}
			// Check structural JSON paths.
			if f.KeyPat == nil && f.ValuePat == nil && matchJSONPathValue(raw, f.Key, valLower) {
				continue
			}
			return false // Not found in either location.
		}
	}
	return true
}

// matchStringOrPat matches a string against either an exact value (case-insensitive) or a compiled glob pattern.
func matchStringOrPat(s, exact string, pat *regexp.Regexp) bool {
	if pat != nil {
		return pat.MatchString(s)
	}
	return strings.EqualFold(s, exact)
}

// matchStringOrPatLower matches a lowercased string against either a lowercased exact value or a compiled glob pattern.
func matchStringOrPatLower(s, exactLower string, pat *regexp.Regexp) bool {
	if pat != nil {
		return pat.MatchString(s)
	}
	return s == exactLower
}

// globTokenFilter returns a filter that matches records where at least one token
// or whitespace-delimited word matches all the given glob patterns (AND semantics).
// Tokens are checked first (cheap, from tokenizer). If no token match, falls back
// to whitespace-delimited words from the raw line for cross-token matches like
// com*controller matching com.example.controller.
func globTokenFilter(globs []GlobFilter) recordFilter {
	return func(rec chunk.Record) bool {
		recordTokens := tokenizer.Tokens(rec.Raw)
		for _, g := range globs {
			if !matchGlobTokensOrRaw(recordTokens, rec.Raw, g.Pattern) {
				return false
			}
		}
		return true
	}
}

// matchesSingleGlob checks if a record matches a glob pattern against
// tokenized words first, then whitespace-delimited words from the raw line.
func matchesSingleGlob(raw []byte, pattern *regexp.Regexp) bool {
	recordTokens := tokenizer.Tokens(raw)
	return matchGlobTokensOrRaw(recordTokens, raw, pattern)
}

// matchGlobTokensOrRaw checks tokens first, then falls back to whitespace-delimited
// words from the raw line. This allows globs to match across tokenizer boundaries
// (e.g., com*controller matching com.example.controller).
func matchGlobTokensOrRaw(tokens []string, raw []byte, pattern *regexp.Regexp) bool {
	// Fast path: check tokenized words.
	if slices.ContainsFunc(tokens, pattern.MatchString) {
		return true
	}

	// Slow path: check whitespace-delimited words from the raw line.
	// This catches cross-token patterns like com*controller matching
	// com.example.controller which the tokenizer splits into [com, example, controller].
	for word := range rawWords(raw) {
		if pattern.Match(word) {
			return true
		}
	}

	return false
}

// rawWords yields whitespace-delimited byte slices from raw.
func rawWords(raw []byte) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		i := 0
		for i < len(raw) {
			// Skip whitespace.
			for i < len(raw) && (raw[i] == ' ' || raw[i] == '\t' || raw[i] == '\n' || raw[i] == '\r') {
				i++
			}
			if i >= len(raw) {
				return
			}
			// Find end of word.
			start := i
			for i < len(raw) && raw[i] != ' ' && raw[i] != '\t' && raw[i] != '\n' && raw[i] != '\r' {
				i++
			}
			if !yield(raw[start:i]) {
				return
			}
		}
	}
}

// applyGlobIndex tries to use the token index for glob pattern filtering.
// For prefix globs (e.g., "error*"), uses LookupPrefix for index acceleration.
// Returns (true, false) if index was used and has matches.
// Returns (true, true) if index was used but no matches exist.
// Returns (false, false) if index unavailable or glob has no usable prefix.
func applyGlobIndex(b *scannerBuilder, indexes index.IndexManager, chunkID chunk.ChunkID, globs []GlobFilter) (ok bool, empty bool) {
	if len(globs) == 0 {
		return true, false
	}

	tokIdx, err := indexes.OpenTokenIndex(chunkID)
	if errors.Is(err, index.ErrIndexNotFound) || err != nil {
		return false, false
	}

	reader := index.NewTokenIndexReader(chunkID, tokIdx.Entries())
	anyUsedIndex := false

	for _, g := range globs {
		prefix, hasPrefix := querylang.ExtractGlobPrefix(g.RawPattern)
		if !hasPrefix {
			continue // no prefix — can't use index for this glob
		}

		// Use prefix lookup to get candidate positions.
		positions, found := reader.LookupPrefix(prefix)
		if !found {
			// No tokens with this prefix exist in the chunk.
			return true, true
		}

		if !b.addPositions(positions) {
			return true, true
		}
		anyUsedIndex = true
	}

	if !anyUsedIndex {
		return false, false
	}
	return true, false
}

// Index application functions. Each returns true if it contributed positions,
// false if the index wasn't available (caller should add a runtime filter).

// applyTokenIndex tries to use the token index for position filtering.
// Returns (true, false) if all tokens found in index and positions added.
// Returns (false, false) if index unavailable or any token not in index (caller should use runtime filter).
// The token index is selective - not all tokens are indexed, so a miss means
// "can't use index" not "no matches exist".
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

	// All tokens must be present in the index (AND semantics).
	for i, tok := range tokens {
		tok = strings.ToLower(tok)
		positions, found := reader.Lookup(tok)
		if !found {
			// Token not in index. If the tokenizer would have indexed this
			// token (valid ASCII, right length, not numeric/UUID), then its
			// absence means zero records contain it — skip the chunk entirely.
			// If the tokenizer would have rejected it, we can't know from the
			// index alone — fall back to runtime filtering.
			if tokenizer.IsIndexable(tok) {
				return true, true // definitive: no matches
			}
			return false, false // not indexable: need runtime filter
		}
		if i == 0 {
			if !b.addPositions(positions) {
				// Intersection resulted in empty set - no matches
				return true, true
			}
		} else {
			// Intersect with existing positions.
			if !b.addPositions(positions) {
				// Intersection resulted in empty set - no matches
				return true, true
			}
		}
	}

	return true, false
}

// applyKeyValueIndex tries to use both attr and kv indexes for position filtering.
// For each filter, it unions positions from both indexes (OR semantics within a filter).
// Across filters, it intersects positions (AND semantics).
//
// Supports wildcard patterns:
//   - Key="foo", Value="bar" - uses KV index for exact key=value match
//   - Key="foo", Value=""    - uses Key index for key existence
//   - Key="", Value="bar"    - uses Value index for value existence
//
// Returns (true, false) if indexes were used and have matches.
// Returns (true, true) if indexes were used but no matches exist.
// Returns (false, false) if indexes not available (caller should add runtime filter).
func applyKeyValueIndex(b *scannerBuilder, indexes index.IndexManager, chunkID chunk.ChunkID, filters []KeyValueFilter) (ok bool, empty bool) {
	if len(filters) == 0 {
		return true, false
	}

	// Open all indexes we might need. We'll check availability per-filter.
	// Attr indexes (authoritative)
	attrKVIdx, attrKVErr := indexes.OpenAttrKVIndex(chunkID)
	attrKeyIdx, attrKeyErr := indexes.OpenAttrKeyIndex(chunkID)
	attrValIdx, attrValErr := indexes.OpenAttrValueIndex(chunkID)

	// KV indexes (heuristic, from message body)
	kvIdx, kvStatus, kvErr := indexes.OpenKVIndex(chunkID)
	kvKeyIdx, kvKeyStatus, kvKeyErr := indexes.OpenKVKeyIndex(chunkID)
	kvValIdx, kvValStatus, kvValErr := indexes.OpenKVValueIndex(chunkID)

	// JSON structural index
	jsonPathIdx, jsonPathStatus, jsonPathErr := indexes.OpenJSONPathIndex(chunkID)
	jsonPVIdx, jsonPVStatus, jsonPVErr := indexes.OpenJSONPVIndex(chunkID)
	var jsonReader *index.JSONIndexReader
	if jsonPathErr == nil || jsonPVErr == nil {
		var pathEntries []index.JSONPathIndexEntry
		var pvEntries []index.JSONPVIndexEntry
		if jsonPathErr == nil {
			pathEntries = jsonPathIdx.Entries()
		}
		if jsonPVErr == nil {
			pvEntries = jsonPVIdx.Entries()
		}
		jsonReader = index.NewJSONIndexReader(chunkID, pathEntries, jsonPathStatus, pvEntries, jsonPVStatus)
	}

	// For each filter, union positions from both attr and kv indexes.
	// Across filters, intersect positions.
	for _, f := range filters {
		keyLower := strings.ToLower(f.Key)
		valLower := strings.ToLower(f.Value)

		var filterPositions []uint64

		if f.Key == "" && f.Value == "" {
			// Both empty - matches everything, skip this filter
			continue
		} else if f.Value == "" {
			// Key only: key=* pattern (key exists with any value)
			// Use key indexes
			if attrKeyErr == nil {
				reader := index.NewAttrKeyIndexReader(chunkID, attrKeyIdx.Entries())
				if positions, found := reader.Lookup(keyLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			if kvKeyErr == nil && kvKeyStatus != index.KVCapped {
				reader := index.NewKVKeyIndexReader(chunkID, kvKeyIdx.Entries())
				if positions, found := reader.Lookup(keyLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			// JSON path index: key existence (dots become null-byte separators)
			if jsonReader != nil {
				jsonPath := dotToNull(keyLower)
				if positions, found := jsonReader.LookupPath(jsonPath); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
		} else if f.Key == "" {
			// Value only: *=value pattern (any key has this value)
			// Use value indexes
			if attrValErr == nil {
				reader := index.NewAttrValueIndexReader(chunkID, attrValIdx.Entries())
				if positions, found := reader.Lookup(valLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			if kvValErr == nil && kvValStatus != index.KVCapped {
				reader := index.NewKVValueIndexReader(chunkID, kvValIdx.Entries())
				if positions, found := reader.Lookup(valLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			// No JSON index for value-only queries (value without path context is ambiguous)
		} else if f.Op != querylang.OpEq {
			// Non-eq comparison operator: use key-only index to find positions
			// where the key exists, then runtime filter on value comparison.
			if attrKeyErr == nil {
				reader := index.NewAttrKeyIndexReader(chunkID, attrKeyIdx.Entries())
				if positions, found := reader.Lookup(keyLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			if kvKeyErr == nil && kvKeyStatus != index.KVCapped {
				reader := index.NewKVKeyIndexReader(chunkID, kvKeyIdx.Entries())
				if positions, found := reader.Lookup(keyLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			if jsonReader != nil {
				jsonPath := dotToNull(keyLower)
				if positions, found := jsonReader.LookupPath(jsonPath); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
		} else {
			// Both key and value: exact key=value match
			// Use KV indexes
			if attrKVErr == nil {
				reader := index.NewAttrKVIndexReader(chunkID, attrKVIdx.Entries())
				if positions, found := reader.Lookup(keyLower, valLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			if kvErr == nil && kvStatus != index.KVCapped {
				reader := index.NewKVIndexReader(chunkID, kvIdx.Entries())
				if positions, found := reader.Lookup(keyLower, valLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
			// JSON path-value index (dots become null-byte separators)
			if jsonReader != nil && jsonReader.PVStatus() != index.JSONCapped {
				jsonPath := dotToNull(keyLower)
				if positions, found := jsonReader.LookupPathValue(jsonPath, valLower); found {
					filterPositions = unionPositions(filterPositions, positions)
				}
			}
		}

		// If no positions found for this filter, fall back to runtime filtering.
		// KV indexes are accelerators, not authorities - an index miss does NOT
		// imply no matching records exist. The (key,value) pair might not have
		// been admitted to the index due to budget limits or cardinality caps.
		if len(filterPositions) == 0 {
			return false, false
		}

		if !b.addPositions(filterPositions) {
			return true, true
		}
	}

	return true, false
}

// GlobFilter represents a glob pattern filter for index acceleration.
type GlobFilter struct {
	Pattern    *regexp.Regexp // compiled glob pattern
	RawPattern string         // original glob string (for explain plan)
}

// ConjunctionToFilters converts a DNF conjunction to tokens, KV filters, glob filters,
// regex runtime filter, and a negation filter.
// Positive predicates are returned as tokens/KV/globs for index acceleration.
// Regex predicates are always runtime-only (no index acceleration).
// Negative predicates are returned as a runtime filter.
func ConjunctionToFilters(conj *querylang.Conjunction) (tokens []string, kv []KeyValueFilter, globs []GlobFilter, negFilter recordFilter) {
	var regexFilters []recordFilter

	// Extract positive predicates for index acceleration
	var exprFilters []recordFilter

	for _, p := range conj.Positive {
		switch p.Kind {
		case querylang.PredToken:
			tokens = append(tokens, p.Value)
		case querylang.PredKV:
			kv = append(kv, KeyValueFilter{Key: p.Key, Value: p.Value, KeyPat: p.KeyPat, ValuePat: p.ValuePat, Op: p.Op})
		case querylang.PredKeyExists:
			kv = append(kv, KeyValueFilter{Key: p.Key, Value: "", KeyPat: p.KeyPat})
		case querylang.PredValueExists:
			kv = append(kv, KeyValueFilter{Key: "", Value: p.Value, ValuePat: p.ValuePat})
		case querylang.PredRegex:
			regexFilters = append(regexFilters, regexFilter(p.Pattern))
		case querylang.PredGlob:
			globs = append(globs, GlobFilter{Pattern: p.Pattern, RawPattern: p.Value})
		case querylang.PredExpr:
			pred := p // capture loop variable
			exprFilters = append(exprFilters, func(rec chunk.Record) bool {
				return evalPredicate(pred, rec)
			})
		}
	}

	// Build combined filter for negatives + regexes + expression predicates
	var filters []recordFilter
	if len(conj.Negative) > 0 {
		filters = append(filters, negativePredicatesFilter(conj.Negative))
	}
	filters = append(filters, regexFilters...)
	filters = append(filters, exprFilters...)

	if len(filters) == 1 {
		negFilter = filters[0]
	} else if len(filters) > 1 {
		negFilter = func(rec chunk.Record) bool {
			for _, f := range filters {
				if !f(rec) {
					return false
				}
			}
			return true
		}
	}

	return tokens, kv, globs, negFilter
}

// negativePredicatesFilter returns a filter that rejects records matching ANY of the negative predicates.
func negativePredicatesFilter(predicates []*querylang.PredicateExpr) recordFilter {
	return func(rec chunk.Record) bool {
		for _, p := range predicates {
			if evalPredicate(p, rec) {
				return false // matches a NOT predicate, reject
			}
		}
		return true // doesn't match any NOT predicate, accept
	}
}

// dnfFilter returns a filter that accepts records matching ANY branch of a DNF.
// A record matches a branch if it matches ALL positive predicates AND NONE of the negative predicates.
// This evaluates only primitive predicates, not boolean logic.
func dnfFilter(dnf *querylang.DNF) recordFilter {
	return func(rec chunk.Record) bool {
		for _, branch := range dnf.Branches {
			if matchesBranch(&branch, rec) {
				return true
			}
		}
		return false
	}
}

// matchesBranch checks if a record matches a single DNF branch.
// Returns true if record matches all positive predicates and none of the negative predicates.
func matchesBranch(branch *querylang.Conjunction, rec chunk.Record) bool {
	// Check all positive predicates (AND semantics)
	for _, p := range branch.Positive {
		if !evalPredicate(p, rec) {
			return false
		}
	}
	// Check all negative predicates (must NOT match any)
	for _, p := range branch.Negative {
		if evalPredicate(p, rec) {
			return false
		}
	}
	return true
}

// evalPredicate evaluates a single predicate against a record.
func evalPredicate(pred *querylang.PredicateExpr, rec chunk.Record) bool {
	switch pred.Kind {
	case querylang.PredToken:
		return matchesSingleToken(rec.Raw, pred.Value)

	case querylang.PredKV:
		return matchesSingleKV(rec.Attrs, rec.Raw, pred)

	case querylang.PredKeyExists:
		return matchesKeyExists(rec.Attrs, rec.Raw, pred)

	case querylang.PredValueExists:
		return matchesValueExists(rec.Attrs, rec.Raw, pred)

	case querylang.PredRegex:
		return pred.Pattern.Match(rec.Raw)

	case querylang.PredGlob:
		return matchesSingleGlob(rec.Raw, pred.Pattern)

	case querylang.PredExpr:
		row := RecordToRow(rec)
		val, err := pipeEval.Eval(pred.ExprLHS, row)
		if err != nil || val.Missing {
			return false
		}
		return compareValues(val.Str, pred.Value, pred.Op)

	default:
		return false
	}
}

// matchesSingleToken checks if a record contains a specific token.
// If the token is indexable (pure token-alphabet characters), it uses tokenized
// matching. Otherwise (e.g. IP addresses, dotted names) it falls back to
// case-insensitive substring search on the raw record data.
func matchesSingleToken(raw []byte, token string) bool {
	tokenLower := strings.ToLower(token)
	if tokenizer.IsIndexable(tokenLower) {
		recordTokens := tokenizer.Tokens(raw)
		return slices.Contains(recordTokens, tokenLower)
	}
	return bytes.Contains(bytes.ToLower(raw), []byte(tokenLower))
}

// compareValues compares two string values using the given operator.
// a is the record value, b is the query value.
//
// For ordering ops (>, >=, <, <=):
//   - If the query value is numeric, require the record value to also be numeric.
//     Non-numeric record values are skipped (return false). This prevents
//     status>=500 from matching status=sent via lexicographic comparison.
//   - If the query value is non-numeric, use case-insensitive lexicographic comparison.
func compareValues(a, b string, op querylang.CompareOp) bool {
	switch op {
	case querylang.OpEq:
		return strings.EqualFold(a, b)
	case querylang.OpNe:
		return !strings.EqualFold(a, b)
	}

	// Try numeric comparison.
	af, aErr := strconv.ParseFloat(a, 64)
	bf, bErr := strconv.ParseFloat(b, 64)
	if aErr == nil && bErr == nil {
		switch op {
		case querylang.OpGt:
			return af > bf
		case querylang.OpGte:
			return af >= bf
		case querylang.OpLt:
			return af < bf
		case querylang.OpLte:
			return af <= bf
		}
	}

	// Query value is numeric but record value isn't — not comparable.
	if bErr == nil && aErr != nil {
		return false
	}

	// Both non-numeric (or record numeric, query non-numeric):
	// case-insensitive lexicographic comparison.
	cmp := strings.Compare(strings.ToLower(a), strings.ToLower(b))
	switch op {
	case querylang.OpGt:
		return cmp > 0
	case querylang.OpGte:
		return cmp >= 0
	case querylang.OpLt:
		return cmp < 0
	case querylang.OpLte:
		return cmp <= 0
	}
	return false
}

// matchesSingleKV checks if a record contains a specific key=value pair
// in either attributes or extracted message body pairs.
// Supports glob patterns via keyPat/valPat (from PredicateExpr.KeyPat/ValuePat)
// and comparison operators via pred.Op.
func matchesSingleKV(attrs chunk.Attributes, raw []byte, pred *querylang.PredicateExpr) bool {
	keyLower := strings.ToLower(pred.Key)
	valueLower := strings.ToLower(pred.Value)

	// For comparison operators other than eq, use compareValues.
	if pred.Op != querylang.OpEq {
		// Check attributes.
		for k, v := range attrs {
			if strings.EqualFold(k, pred.Key) && compareValues(v, pred.Value, pred.Op) {
				return true
			}
		}
		// Check message body.
		pairs := tokenizer.CombinedExtract(raw, kvExtractors)
		for _, kv := range pairs {
			if kv.Key == keyLower && compareValues(kv.Value, pred.Value, pred.Op) {
				return true
			}
		}
		// Check structural JSON paths.
		if matchJSONPathCompare(raw, pred.Key, pred.Value, pred.Op) {
			return true
		}
		return false
	}

	// OpEq: original exact-match logic.
	// Check attributes (case-insensitive).
	for k, v := range attrs {
		if matchStringOrPat(k, pred.Key, pred.KeyPat) && matchStringOrPat(v, pred.Value, pred.ValuePat) {
			return true
		}
	}

	// Check message body (flat key=value extraction).
	pairs := tokenizer.CombinedExtract(raw, kvExtractors)
	for _, kv := range pairs {
		if matchStringOrPatLower(kv.Key, keyLower, pred.KeyPat) && matchStringOrPatLower(strings.ToLower(kv.Value), valueLower, pred.ValuePat) {
			return true
		}
	}

	// Check structural JSON paths (e.g. "level" or "kubernetes.namespace").
	if pred.KeyPat == nil && pred.ValuePat == nil {
		if matchJSONPathValue(raw, pred.Key, valueLower) {
			return true
		}
	}

	return false
}

// matchesKeyExists checks if a record has a key (any value) in attrs or message body.
// Supports glob patterns via keyPat (from PredicateExpr.KeyPat).
func matchesKeyExists(attrs chunk.Attributes, raw []byte, pred *querylang.PredicateExpr) bool {
	keyLower := strings.ToLower(pred.Key)

	// Check attributes.
	for k := range attrs {
		if matchStringOrPat(k, pred.Key, pred.KeyPat) {
			return true
		}
	}

	// Check message body.
	pairs := tokenizer.CombinedExtract(raw, kvExtractors)
	for _, kv := range pairs {
		if matchStringOrPatLower(kv.Key, keyLower, pred.KeyPat) {
			return true
		}
	}

	// Check structural JSON paths (e.g. "level" or "kubernetes.namespace").
	if pred.KeyPat == nil {
		if matchJSONPathExists(raw, pred.Key) {
			return true
		}
	}

	return false
}

// matchesValueExists checks if a record has a value (any key) in attrs or message body.
// Supports glob patterns via valPat (from PredicateExpr.ValuePat).
func matchesValueExists(attrs chunk.Attributes, raw []byte, pred *querylang.PredicateExpr) bool {
	valueLower := strings.ToLower(pred.Value)

	// Check attributes.
	for _, v := range attrs {
		if matchStringOrPat(v, pred.Value, pred.ValuePat) {
			return true
		}
	}

	// Check message body.
	pairs := tokenizer.CombinedExtract(raw, kvExtractors)
	for _, kv := range pairs {
		if matchStringOrPatLower(strings.ToLower(kv.Value), valueLower, pred.ValuePat) {
			return true
		}
	}

	return false
}

// sourceTimeFilter returns a filter that checks SourceTS bounds.
// Records with zero SourceTS are excluded if any bound is set.
func sourceTimeFilter(start, end time.Time) recordFilter {
	return func(rec chunk.Record) bool {
		// If SourceTS is zero, we can't filter by it - exclude if bounds are set
		if rec.SourceTS.IsZero() {
			return false
		}
		if !start.IsZero() && rec.SourceTS.Before(start) {
			return false
		}
		if !end.IsZero() && !rec.SourceTS.Before(end) {
			return false
		}
		return true
	}
}

// ingestTimeFilter returns a filter that checks IngestTS bounds.
func ingestTimeFilter(start, end time.Time) recordFilter {
	return func(rec chunk.Record) bool {
		if !start.IsZero() && rec.IngestTS.Before(start) {
			return false
		}
		if !end.IsZero() && !rec.IngestTS.Before(end) {
			return false
		}
		return true
	}
}

// dotToNull converts dots in a key to null bytes for JSON path lookup.
// e.g. "kubernetes.namespace" → "kubernetes\x00namespace"
func dotToNull(key string) string {
	return strings.ReplaceAll(key, ".", "\x00")
}

// matchJSONPathValue walks JSON in the raw record and checks if any path
// matching the dotted key contains the expected value.
// The key uses dots as path separators (e.g. "kubernetes.namespace").
func matchJSONPathValue(raw []byte, key, valueLower string) bool {
	pathTarget := dotToNull(strings.ToLower(key))
	found := false
	tokenizer.WalkJSON(raw, nil, func(path, value []byte) {
		if found {
			return
		}
		if strings.ToLower(string(path)) == pathTarget &&
			strings.ToLower(string(value)) == valueLower {
			found = true
		}
	})
	return found
}

// matchJSONPathCompare walks JSON in the raw record and checks if any path
// matching the dotted key satisfies the comparison operator against the expected value.
func matchJSONPathCompare(raw []byte, key, queryValue string, op querylang.CompareOp) bool {
	pathTarget := dotToNull(strings.ToLower(key))
	found := false
	tokenizer.WalkJSON(raw, nil, func(path, value []byte) {
		if found {
			return
		}
		if strings.ToLower(string(path)) == pathTarget &&
			compareValues(string(value), queryValue, op) {
			found = true
		}
	})
	return found
}

// matchJSONPathExists walks JSON in the raw record and checks if any path
// matches the dotted key.
func matchJSONPathExists(raw []byte, key string) bool {
	pathTarget := dotToNull(strings.ToLower(key))
	found := false
	tokenizer.WalkJSON(raw, func(path []byte) {
		if found {
			return
		}
		if strings.ToLower(string(path)) == pathTarget {
			found = true
		}
	}, nil)
	return found
}
