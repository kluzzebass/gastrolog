package query

import (
	"errors"
	"iter"
	"sort"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/querylang"
	"gastrolog/internal/tokenizer"
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
	recordTokens := tokenizer.Tokens(raw)
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

// keyValueFilter returns a filter that matches records where all key=value pairs
// are found in either the record's attributes OR the message body.
func keyValueFilter(filters []KeyValueFilter) recordFilter {
	return func(rec chunk.Record) bool {
		return matchesKeyValue(rec.Attrs, rec.Raw, filters)
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
			pairs := tokenizer.ExtractKeyValues(raw)
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
				if strings.EqualFold(k, f.Key) {
					found = true
					break
				}
			}
			if found {
				continue
			}
			// Check message body
			pairs := getMsgPairs()
			if _, ok := pairs[keyLower]; ok {
				continue
			}
			return false // Key not found in either location
		} else if f.Key == "" {
			// Value only: *=value pattern (any key has this value)
			// Check attrs
			found := false
			for _, v := range recAttrs {
				if strings.EqualFold(v, f.Value) {
					found = true
					break
				}
			}
			if found {
				continue
			}
			// Check message body
			values := getMsgValues()
			if _, ok := values[valLower]; ok {
				continue
			}
			return false // Value not found in either location
		} else {
			// Both key and value: exact key=value match
			// Check attributes first (cheaper).
			if v, ok := recAttrs[f.Key]; ok && strings.EqualFold(v, f.Value) {
				continue // Found in attrs, this filter passes.
			}

			// Check message body.
			pairs := getMsgPairs()
			if values, ok := pairs[keyLower]; ok {
				if _, found := values[valLower]; found {
					continue // Found in message, this filter passes.
				}
			}

			return false // Not found in either location.
		}
	}
	return true
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
	// If any token is not in the index, fall back to runtime filtering
	// since the index is selective and doesn't contain all tokens.
	for i, tok := range tokens {
		positions, found := reader.Lookup(tok)
		if !found {
			// Token not in index - can't use index, need runtime filter
			return false, false
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
				if positions, found := reader.Lookup(f.Key, f.Value); found {
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

// ConjunctionToFilters converts a DNF conjunction to tokens, KV filters, and a negation filter.
// Positive predicates are returned as tokens/KV for index acceleration.
// Negative predicates are returned as a runtime filter.
func ConjunctionToFilters(conj *querylang.Conjunction) (tokens []string, kv []KeyValueFilter, negFilter recordFilter) {
	// Extract positive predicates for index acceleration
	for _, p := range conj.Positive {
		switch p.Kind {
		case querylang.PredToken:
			tokens = append(tokens, p.Value)
		case querylang.PredKV:
			kv = append(kv, KeyValueFilter{Key: p.Key, Value: p.Value})
		case querylang.PredKeyExists:
			kv = append(kv, KeyValueFilter{Key: p.Key, Value: ""})
		case querylang.PredValueExists:
			kv = append(kv, KeyValueFilter{Key: "", Value: p.Value})
		}
	}

	// Build negation filter for negative predicates
	if len(conj.Negative) > 0 {
		negFilter = negativePredicatesFilter(conj.Negative)
	}

	return tokens, kv, negFilter
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
		return matchesSingleKV(rec.Attrs, rec.Raw, pred.Key, pred.Value)

	case querylang.PredKeyExists:
		return matchesKeyExists(rec.Attrs, rec.Raw, pred.Key)

	case querylang.PredValueExists:
		return matchesValueExists(rec.Attrs, rec.Raw, pred.Value)

	default:
		return false
	}
}

// matchesSingleToken checks if a record contains a specific token.
func matchesSingleToken(raw []byte, token string) bool {
	// Lowercase the token for case-insensitive matching.
	tokenLower := strings.ToLower(token)
	recordTokens := tokenizer.Tokens(raw)
	for _, t := range recordTokens {
		if t == tokenLower {
			return true
		}
	}
	return false
}

// matchesSingleKV checks if a record contains a specific key=value pair
// in either attributes or extracted message body pairs.
func matchesSingleKV(attrs chunk.Attributes, raw []byte, key, value string) bool {
	keyLower := strings.ToLower(key)
	valueLower := strings.ToLower(value)

	// Check attributes (case-insensitive).
	for k, v := range attrs {
		if strings.EqualFold(k, key) && strings.EqualFold(v, value) {
			return true
		}
	}

	// Check message body.
	pairs := tokenizer.ExtractKeyValues(raw)
	for _, kv := range pairs {
		if kv.Key == keyLower && strings.ToLower(kv.Value) == valueLower {
			return true
		}
	}

	return false
}

// matchesKeyExists checks if a record has a key (any value) in attrs or message body.
func matchesKeyExists(attrs chunk.Attributes, raw []byte, key string) bool {
	keyLower := strings.ToLower(key)

	// Check attributes.
	for k := range attrs {
		if strings.EqualFold(k, key) {
			return true
		}
	}

	// Check message body.
	pairs := tokenizer.ExtractKeyValues(raw)
	for _, kv := range pairs {
		if kv.Key == keyLower {
			return true
		}
	}

	return false
}

// matchesValueExists checks if a record has a value (any key) in attrs or message body.
func matchesValueExists(attrs chunk.Attributes, raw []byte, value string) bool {
	valueLower := strings.ToLower(value)

	// Check attributes.
	for _, v := range attrs {
		if strings.EqualFold(v, value) {
			return true
		}
	}

	// Check message body.
	pairs := tokenizer.ExtractKeyValues(raw)
	for _, kv := range pairs {
		if strings.ToLower(kv.Value) == valueLower {
			return true
		}
	}

	return false
}

// notTokenFilter returns a filter that excludes records containing any of the given tokens.
func notTokenFilter(tokens []string) recordFilter {
	return func(rec chunk.Record) bool {
		return !matchesTokens(rec.Raw, tokens)
	}
}

// notKeyValueFilter returns a filter that excludes records matching any of the given key=value pairs.
func notKeyValueFilter(filters []KeyValueFilter) recordFilter {
	return func(rec chunk.Record) bool {
		return !matchesKeyValue(rec.Attrs, rec.Raw, filters)
	}
}
