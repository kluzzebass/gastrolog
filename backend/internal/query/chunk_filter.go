package query

import (
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
)

// chunkKey is the reserved key for chunk filtering.
const chunkKey = "chunk"

// ExtractChunkFilter extracts chunk predicates from a BoolExpr and returns:
// - chunks: the set of chunk IDs to query (nil means all chunks)
// - remainingExpr: the expression with chunk predicates removed (nil if nothing remains)
//
// Chunk predicates are KV predicates with key="chunk":
//   - chunk=01h5kq3g8r000         -> query only that chunk
//   - chunk=abc OR chunk=def       -> query both chunks
//   - error chunk=01h5kq3g8r000   -> query that chunk for "error" token
//
// Chunk predicates at the top level (ANDed with other terms) are extracted.
// Invalid chunk IDs are silently ignored (left in the expression for runtime).
func ExtractChunkFilter(expr querylang.Expr) (chunks []chunk.ChunkID, remainingExpr querylang.Expr) {
	if expr == nil {
		return nil, nil
	}

	extracted := make(map[chunk.ChunkID]struct{})
	remaining := extractChunkPredicates(expr, extracted)

	if len(extracted) == 0 {
		return nil, expr
	}

	chunks = make([]chunk.ChunkID, 0, len(extracted))
	for id := range extracted {
		chunks = append(chunks, id)
	}

	return chunks, remaining
}

// extractChunkPredicates recursively extracts chunk=X predicates from ANDed terms.
func extractChunkPredicates(expr querylang.Expr, chunks map[chunk.ChunkID]struct{}) querylang.Expr {
	switch e := expr.(type) {
	case *querylang.PredicateExpr:
		return extractChunkPredicate(e, chunks)
	case *querylang.AndExpr:
		return extractChunkFromAnd(e, chunks)
	case *querylang.OrExpr:
		return extractChunkFromOr(e, chunks)
	default:
		return expr
	}
}

// extractChunkPredicate extracts a single chunk=X predicate.
// Returns nil if the predicate was extracted, or the original expression if not.
func extractChunkPredicate(e *querylang.PredicateExpr, chunks map[chunk.ChunkID]struct{}) querylang.Expr {
	if e.Kind != querylang.PredKV || !strings.EqualFold(e.Key, chunkKey) {
		return e
	}
	id, err := chunk.ParseChunkID(e.Value)
	if err != nil {
		return e // invalid chunk ID, leave for runtime
	}
	chunks[id] = struct{}{}
	return nil
}

// extractChunkFromAnd extracts chunk predicates from an AND expression,
// returning the remaining non-chunk terms or nil if all were extracted.
func extractChunkFromAnd(e *querylang.AndExpr, chunks map[chunk.ChunkID]struct{}) querylang.Expr {
	var remaining []querylang.Expr
	for _, term := range e.Terms {
		if r := extractChunkPredicates(term, chunks); r != nil {
			remaining = append(remaining, r)
		}
	}
	if len(remaining) == 0 {
		return nil
	}
	if len(remaining) == 1 {
		return remaining[0]
	}
	return &querylang.AndExpr{Terms: remaining}
}

// extractChunkFromOr extracts chunk predicates from an OR expression.
// All terms must be valid chunk predicates for extraction; otherwise returns the original expression.
func extractChunkFromOr(e *querylang.OrExpr, chunks map[chunk.ChunkID]struct{}) querylang.Expr {
	if !isAllChunkPredicates(e.Terms) {
		return e
	}
	for _, term := range e.Terms {
		p := term.(*querylang.PredicateExpr)
		id, _ := chunk.ParseChunkID(p.Value)
		chunks[id] = struct{}{}
	}
	return nil
}

// isAllChunkPredicates returns true if every term is a valid chunk=X predicate.
func isAllChunkPredicates(terms []querylang.Expr) bool {
	for _, term := range terms {
		p, ok := term.(*querylang.PredicateExpr)
		if !ok || p.Kind != querylang.PredKV || !strings.EqualFold(p.Key, chunkKey) {
			return false
		}
		if _, err := chunk.ParseChunkID(p.Value); err != nil {
			return false
		}
	}
	return true
}
