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
		if e.Kind == querylang.PredKV && strings.EqualFold(e.Key, chunkKey) {
			id, err := chunk.ParseChunkID(e.Value)
			if err != nil {
				return expr // invalid chunk ID, leave for runtime
			}
			chunks[id] = struct{}{}
			return nil
		}
		return expr

	case *querylang.AndExpr:
		var remaining []querylang.Expr
		for _, term := range e.Terms {
			r := extractChunkPredicates(term, chunks)
			if r != nil {
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

	case *querylang.OrExpr:
		allChunkPredicates := true
		for _, term := range e.Terms {
			p, ok := term.(*querylang.PredicateExpr)
			if !ok || p.Kind != querylang.PredKV || !strings.EqualFold(p.Key, chunkKey) {
				allChunkPredicates = false
				break
			}
			if _, err := chunk.ParseChunkID(p.Value); err != nil {
				allChunkPredicates = false
				break
			}
		}

		if allChunkPredicates {
			for _, term := range e.Terms {
				p := term.(*querylang.PredicateExpr)
				id, _ := chunk.ParseChunkID(p.Value)
				chunks[id] = struct{}{}
			}
			return nil
		}

		return expr

	case *querylang.NotExpr:
		return expr

	default:
		return expr
	}
}
