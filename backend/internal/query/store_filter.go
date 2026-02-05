package query

import (
	"strings"

	"gastrolog/internal/querylang"
)

// storeKey is the reserved key for store filtering.
const storeKey = "store"

// ExtractStoreFilter extracts store predicates from a BoolExpr and returns:
// - stores: the set of store IDs to query (nil means all stores)
// - remainingExpr: the expression with store predicates removed (nil if nothing remains)
//
// Store predicates are KV predicates with key="store":
//   - store=prod         -> query only "prod" store
//   - store=prod OR store=staging -> query "prod" and "staging" stores
//   - error store=prod   -> query "prod" for "error" token
//
// Store predicates at the top level (ANDed with other terms) are extracted.
// Store predicates inside OR branches or negated are left in place and
// handled at runtime (though this is an unusual use case).
func ExtractStoreFilter(expr querylang.Expr, allStores []string) (stores []string, remainingExpr querylang.Expr) {
	if expr == nil {
		return nil, nil // nil means all stores
	}

	extracted := make(map[string]struct{})
	remaining := extractStorePredicates(expr, extracted)

	if len(extracted) == 0 {
		return nil, expr // no store filter, return original expression
	}

	// Convert map to slice
	stores = make([]string, 0, len(extracted))
	for s := range extracted {
		stores = append(stores, s)
	}

	return stores, remaining
}

// extractStorePredicates recursively extracts store=X predicates from ANDed terms.
// Returns the remaining expression with store predicates removed.
func extractStorePredicates(expr querylang.Expr, stores map[string]struct{}) querylang.Expr {
	switch e := expr.(type) {
	case *querylang.PredicateExpr:
		if e.Kind == querylang.PredKV && strings.EqualFold(e.Key, storeKey) {
			stores[e.Value] = struct{}{}
			return nil // remove this predicate
		}
		return expr // keep non-store predicates

	case *querylang.AndExpr:
		// Extract from all terms, keep non-store terms
		var remaining []querylang.Expr
		for _, term := range e.Terms {
			r := extractStorePredicates(term, stores)
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
		// Check if ALL branches are store predicates (store=A OR store=B)
		allStorePredicates := true
		for _, term := range e.Terms {
			if p, ok := term.(*querylang.PredicateExpr); ok {
				if p.Kind == querylang.PredKV && strings.EqualFold(p.Key, storeKey) {
					continue
				}
			}
			allStorePredicates = false
			break
		}

		if allStorePredicates {
			// Extract all store values, remove entire OR
			for _, term := range e.Terms {
				p := term.(*querylang.PredicateExpr)
				stores[p.Value] = struct{}{}
			}
			return nil
		}

		// Mixed OR - don't extract, keep as-is for runtime filtering
		// This handles unusual cases like: (store=prod AND error) OR (store=staging AND warn)
		return expr

	case *querylang.NotExpr:
		// Don't extract negated store predicates - they're weird but valid
		// e.g., NOT store=prod means "all stores except prod"
		// For now, leave these for runtime filtering
		return expr

	default:
		return expr
	}
}

// IsStoreOnlyQuery returns true if the expression contains only store predicates.
// This is used to detect queries like "store=prod" with no other filters.
func IsStoreOnlyQuery(expr querylang.Expr) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *querylang.PredicateExpr:
		return e.Kind == querylang.PredKV && strings.EqualFold(e.Key, storeKey)

	case *querylang.AndExpr:
		for _, term := range e.Terms {
			if !IsStoreOnlyQuery(term) {
				return false
			}
		}
		return true

	case *querylang.OrExpr:
		for _, term := range e.Terms {
			if !IsStoreOnlyQuery(term) {
				return false
			}
		}
		return true

	default:
		return false
	}
}
