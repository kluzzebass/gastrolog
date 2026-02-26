package query

import (
	"strings"

	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)

// vaultKey is the reserved key for vault filtering.
const vaultKey = "vault"

// ExtractVaultFilter extracts vault predicates from a BoolExpr and returns:
// - vaults: the set of vault IDs to query (nil means all vaults)
// - remainingExpr: the expression with vault predicates removed (nil if nothing remains)
//
// Vault predicates are KV predicates with key="vault":
//   - vault=prod         -> query only "prod" vault
//   - vault=prod OR vault=staging -> query "prod" and "staging" vaults
//   - error vault=prod   -> query "prod" for "error" token
//
// Vault predicates at the top level (ANDed with other terms) are extracted.
// Vault predicates inside OR branches or negated are left in place and
// handled at runtime (though this is an unusual use case).
func ExtractVaultFilter(expr querylang.Expr, allVaults []uuid.UUID) (vaults []uuid.UUID, remainingExpr querylang.Expr) {
	if expr == nil {
		return nil, nil // nil means all vaults
	}

	extracted := make(map[string]struct{})
	remaining := extractVaultPredicates(expr, extracted)

	if len(extracted) == 0 {
		return nil, expr // no vault filter, return original expression
	}

	// Convert extracted string values to UUIDs.
	vaults = make([]uuid.UUID, 0, len(extracted))
	for s := range extracted {
		id, err := uuid.Parse(s)
		if err != nil {
			continue // skip unparseable values
		}
		vaults = append(vaults, id)
	}

	if len(vaults) == 0 {
		return nil, expr // no valid UUIDs found, return original expression
	}

	return vaults, remaining
}

// extractVaultPredicates recursively extracts vault=X predicates from ANDed terms.
// Returns the remaining expression with vault predicates removed.
func extractVaultPredicates(expr querylang.Expr, vaults map[string]struct{}) querylang.Expr {
	switch e := expr.(type) {
	case *querylang.PredicateExpr:
		if e.Kind == querylang.PredKV && strings.EqualFold(e.Key, vaultKey) {
			vaults[e.Value] = struct{}{}
			return nil // remove this predicate
		}
		return expr // keep non-vault predicates

	case *querylang.AndExpr:
		// Extract from all terms, keep non-vault terms
		var remaining []querylang.Expr
		for _, term := range e.Terms {
			r := extractVaultPredicates(term, vaults)
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
		// Check if ALL branches are vault predicates (vault=A OR vault=B)
		allVaultPredicates := true
		for _, term := range e.Terms {
			if p, ok := term.(*querylang.PredicateExpr); ok {
				if p.Kind == querylang.PredKV && strings.EqualFold(p.Key, vaultKey) {
					continue
				}
			}
			allVaultPredicates = false
			break
		}

		if allVaultPredicates {
			// Extract all vault values, remove entire OR
			for _, term := range e.Terms {
				p := term.(*querylang.PredicateExpr)
				vaults[p.Value] = struct{}{}
			}
			return nil
		}

		// Mixed OR - don't extract, keep as-is for runtime filtering
		// This handles unusual cases like: (vault=prod AND error) OR (vault=staging AND warn)
		return expr

	case *querylang.NotExpr:
		// Don't extract negated vault predicates - they're weird but valid
		// e.g., NOT vault=prod means "all vaults except prod"
		// For now, leave these for runtime filtering
		return expr

	default:
		return expr
	}
}

// IsVaultOnlyQuery returns true if the expression contains only vault predicates.
// This is used to detect queries like "vault=prod" with no other filters.
func IsVaultOnlyQuery(expr querylang.Expr) bool {
	if expr == nil {
		return false
	}

	switch e := expr.(type) {
	case *querylang.PredicateExpr:
		return e.Kind == querylang.PredKV && strings.EqualFold(e.Key, vaultKey)

	case *querylang.AndExpr:
		for _, term := range e.Terms {
			if !IsVaultOnlyQuery(term) {
				return false
			}
		}
		return true

	case *querylang.OrExpr:
		for _, term := range e.Terms {
			if !IsVaultOnlyQuery(term) {
				return false
			}
		}
		return true

	default:
		return false
	}
}
