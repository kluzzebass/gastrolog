package querylang

import (
	"fmt"
	"strings"
)

// CompileAttrFilter parses, validates, and converts a filter expression to DNF
// for attribute matching. Returns nil DNF for empty input (match-all).
// Rejects predicates that don't apply to attributes (tokens, regexes, globs).
func CompileAttrFilter(expr string) (*DNF, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, nil
	}

	parsed, err := Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}

	if err := ValidateAttrFilter(parsed); err != nil {
		return nil, err
	}

	dnf := ToDNF(parsed)
	return &dnf, nil
}

// ValidateAttrFilter checks that an expression only uses attribute-based predicates.
// Token, regex, and glob predicates are rejected because attribute filters only
// look at key-value metadata, not raw log content.
func ValidateAttrFilter(expr Expr) error {
	switch e := expr.(type) {
	case *PredicateExpr:
		switch e.Kind { //nolint:exhaustive // only rejecting content-based predicates; KV/key-exists/value-exists/expr are valid
		case PredToken:
			return fmt.Errorf("token predicates not allowed in filters (use key=value): %q", e.Value)
		case PredRegex:
			return fmt.Errorf("regex predicates not allowed in filters (use key=value): /%s/", e.Value)
		case PredGlob:
			return fmt.Errorf("glob predicates not allowed in filters (use key=value): %q", e.Value)
		}
		return nil

	case *AndExpr:
		for _, term := range e.Terms {
			if err := ValidateAttrFilter(term); err != nil {
				return err
			}
		}
		return nil

	case *OrExpr:
		for _, term := range e.Terms {
			if err := ValidateAttrFilter(term); err != nil {
				return err
			}
		}
		return nil

	case *NotExpr:
		return ValidateAttrFilter(e.Term)

	default:
		return nil
	}
}

// AttrSource is a read view over record attributes for filter evaluation.
// It exists so hot paths can evaluate filters against layered or wire-form
// attribute views without materializing a map[string]string per record —
// per-record enrichment copies alone measured ~10GB of garbage per run.
// Get is an exact-key lookup; All iterates every key/value pair (used by
// case-insensitive fallbacks and glob scans) and must not yield duplicate
// keys.
type AttrSource interface {
	Get(key string) (string, bool)
	All(yield func(key, value string) bool)
}

// mapSource adapts a plain attributes map to AttrSource.
type mapSource map[string]string

func (m mapSource) Get(key string) (string, bool) { v, ok := m[key]; return v, ok }

func (m mapSource) All(yield func(key, value string) bool) {
	for k, v := range m {
		if !yield(k, v) {
			return
		}
	}
}

// MatchAttrs checks if attributes match a DNF expression.
// A nil DNF matches everything (no filter configured).
func MatchAttrs(dnf *DNF, attrs map[string]string) bool {
	return MatchAttrSource(dnf, mapSource(attrs))
}

// MatchAttrSource is MatchAttrs over any AttrSource. Generic so concrete
// sources evaluate without interface boxing on the per-record path.
func MatchAttrSource[S AttrSource](dnf *DNF, attrs S) bool {
	if dnf == nil {
		return true
	}
	for _, branch := range dnf.Branches {
		if matchBranchAttrs(&branch, attrs) {
			return true
		}
	}
	return false
}

// matchBranchAttrs checks if attributes match a single DNF branch.
func matchBranchAttrs[S AttrSource](branch *Conjunction, attrs S) bool {
	for _, p := range branch.Positive {
		if !evalAttrPredicate(p, attrs) {
			return false
		}
	}
	for _, p := range branch.Negative {
		if evalAttrPredicate(p, attrs) {
			return false
		}
	}
	return true
}

// EvalAttrPredicate evaluates a predicate against attributes.
// Supports glob patterns in key and value positions via KeyPat/ValuePat.
func EvalAttrPredicate(pred *PredicateExpr, attrs map[string]string) bool {
	return evalAttrPredicate(pred, mapSource(attrs))
}

func evalAttrPredicate[S AttrSource](pred *PredicateExpr, attrs S) bool {
	switch pred.Kind {
	case PredKV:
		return evalKV(pred, attrs)
	case PredKeyExists:
		return evalKeyExists(pred, attrs)
	case PredValueExists:
		return evalValueExists(pred, attrs)
	case PredToken, PredRegex, PredGlob, PredExpr:
		// Not applicable to attr matching — should be caught by validation.
		return false
	default:
		return false
	}
}

// evalKV evaluates a key=value predicate against attributes.
func evalKV[S AttrSource](pred *PredicateExpr, attrs S) bool {
	if pred.KeyPat != nil {
		return evalKVGlobKey(pred, attrs)
	}
	// Exact key lookup (case-insensitive).
	if v, ok := attrs.Get(pred.Key); ok && matchValue(pred, v) {
		return true
	}
	found := false
	attrs.All(func(k, v string) bool {
		if strings.EqualFold(k, pred.Key) && matchValue(pred, v) {
			found = true
			return false
		}
		return true
	})
	return found
}

// evalKVGlobKey evaluates a KV predicate with a glob key pattern.
func evalKVGlobKey[S AttrSource](pred *PredicateExpr, attrs S) bool {
	found := false
	attrs.All(func(k, v string) bool {
		if pred.KeyPat.MatchString(k) && matchValue(pred, v) {
			found = true
			return false
		}
		return true
	})
	return found
}

// evalKeyExists evaluates a key-exists predicate against attributes.
func evalKeyExists[S AttrSource](pred *PredicateExpr, attrs S) bool {
	if pred.KeyPat != nil {
		found := false
		attrs.All(func(k, _ string) bool {
			if pred.KeyPat.MatchString(k) {
				found = true
				return false
			}
			return true
		})
		return found
	}
	if _, ok := attrs.Get(pred.Key); ok {
		return true
	}
	found := false
	attrs.All(func(k, _ string) bool {
		if strings.EqualFold(k, pred.Key) {
			found = true
			return false
		}
		return true
	})
	return found
}

// evalValueExists evaluates a value-exists predicate against attributes.
func evalValueExists[S AttrSource](pred *PredicateExpr, attrs S) bool {
	found := false
	attrs.All(func(_, v string) bool {
		if pred.ValuePat != nil {
			if pred.ValuePat.MatchString(v) {
				found = true
				return false
			}
			return true
		}
		if strings.EqualFold(v, pred.Value) {
			found = true
			return false
		}
		return true
	})
	return found
}

// matchValue checks if a value matches a predicate's value, using glob pattern
// if available, otherwise case-insensitive exact match.
func matchValue(pred *PredicateExpr, v string) bool {
	if pred.ValuePat != nil {
		return pred.ValuePat.MatchString(v)
	}
	return strings.EqualFold(v, pred.Value)
}
