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
		switch e.Kind {
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

// MatchAttrs checks if attributes match a DNF expression.
// A nil DNF matches everything (no filter configured).
func MatchAttrs(dnf *DNF, attrs map[string]string) bool {
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
func matchBranchAttrs(branch *Conjunction, attrs map[string]string) bool {
	for _, p := range branch.Positive {
		if !EvalAttrPredicate(p, attrs) {
			return false
		}
	}
	for _, p := range branch.Negative {
		if EvalAttrPredicate(p, attrs) {
			return false
		}
	}
	return true
}

// EvalAttrPredicate evaluates a predicate against attributes.
// Supports glob patterns in key and value positions via KeyPat/ValuePat.
func EvalAttrPredicate(pred *PredicateExpr, attrs map[string]string) bool {
	switch pred.Kind {
	case PredKV:
		if pred.KeyPat != nil {
			// Glob key, check all matching keys.
			for k, v := range attrs {
				if pred.KeyPat.MatchString(k) {
					if matchValue(pred, v) {
						return true
					}
				}
			}
			return false
		}
		// Exact key lookup (case-insensitive).
		if v, ok := attrs[pred.Key]; ok {
			if matchValue(pred, v) {
				return true
			}
		}
		for k, v := range attrs {
			if strings.EqualFold(k, pred.Key) && matchValue(pred, v) {
				return true
			}
		}
		return false

	case PredKeyExists:
		if pred.KeyPat != nil {
			for k := range attrs {
				if pred.KeyPat.MatchString(k) {
					return true
				}
			}
			return false
		}
		if _, ok := attrs[pred.Key]; ok {
			return true
		}
		for k := range attrs {
			if strings.EqualFold(k, pred.Key) {
				return true
			}
		}
		return false

	case PredValueExists:
		if pred.ValuePat != nil {
			for _, v := range attrs {
				if pred.ValuePat.MatchString(v) {
					return true
				}
			}
			return false
		}
		for _, v := range attrs {
			if strings.EqualFold(v, pred.Value) {
				return true
			}
		}
		return false

	case PredToken, PredRegex, PredGlob:
		// Not applicable to attr matching — should be caught by validation.
		return false

	default:
		return false
	}
}

// matchValue checks if a value matches a predicate's value, using glob pattern
// if available, otherwise case-insensitive exact match.
func matchValue(pred *PredicateExpr, v string) bool {
	if pred.ValuePat != nil {
		return pred.ValuePat.MatchString(v)
	}
	return strings.EqualFold(v, pred.Value)
}
