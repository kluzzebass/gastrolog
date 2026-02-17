// Package querylang provides a boolean query language parser for GastroLog.
// It parses user query strings into a semantic AST that the query engine consumes.
//
// This package is a frontend parsing layer only. It MUST NOT:
//   - Access indexes
//   - Plan execution
//   - Execute queries
//   - Handle pagination or resume tokens
//   - Know about chunks, storage, or indexes
package querylang

import (
	"fmt"
	"regexp"
	"strings"
)

// Expr is the interface for all AST nodes.
// The marker method prevents external types from implementing Expr.
type Expr interface {
	expr()
	// String returns a human-readable representation of the expression.
	String() string
}

// AndExpr represents logical AND of multiple expressions.
// Invariant: len(Terms) >= 2
type AndExpr struct {
	Terms []Expr
}

func (AndExpr) expr() {}

func (a *AndExpr) String() string {
	parts := make([]string, len(a.Terms))
	for i, t := range a.Terms {
		parts[i] = t.String()
	}
	return "(" + strings.Join(parts, " AND ") + ")"
}

// OrExpr represents logical OR of multiple expressions.
// Invariant: len(Terms) >= 2
type OrExpr struct {
	Terms []Expr
}

func (OrExpr) expr() {}

func (o *OrExpr) String() string {
	parts := make([]string, len(o.Terms))
	for i, t := range o.Terms {
		parts[i] = t.String()
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}

// NotExpr represents logical negation.
type NotExpr struct {
	Term Expr
}

func (NotExpr) expr() {}

func (n *NotExpr) String() string {
	return "NOT " + n.Term.String()
}

// PredicateExpr represents a leaf predicate.
type PredicateExpr struct {
	Kind    PredicateKind
	Key     string         // empty for Token kind
	Value   string         // the token or value; for PredRegex, the raw pattern
	Pattern *regexp.Regexp // compiled regex; set only for PredRegex
}

func (PredicateExpr) expr() {}

func (p *PredicateExpr) String() string {
	switch p.Kind {
	case PredToken:
		return fmt.Sprintf("token(%s)", p.Value)
	case PredKV:
		return fmt.Sprintf("%s=%s", p.Key, p.Value)
	case PredKeyExists:
		return fmt.Sprintf("%s=*", p.Key)
	case PredValueExists:
		return fmt.Sprintf("*=%s", p.Value)
	case PredRegex:
		return fmt.Sprintf("regex(/%s/)", p.Value)
	default:
		return fmt.Sprintf("unknown(%d)", p.Kind)
	}
}

// flattenAnd combines two expressions into an AndExpr, flattening nested AndExprs.
func flattenAnd(left, right Expr) Expr {
	var terms []Expr

	if a, ok := left.(*AndExpr); ok {
		terms = append(terms, a.Terms...)
	} else {
		terms = append(terms, left)
	}

	if a, ok := right.(*AndExpr); ok {
		terms = append(terms, a.Terms...)
	} else {
		terms = append(terms, right)
	}

	return &AndExpr{Terms: terms}
}

// FlattenAnd combines multiple expressions into an AndExpr, flattening nested AndExprs.
// This is the exported version for use by other packages.
func FlattenAnd(exprs ...Expr) Expr {
	if len(exprs) == 0 {
		return nil
	}
	if len(exprs) == 1 {
		return exprs[0]
	}

	var terms []Expr
	for _, e := range exprs {
		if a, ok := e.(*AndExpr); ok {
			terms = append(terms, a.Terms...)
		} else {
			terms = append(terms, e)
		}
	}
	return &AndExpr{Terms: terms}
}

// flattenOr combines two expressions into an OrExpr, flattening nested OrExprs.
func flattenOr(left, right Expr) Expr {
	var terms []Expr

	if o, ok := left.(*OrExpr); ok {
		terms = append(terms, o.Terms...)
	} else {
		terms = append(terms, left)
	}

	if o, ok := right.(*OrExpr); ok {
		terms = append(terms, o.Terms...)
	} else {
		terms = append(terms, right)
	}

	return &OrExpr{Terms: terms}
}
