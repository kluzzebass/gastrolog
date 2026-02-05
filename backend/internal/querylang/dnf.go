package querylang

import "slices"

// DNF (Disjunctive Normal Form) conversion for boolean expressions.
//
// DNF is an OR of ANDs: (A AND B) OR (C AND D) OR ...
// Each AND clause is called a "conjunction" or "branch".
//
// This enables efficient query execution:
// 1. Each branch can independently use indexes for its positive predicates
// 2. Results from all branches are unioned (deduplicated)
// 3. NOT predicates become runtime filters within their branch

// Conjunction represents a single AND clause in DNF form.
// It contains positive predicates (can use indexes) and negative predicates (runtime filter).
type Conjunction struct {
	Positive []*PredicateExpr // predicates that must match (AND semantics)
	Negative []*PredicateExpr // predicates that must NOT match (AND semantics)
}

// DNF represents a query in Disjunctive Normal Form.
// The query matches if ANY conjunction matches (OR semantics).
type DNF struct {
	Branches []Conjunction
}

// ToDNF converts a boolean expression to Disjunctive Normal Form.
//
// Examples:
//   - "error" -> 1 branch: {Positive: [error]}
//   - "error AND warn" -> 1 branch: {Positive: [error, warn]}
//   - "error OR warn" -> 2 branches: {Positive: [error]}, {Positive: [warn]}
//   - "NOT error" -> 1 branch: {Negative: [error]}
//   - "(error OR warn) AND NOT debug" -> 2 branches:
//   - {Positive: [error], Negative: [debug]}
//   - {Positive: [warn], Negative: [debug]}
//   - "(a AND b) OR (c AND d)" -> 2 branches:
//   - {Positive: [a, b]}
//   - {Positive: [c, d]}
func ToDNF(expr Expr) DNF {
	// Convert to DNF by recursive distribution
	branches := toDNFBranches(expr)
	return DNF{Branches: branches}
}

// toDNFBranches converts an expression to a list of conjunctions.
// Each conjunction represents one OR branch.
func toDNFBranches(expr Expr) []Conjunction {
	switch e := expr.(type) {
	case *PredicateExpr:
		// Single predicate: one branch with one positive predicate
		return []Conjunction{{Positive: []*PredicateExpr{e}}}

	case *NotExpr:
		// NOT expr: push NOT down to predicates
		return toDNFNot(e.Term)

	case *AndExpr:
		// AND: cross-product of all term branches
		return toDNFAnd(e.Terms)

	case *OrExpr:
		// OR: concatenate all term branches
		return toDNFOr(e.Terms)

	default:
		return nil
	}
}

// toDNFNot handles NOT by pushing negation down.
// NOT (A AND B) = (NOT A) OR (NOT B)  [De Morgan]
// NOT (A OR B) = (NOT A) AND (NOT B)  [De Morgan]
// NOT (NOT A) = A                      [Double negation]
// NOT predicate = negative predicate
func toDNFNot(expr Expr) []Conjunction {
	switch e := expr.(type) {
	case *PredicateExpr:
		// NOT predicate: one branch with one negative predicate
		return []Conjunction{{Negative: []*PredicateExpr{e}}}

	case *NotExpr:
		// NOT NOT A = A (double negation elimination)
		return toDNFBranches(e.Term)

	case *AndExpr:
		// NOT (A AND B) = (NOT A) OR (NOT B) [De Morgan]
		var result []Conjunction
		for _, term := range e.Terms {
			result = append(result, toDNFNot(term)...)
		}
		return result

	case *OrExpr:
		// NOT (A OR B) = (NOT A) AND (NOT B) [De Morgan]
		// This becomes the cross-product of negated terms
		negatedTerms := make([][]Conjunction, len(e.Terms))
		for i, term := range e.Terms {
			negatedTerms[i] = toDNFNot(term)
		}
		return crossProduct(negatedTerms)

	default:
		return nil
	}
}

// toDNFAnd handles AND by computing cross-product of branches.
// (A1 OR A2) AND (B1 OR B2) = (A1 AND B1) OR (A1 AND B2) OR (A2 AND B1) OR (A2 AND B2)
func toDNFAnd(terms []Expr) []Conjunction {
	if len(terms) == 0 {
		return []Conjunction{{}}
	}

	// Get DNF branches for each term
	termBranches := make([][]Conjunction, len(terms))
	for i, term := range terms {
		termBranches[i] = toDNFBranches(term)
	}

	return crossProduct(termBranches)
}

// toDNFOr handles OR by concatenating branches.
func toDNFOr(terms []Expr) []Conjunction {
	var result []Conjunction
	for _, term := range terms {
		result = append(result, toDNFBranches(term)...)
	}
	return result
}

// crossProduct computes the cross-product of conjunction lists.
// Each element in the result is the merge of one conjunction from each input list.
func crossProduct(lists [][]Conjunction) []Conjunction {
	if len(lists) == 0 {
		return []Conjunction{{}}
	}

	// Start with the first list
	result := lists[0]

	// Iteratively combine with remaining lists
	for i := 1; i < len(lists); i++ {
		result = combineLists(result, lists[i])
	}

	return result
}

// combineLists combines two lists of conjunctions by merging each pair.
func combineLists(a, b []Conjunction) []Conjunction {
	var result []Conjunction
	for _, ca := range a {
		for _, cb := range b {
			result = append(result, mergeConjunctions(ca, cb))
		}
	}
	return result
}

// mergeConjunctions merges two conjunctions into one.
func mergeConjunctions(a, b Conjunction) Conjunction {
	return Conjunction{
		Positive: slices.Concat(a.Positive, b.Positive),
		Negative: slices.Concat(a.Negative, b.Negative),
	}
}

// IsEmpty returns true if the conjunction has no predicates.
func (c *Conjunction) IsEmpty() bool {
	return len(c.Positive) == 0 && len(c.Negative) == 0
}

// String returns a human-readable representation of the conjunction.
func (c *Conjunction) String() string {
	var parts []string
	for _, p := range c.Positive {
		parts = append(parts, p.String())
	}
	for _, p := range c.Negative {
		parts = append(parts, "NOT "+p.String())
	}
	if len(parts) == 0 {
		return "(empty)"
	}
	if len(parts) == 1 {
		return parts[0]
	}
	return "(" + joinStrings(parts, " AND ") + ")"
}

// String returns a human-readable representation of the DNF.
func (d *DNF) String() string {
	if len(d.Branches) == 0 {
		return "(empty)"
	}
	if len(d.Branches) == 1 {
		return d.Branches[0].String()
	}
	var parts []string
	for _, b := range d.Branches {
		parts = append(parts, b.String())
	}
	return joinStrings(parts, " OR ")
}

func joinStrings(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += sep + parts[i]
	}
	return result
}
