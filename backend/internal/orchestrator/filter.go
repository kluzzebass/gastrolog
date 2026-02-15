package orchestrator

import (
	"fmt"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"

	"github.com/google/uuid"
)


// FilterKind identifies the type of store filter.
type FilterKind int

const (
	// FilterNone means the store receives nothing (empty filter).
	FilterNone FilterKind = iota
	// FilterCatchAll means the store receives all messages ("*").
	FilterCatchAll
	// FilterCatchRest means the store receives unmatched messages ("+").
	FilterCatchRest
	// FilterExpr means the store uses an expression filter.
	FilterExpr
)

// CompiledFilter is a pre-compiled store filter for fast evaluation.
type CompiledFilter struct {
	StoreID uuid.UUID
	Kind    FilterKind
	Expr    string         // original filter expression (for config reconstruction)
	DNF     *querylang.DNF // only set for FilterExpr
}

// CompileFilter parses a filter string and returns a compiled filter.
// Returns an error if the filter expression is invalid or uses unsupported predicates.
func CompileFilter(storeID uuid.UUID, filter string) (*CompiledFilter, error) {
	filter = strings.TrimSpace(filter)

	// Empty filter = receives nothing
	if filter == "" {
		return &CompiledFilter{StoreID: storeID, Kind: FilterNone, Expr: ""}, nil
	}

	// Catch-all
	if filter == "*" {
		return &CompiledFilter{StoreID: storeID, Kind: FilterCatchAll, Expr: "*"}, nil
	}

	// Catch-the-rest
	if filter == "+" {
		return &CompiledFilter{StoreID: storeID, Kind: FilterCatchRest, Expr: "+"}, nil
	}

	// Parse as querylang expression
	expr, err := querylang.Parse(filter)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}

	// Validate: reject token predicates (only attr-based filtering allowed)
	if err := validateFilterExpr(expr); err != nil {
		return nil, err
	}

	// Compile to DNF for fast evaluation
	dnf := querylang.ToDNF(expr)

	return &CompiledFilter{
		StoreID: storeID,
		Kind:    FilterExpr,
		Expr:    filter,
		DNF:     &dnf,
	}, nil
}

// validateFilterExpr checks that the expression only uses attr-based predicates.
// Token predicates are not allowed because filters only look at Attrs, not Raw.
func validateFilterExpr(expr querylang.Expr) error {
	switch e := expr.(type) {
	case *querylang.PredicateExpr:
		if e.Kind == querylang.PredToken {
			return fmt.Errorf("token predicates not allowed in filters (use key=value): %q", e.Value)
		}
		return nil

	case *querylang.AndExpr:
		for _, term := range e.Terms {
			if err := validateFilterExpr(term); err != nil {
				return err
			}
		}
		return nil

	case *querylang.OrExpr:
		for _, term := range e.Terms {
			if err := validateFilterExpr(term); err != nil {
				return err
			}
		}
		return nil

	case *querylang.NotExpr:
		return validateFilterExpr(e.Term)

	default:
		return nil
	}
}

// FilterSet evaluates store filters to determine which stores receive a message.
type FilterSet struct {
	filters []*CompiledFilter
}

// NewFilterSet creates a filter set from compiled filters.
func NewFilterSet(filters []*CompiledFilter) *FilterSet {
	return &FilterSet{filters: filters}
}

// AddOrUpdate returns a new FilterSet with the given store's filter compiled and
// added or updated. Returns error if the filter expression is invalid.
// Safe to call on a nil receiver (creates a fresh set).
func (fs *FilterSet) AddOrUpdate(storeID uuid.UUID, filterExpr string) (*FilterSet, error) {
	f, err := CompileFilter(storeID, filterExpr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter for store %s: %w", storeID, err)
	}

	var filters []*CompiledFilter
	if fs != nil {
		for _, existing := range fs.filters {
			if existing.StoreID != storeID {
				filters = append(filters, existing)
			}
		}
	}
	filters = append(filters, f)

	return NewFilterSet(filters), nil
}

// Without returns a new FilterSet excluding filters for the given store IDs.
// Returns nil if the resulting set is empty. Safe to call on a nil receiver.
func (fs *FilterSet) Without(storeIDs ...uuid.UUID) *FilterSet {
	if fs == nil {
		return nil
	}

	exclude := make(map[uuid.UUID]struct{}, len(storeIDs))
	for _, id := range storeIDs {
		exclude[id] = struct{}{}
	}

	var filters []*CompiledFilter
	for _, f := range fs.filters {
		if _, skip := exclude[f.StoreID]; !skip {
			filters = append(filters, f)
		}
	}

	if len(filters) == 0 {
		return nil
	}
	return NewFilterSet(filters)
}

// Match returns the store IDs that should receive a message with the given attributes.
//
// TODO(telemetry): Track filter metrics to detect message loss:
//   - messages_matched_total (counter per store)
//   - messages_dropped_total (counter for messages matching no filters)
//   - messages_ingested_total (total messages received)
//
// This enables alerting on drop rate and visibility into filter distribution.
func (fs *FilterSet) Match(attrs chunk.Attributes) []uuid.UUID {
	var result []uuid.UUID
	matchedExpr := false

	// First pass: evaluate expression filters and catch-all
	for _, f := range fs.filters {
		switch f.Kind {
		case FilterNone:
			// Skip - receives nothing
		case FilterCatchAll:
			result = append(result, f.StoreID)
		case FilterExpr:
			if matchesAttrs(f.DNF, attrs) {
				result = append(result, f.StoreID)
				matchedExpr = true
			}
		case FilterCatchRest:
			// Handled in second pass
		}
	}

	// Second pass: catch-the-rest only if no expression filters matched
	if !matchedExpr {
		for _, f := range fs.filters {
			if f.Kind == FilterCatchRest {
				result = append(result, f.StoreID)
			}
		}
	}

	return result
}

// matchesAttrs checks if attributes match a DNF expression.
func matchesAttrs(dnf *querylang.DNF, attrs chunk.Attributes) bool {
	for _, branch := range dnf.Branches {
		if matchesBranchAttrs(&branch, attrs) {
			return true
		}
	}
	return false
}

// matchesBranchAttrs checks if attributes match a single DNF branch.
func matchesBranchAttrs(branch *querylang.Conjunction, attrs chunk.Attributes) bool {
	// Check all positive predicates (AND semantics)
	for _, p := range branch.Positive {
		if !evalAttrPredicate(p, attrs) {
			return false
		}
	}
	// Check all negative predicates (must NOT match any)
	for _, p := range branch.Negative {
		if evalAttrPredicate(p, attrs) {
			return false
		}
	}
	return true
}

// evalAttrPredicate evaluates a predicate against attributes only.
func evalAttrPredicate(pred *querylang.PredicateExpr, attrs chunk.Attributes) bool {
	switch pred.Kind {
	case querylang.PredKV:
		// Exact key=value match (case-insensitive)
		if v, ok := attrs[pred.Key]; ok {
			return strings.EqualFold(v, pred.Value)
		}
		// Also check case-insensitive key lookup
		for k, v := range attrs {
			if strings.EqualFold(k, pred.Key) && strings.EqualFold(v, pred.Value) {
				return true
			}
		}
		return false

	case querylang.PredKeyExists:
		// Key exists with any value
		if _, ok := attrs[pred.Key]; ok {
			return true
		}
		for k := range attrs {
			if strings.EqualFold(k, pred.Key) {
				return true
			}
		}
		return false

	case querylang.PredValueExists:
		// Any key has this value
		for _, v := range attrs {
			if strings.EqualFold(v, pred.Value) {
				return true
			}
		}
		return false

	case querylang.PredToken:
		// Should not happen - validated at compile time
		return false

	default:
		return false
	}
}
