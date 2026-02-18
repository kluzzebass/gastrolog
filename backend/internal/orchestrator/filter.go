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

	// Validate: reject non-attr predicates (tokens, regexes, globs).
	if err := querylang.ValidateAttrFilter(expr); err != nil {
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
			if querylang.MatchAttrs(f.DNF, attrs) {
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

