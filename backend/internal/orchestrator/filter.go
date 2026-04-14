package orchestrator

import (
	"gastrolog/internal/glid"
	"fmt"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"

)


// FilterKind identifies the type of vault filter.
type FilterKind int

const (
	// FilterNone means the vault receives nothing (empty filter).
	FilterNone FilterKind = iota
	// FilterCatchAll means the vault receives all messages ("*").
	FilterCatchAll
	// FilterCatchRest means the vault receives unmatched messages ("+").
	FilterCatchRest
	// FilterExpr means the vault uses an expression filter.
	FilterExpr
)

// CompiledFilter is a pre-compiled vault filter for fast evaluation.
type CompiledFilter struct {
	VaultID glid.GLID
	Kind    FilterKind
	Expr    string         // original filter expression (for config reconstruction)
	DNF     *querylang.DNF // only set for FilterExpr
	NodeID  string         // owning node (empty = local vault)
	RouteID glid.GLID      // which route produced this filter (zero = legacy/direct)
}

// MatchResult pairs a vault ID with the node that owns it.
type MatchResult struct {
	VaultID glid.GLID
	NodeID  string    // empty = local vault
	RouteID glid.GLID // which route caused this match (zero = legacy/direct)
}

// CompileFilter parses a filter string and returns a compiled filter.
// Returns an error if the filter expression is invalid or uses unsupported predicates.
func CompileFilter(vaultID glid.GLID, filter string) (*CompiledFilter, error) {
	filter = strings.TrimSpace(filter)

	// Empty filter = receives nothing
	if filter == "" {
		return &CompiledFilter{VaultID: vaultID, Kind: FilterNone, Expr: ""}, nil
	}

	// Catch-all
	if filter == "*" {
		return &CompiledFilter{VaultID: vaultID, Kind: FilterCatchAll, Expr: "*"}, nil
	}

	// Catch-the-rest
	if filter == "+" {
		return &CompiledFilter{VaultID: vaultID, Kind: FilterCatchRest, Expr: "+"}, nil
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
		VaultID: vaultID,
		Kind:    FilterExpr,
		Expr:    filter,
		DNF:     &dnf,
	}, nil
}

// FilterSet evaluates vault filters to determine which vaults receive a message.
type FilterSet struct {
	filters []*CompiledFilter
}

// NewFilterSet creates a filter set from compiled filters.
func NewFilterSet(filters []*CompiledFilter) *FilterSet {
	return &FilterSet{filters: filters}
}

// AddOrUpdate returns a new FilterSet with the given vault's filter compiled and
// added or updated. Returns error if the filter expression is invalid.
// Safe to call on a nil receiver (creates a fresh set).
func (fs *FilterSet) AddOrUpdate(vaultID glid.GLID, filterExpr string) (*FilterSet, error) {
	f, err := CompileFilter(vaultID, filterExpr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter for vault %s: %w", vaultID, err)
	}

	var filters []*CompiledFilter
	if fs != nil {
		for _, existing := range fs.filters {
			if existing.VaultID != vaultID {
				filters = append(filters, existing)
			}
		}
	}
	filters = append(filters, f)

	return NewFilterSet(filters), nil
}

// AddOrUpdateWithNode is like AddOrUpdate but also sets the NodeID on the
// compiled filter. Use this for remote vault destinations so MatchWithNode
// can distinguish local from remote targets.
func (fs *FilterSet) AddOrUpdateWithNode(vaultID glid.GLID, filterExpr, nodeID string) (*FilterSet, error) {
	f, err := CompileFilter(vaultID, filterExpr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter for vault %s: %w", vaultID, err)
	}
	f.NodeID = nodeID

	var filters []*CompiledFilter
	if fs != nil {
		for _, existing := range fs.filters {
			if existing.VaultID != vaultID {
				filters = append(filters, existing)
			}
		}
	}
	filters = append(filters, f)

	return NewFilterSet(filters), nil
}

// AddOrUpdateWithNodeAndRoute is like AddOrUpdateWithNode but also sets the
// RouteID on the compiled filter. Use this when building filters from route
// configuration so per-route stats can be tracked.
func (fs *FilterSet) AddOrUpdateWithNodeAndRoute(vaultID glid.GLID, filterExpr, nodeID string, routeID glid.GLID) (*FilterSet, error) {
	f, err := CompileFilter(vaultID, filterExpr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter for vault %s: %w", vaultID, err)
	}
	f.NodeID = nodeID
	f.RouteID = routeID

	var filters []*CompiledFilter
	if fs != nil {
		for _, existing := range fs.filters {
			if existing.VaultID != vaultID {
				filters = append(filters, existing)
			}
		}
	}
	filters = append(filters, f)

	return NewFilterSet(filters), nil
}

// Without returns a new FilterSet excluding filters for the given vault IDs.
// Returns nil if the resulting set is empty. Safe to call on a nil receiver.
func (fs *FilterSet) Without(vaultIDs ...glid.GLID) *FilterSet {
	if fs == nil {
		return nil
	}

	exclude := make(map[glid.GLID]struct{}, len(vaultIDs))
	for _, id := range vaultIDs {
		exclude[id] = struct{}{}
	}

	var filters []*CompiledFilter
	for _, f := range fs.filters {
		if _, skip := exclude[f.VaultID]; !skip {
			filters = append(filters, f)
		}
	}

	if len(filters) == 0 {
		return nil
	}
	return NewFilterSet(filters)
}

// Match returns the vault IDs that should receive a message with the given attributes.
//
// TODO(telemetry): Track filter metrics to detect message loss:
//   - messages_matched_total (counter per vault)
//   - messages_dropped_total (counter for messages matching no filters)
//   - messages_ingested_total (total messages received)
//
// This enables alerting on drop rate and visibility into filter distribution.
func (fs *FilterSet) Match(attrs chunk.Attributes) []glid.GLID {
	var result []glid.GLID
	matchedExpr := false

	// First pass: evaluate expression filters and catch-all
	for _, f := range fs.filters {
		switch f.Kind {
		case FilterNone:
			// Skip - receives nothing
		case FilterCatchAll:
			result = append(result, f.VaultID)
		case FilterExpr:
			if querylang.MatchAttrs(f.DNF, attrs) {
				result = append(result, f.VaultID)
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
				result = append(result, f.VaultID)
			}
		}
	}

	return result
}

// MatchWithNode returns MatchResults (vault ID + owning node) for all
// filters that match the given attributes. Same logic as Match() but
// preserves the NodeID so callers can partition local vs. remote delivery.
func (fs *FilterSet) MatchWithNode(attrs chunk.Attributes) []MatchResult {
	var result []MatchResult
	matchedExpr := false

	for _, f := range fs.filters {
		switch f.Kind {
		case FilterNone:
			// Skip
		case FilterCatchAll:
			result = append(result, MatchResult{VaultID: f.VaultID, NodeID: f.NodeID, RouteID: f.RouteID})
		case FilterExpr:
			if querylang.MatchAttrs(f.DNF, attrs) {
				result = append(result, MatchResult{VaultID: f.VaultID, NodeID: f.NodeID, RouteID: f.RouteID})
				matchedExpr = true
			}
		case FilterCatchRest:
			// Handled in second pass
		}
	}

	if !matchedExpr {
		for _, f := range fs.filters {
			if f.Kind == FilterCatchRest {
				result = append(result, MatchResult{VaultID: f.VaultID, NodeID: f.NodeID, RouteID: f.RouteID})
			}
		}
	}

	return result
}

