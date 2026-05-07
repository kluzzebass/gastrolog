package orchestrator

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/querylang"
)

// MatchKind identifies how a route's match expression evaluates.
//
// gastrolog-4kkoo (Phase 5): the Phase-4 FilterCatchRest (`+`) kind is
// gone. With first-match-wins by priority, an explicit catch-all route
// at the lowest priority replaces it.
type MatchKind int

const (
	// MatchNone means the route's expression is empty — the route
	// never fires. Useful as a temporary "muted" state for an enabled
	// route without rewriting the expression.
	MatchNone MatchKind = iota
	// MatchAll means the route's expression is "*" — every record
	// matches.
	MatchAll
	// MatchExpr means the route uses an attribute filter expression
	// compiled to DNF.
	MatchExpr
)

// CompiledRoute is a route with its match expression pre-compiled for
// fast evaluation. Phase 5: a route owns its full destination set, the
// distribution mode, and the routing-priority position. The hot path
// walks the priority-sorted slice and returns the first match.
type CompiledRoute struct {
	RouteID      glid.GLID
	Name         string // tiebreaker when priorities collide
	Priority     int32
	Kind         MatchKind
	Expr         string         // original expression (for config reconstruction)
	DNF          *querylang.DNF // only set for MatchExpr
	Destinations []RouteDestination
	Distribution string // "fanout" (default), "round-robin", or "failover"
}

// RouteDestination is one delivery target on a compiled route. NodeID
// is empty for vaults owned by the local node; non-empty for remote
// vaults that need cross-node forwarding.
type RouteDestination struct {
	VaultID glid.GLID
	NodeID  string
}

// MatchResult pairs a destination vault with the node that owns it,
// the route that fired the match, and the route's distribution mode.
// Returned by RouteSet.Match — kept as a wire-stable shape so per-route
// stats and forward decisions stay unchanged across the Phase 5
// refactor.
type MatchResult struct {
	VaultID      glid.GLID
	NodeID       string
	RouteID      glid.GLID
	Distribution string
}

// CompileRoute parses a route's match expression and returns a
// CompiledRoute. Returns an error if the expression is invalid or
// uses unsupported predicates. Empty expression compiles to MatchNone
// (route is enrolled but never fires); "*" compiles to MatchAll.
func CompileRoute(routeID glid.GLID, name string, priority int32, expression string, dests []RouteDestination, distribution string) (*CompiledRoute, error) {
	r := &CompiledRoute{
		RouteID:      routeID,
		Name:         name,
		Priority:     priority,
		Expr:         expression,
		Destinations: dests,
		Distribution: distribution,
	}
	expr := strings.TrimSpace(expression)

	switch expr {
	case "":
		r.Kind = MatchNone
		return r, nil
	case "*":
		r.Kind = MatchAll
		return r, nil
	}

	parsed, err := querylang.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("invalid filter expression: %w", err)
	}
	if err := querylang.ValidateAttrFilter(parsed); err != nil {
		return nil, err
	}
	dnf := querylang.ToDNF(parsed)
	r.Kind = MatchExpr
	r.DNF = &dnf
	return r, nil
}

// RouteSet evaluates the cluster-wide routing table against a record
// and returns the destinations of the first matching route, in
// priority order. No-match drops silently — operators add an explicit
// catch-all route at the lowest priority for fallback handling.
type RouteSet struct {
	routes []*CompiledRoute // sorted by (priority asc, name asc)
}

// NewRouteSet creates a RouteSet from compiled routes, sorted by
// priority (lower fires first) with name as deterministic tiebreaker.
// The caller's slice is copied; callers may free their own copy.
func NewRouteSet(routes []*CompiledRoute) *RouteSet {
	cp := make([]*CompiledRoute, len(routes))
	copy(cp, routes)
	slices.SortFunc(cp, func(a, b *CompiledRoute) int {
		if c := cmp.Compare(a.Priority, b.Priority); c != 0 {
			return c
		}
		return cmp.Compare(a.Name, b.Name)
	})
	return &RouteSet{routes: cp}
}

// Routes returns the compiled routes in priority order. Read-only —
// callers must not mutate the returned slice.
func (rs *RouteSet) Routes() []*CompiledRoute {
	if rs == nil {
		return nil
	}
	return rs.routes
}

// Match returns the destinations of the first route that matches the
// record's attributes. Returns nil if no route matches (drop) or if
// the routing table is empty. Walks routes in priority order and
// stops on the first match.
//
// gastrolog-4kkoo (Phase 5): synthetic-attribute injection (`_source`,
// `_ingester`, `_vault`, `_reason`) lands in a follow-up commit. For
// now Match evaluates against the record's raw attrs only.
func (rs *RouteSet) Match(attrs chunk.Attributes) []MatchResult {
	if rs == nil {
		return nil
	}
	for _, r := range rs.routes {
		if !routeMatches(r, attrs) {
			continue
		}
		out := make([]MatchResult, 0, len(r.Destinations))
		for _, d := range r.Destinations {
			out = append(out, MatchResult{
				VaultID:      d.VaultID,
				NodeID:       d.NodeID,
				RouteID:      r.RouteID,
				Distribution: r.Distribution,
			})
		}
		return out
	}
	return nil
}

// routeMatches evaluates a single compiled route against a record's
// attributes. Pulled out so the same logic backs both Match and any
// future synthetic-attribute variants.
func routeMatches(r *CompiledRoute, attrs chunk.Attributes) bool {
	switch r.Kind {
	case MatchNone:
		return false
	case MatchAll:
		return true
	case MatchExpr:
		return querylang.MatchAttrs(r.DNF, attrs)
	}
	return false
}
