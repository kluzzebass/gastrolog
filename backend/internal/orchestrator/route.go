package orchestrator

import (
	"fmt"
	"strings"

	"gastrolog/internal/chunk"
	"gastrolog/internal/querylang"
)

// RouteKind identifies the type of routing rule.
type RouteKind int

const (
	// RouteNone means the store receives nothing (empty route).
	RouteNone RouteKind = iota
	// RouteCatchAll means the store receives all messages ("*").
	RouteCatchAll
	// RouteCatchRest means the store receives unmatched messages ("+").
	RouteCatchRest
	// RouteExpr means the store uses an expression filter.
	RouteExpr
)

// CompiledRoute is a pre-compiled routing rule for fast evaluation.
type CompiledRoute struct {
	StoreID string
	Kind    RouteKind
	Expr    string         // original route expression (for config reconstruction)
	DNF     *querylang.DNF // only set for RouteExpr
}

// CompileRoute parses a route string and returns a compiled route.
// Returns an error if the route expression is invalid or uses unsupported predicates.
func CompileRoute(storeID, route string) (*CompiledRoute, error) {
	route = strings.TrimSpace(route)

	// Empty route = receives nothing
	if route == "" {
		return &CompiledRoute{StoreID: storeID, Kind: RouteNone, Expr: ""}, nil
	}

	// Catch-all
	if route == "*" {
		return &CompiledRoute{StoreID: storeID, Kind: RouteCatchAll, Expr: "*"}, nil
	}

	// Catch-the-rest
	if route == "+" {
		return &CompiledRoute{StoreID: storeID, Kind: RouteCatchRest, Expr: "+"}, nil
	}

	// Parse as querylang expression
	expr, err := querylang.Parse(route)
	if err != nil {
		return nil, fmt.Errorf("invalid route expression: %w", err)
	}

	// Validate: reject token predicates (only attr-based filtering allowed)
	if err := validateRouteExpr(expr); err != nil {
		return nil, err
	}

	// Compile to DNF for fast evaluation
	dnf := querylang.ToDNF(expr)

	return &CompiledRoute{
		StoreID: storeID,
		Kind:    RouteExpr,
		Expr:    route,
		DNF:     &dnf,
	}, nil
}

// validateRouteExpr checks that the expression only uses attr-based predicates.
// Token predicates are not allowed because routing only looks at Attrs, not Raw.
func validateRouteExpr(expr querylang.Expr) error {
	switch e := expr.(type) {
	case *querylang.PredicateExpr:
		if e.Kind == querylang.PredToken {
			return fmt.Errorf("token predicates not allowed in routes (use key=value): %q", e.Value)
		}
		return nil

	case *querylang.AndExpr:
		for _, term := range e.Terms {
			if err := validateRouteExpr(term); err != nil {
				return err
			}
		}
		return nil

	case *querylang.OrExpr:
		for _, term := range e.Terms {
			if err := validateRouteExpr(term); err != nil {
				return err
			}
		}
		return nil

	case *querylang.NotExpr:
		return validateRouteExpr(e.Term)

	default:
		return nil
	}
}

// Router evaluates routing rules to determine which stores receive a message.
type Router struct {
	routes []*CompiledRoute
}

// NewRouter creates a router from compiled routes.
func NewRouter(routes []*CompiledRoute) *Router {
	return &Router{routes: routes}
}

// Route returns the store IDs that should receive a message with the given attributes.
//
// TODO(telemetry): Track routing metrics to detect message loss:
//   - messages_routed_total (counter per store)
//   - messages_dropped_total (counter for messages matching no routes)
//   - messages_ingested_total (total messages received)
//
// This enables alerting on drop rate and visibility into routing distribution.
func (r *Router) Route(attrs chunk.Attributes) []string {
	var result []string
	matchedExpr := false

	// First pass: evaluate expression routes and catch-all
	for _, route := range r.routes {
		switch route.Kind {
		case RouteNone:
			// Skip - receives nothing
		case RouteCatchAll:
			result = append(result, route.StoreID)
		case RouteExpr:
			if matchesAttrs(route.DNF, attrs) {
				result = append(result, route.StoreID)
				matchedExpr = true
			}
		case RouteCatchRest:
			// Handled in second pass
		}
	}

	// Second pass: catch-the-rest only if no expression routes matched
	if !matchedExpr {
		for _, route := range r.routes {
			if route.Kind == RouteCatchRest {
				result = append(result, route.StoreID)
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
