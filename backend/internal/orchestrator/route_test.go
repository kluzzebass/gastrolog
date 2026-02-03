package orchestrator

import (
	"testing"

	"gastrolog/internal/chunk"
)

func TestCompileRoute(t *testing.T) {
	tests := []struct {
		name      string
		route     string
		wantKind  RouteKind
		wantError bool
	}{
		{
			name:     "empty route",
			route:    "",
			wantKind: RouteNone,
		},
		{
			name:     "catch-all",
			route:    "*",
			wantKind: RouteCatchAll,
		},
		{
			name:     "catch-the-rest",
			route:    "+",
			wantKind: RouteCatchRest,
		},
		{
			name:     "simple kv expression",
			route:    "env=prod",
			wantKind: RouteExpr,
		},
		{
			name:     "complex expression",
			route:    "env=prod AND level=error",
			wantKind: RouteExpr,
		},
		{
			name:     "or expression",
			route:    "env=staging OR env=dev",
			wantKind: RouteExpr,
		},
		{
			name:     "key exists",
			route:    "env=*",
			wantKind: RouteExpr,
		},
		{
			name:     "value exists",
			route:    "*=error",
			wantKind: RouteExpr,
		},
		{
			name:     "not expression",
			route:    "NOT env=prod",
			wantKind: RouteExpr,
		},
		{
			name:      "token predicate rejected",
			route:     "error",
			wantError: true,
		},
		{
			name:      "token in and expression rejected",
			route:     "error AND env=prod",
			wantError: true,
		},
		{
			name:      "invalid syntax",
			route:     "env=prod AND",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			route, err := CompileRoute("test-store", tt.route)
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if route.Kind != tt.wantKind {
				t.Errorf("got kind %v, want %v", route.Kind, tt.wantKind)
			}
		})
	}
}

func TestRouterRoute(t *testing.T) {
	// Set up routes:
	// - prod-errors: env=prod AND level=error
	// - staging: env=staging
	// - archive: * (catch-all)
	// - unrouted: + (catch-the-rest)
	// - disabled: (empty, receives nothing)

	prodErrors, _ := CompileRoute("prod-errors", "env=prod AND level=error")
	staging, _ := CompileRoute("staging", "env=staging")
	archive, _ := CompileRoute("archive", "*")
	unrouted, _ := CompileRoute("unrouted", "+")
	disabled, _ := CompileRoute("disabled", "")

	router := NewRouter([]*CompiledRoute{prodErrors, staging, archive, unrouted, disabled})

	tests := []struct {
		name       string
		attrs      chunk.Attributes
		wantStores []string
	}{
		{
			name:       "prod error goes to prod-errors and archive",
			attrs:      chunk.Attributes{"env": "prod", "level": "error"},
			wantStores: []string{"prod-errors", "archive"},
		},
		{
			name:       "prod info goes to archive and unrouted (no expr match)",
			attrs:      chunk.Attributes{"env": "prod", "level": "info"},
			wantStores: []string{"archive", "unrouted"},
		},
		{
			name:       "staging goes to staging and archive",
			attrs:      chunk.Attributes{"env": "staging", "level": "debug"},
			wantStores: []string{"staging", "archive"},
		},
		{
			name:       "unknown env goes to archive and unrouted",
			attrs:      chunk.Attributes{"env": "unknown"},
			wantStores: []string{"archive", "unrouted"},
		},
		{
			name:       "no attrs goes to archive and unrouted",
			attrs:      chunk.Attributes{},
			wantStores: []string{"archive", "unrouted"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := router.Route(tt.attrs)
			if !sameElements(got, tt.wantStores) {
				t.Errorf("got stores %v, want %v", got, tt.wantStores)
			}
		})
	}
}

func TestRouterCatchRestOnlyWhenNoExprMatch(t *testing.T) {
	// catch-the-rest should NOT receive messages when an expression route matches
	prodOnly, _ := CompileRoute("prod", "env=prod")
	catchRest, _ := CompileRoute("unrouted", "+")

	router := NewRouter([]*CompiledRoute{prodOnly, catchRest})

	// Message matches prod - catchRest should NOT receive it
	got := router.Route(chunk.Attributes{"env": "prod"})
	if contains(got, "unrouted") {
		t.Errorf("catch-the-rest should not receive message that matched expression route: %v", got)
	}
	if !contains(got, "prod") {
		t.Errorf("prod should receive message: %v", got)
	}

	// Message doesn't match any expression - catchRest SHOULD receive it
	got = router.Route(chunk.Attributes{"env": "staging"})
	if !contains(got, "unrouted") {
		t.Errorf("catch-the-rest should receive unmatched message: %v", got)
	}
	if contains(got, "prod") {
		t.Errorf("prod should not receive non-matching message: %v", got)
	}
}

func TestRouterCatchAllDoesNotPreventCatchRest(t *testing.T) {
	// catch-all should NOT prevent catch-the-rest from receiving unmatched messages
	// (catch-all is not an "expression match")
	catchAll, _ := CompileRoute("archive", "*")
	catchRest, _ := CompileRoute("unrouted", "+")

	router := NewRouter([]*CompiledRoute{catchAll, catchRest})

	// No expression routes, so catch-the-rest should receive it too
	got := router.Route(chunk.Attributes{"env": "prod"})
	if !contains(got, "archive") {
		t.Errorf("catch-all should receive message: %v", got)
	}
	if !contains(got, "unrouted") {
		t.Errorf("catch-the-rest should receive message when no expression routes match: %v", got)
	}
}

func TestRouterEmptyRouteReceivesNothing(t *testing.T) {
	disabled, _ := CompileRoute("disabled", "")
	catchAll, _ := CompileRoute("archive", "*")

	router := NewRouter([]*CompiledRoute{disabled, catchAll})

	got := router.Route(chunk.Attributes{"env": "prod"})
	if contains(got, "disabled") {
		t.Errorf("empty route should not receive any messages: %v", got)
	}
	if !contains(got, "archive") {
		t.Errorf("catch-all should receive message: %v", got)
	}
}

func TestRouterCaseInsensitiveMatching(t *testing.T) {
	route, _ := CompileRoute("prod", "env=PROD")
	router := NewRouter([]*CompiledRoute{route})

	// Should match case-insensitively
	got := router.Route(chunk.Attributes{"env": "prod"})
	if !contains(got, "prod") {
		t.Errorf("should match case-insensitively: %v", got)
	}

	got = router.Route(chunk.Attributes{"ENV": "PROD"})
	if !contains(got, "prod") {
		t.Errorf("should match case-insensitively with uppercase key: %v", got)
	}
}

func TestRouterNotExpression(t *testing.T) {
	// Route that excludes prod
	notProd, _ := CompileRoute("not-prod", "NOT env=prod")
	router := NewRouter([]*CompiledRoute{notProd})

	// prod should not match
	got := router.Route(chunk.Attributes{"env": "prod"})
	if contains(got, "not-prod") {
		t.Errorf("NOT env=prod should not match env=prod: %v", got)
	}

	// staging should match
	got = router.Route(chunk.Attributes{"env": "staging"})
	if !contains(got, "not-prod") {
		t.Errorf("NOT env=prod should match env=staging: %v", got)
	}
}

func TestRouterKeyExists(t *testing.T) {
	// Route that matches any message with an "env" key
	hasEnv, _ := CompileRoute("has-env", "env=*")
	router := NewRouter([]*CompiledRoute{hasEnv})

	// Should match when key exists
	got := router.Route(chunk.Attributes{"env": "anything"})
	if !contains(got, "has-env") {
		t.Errorf("env=* should match when env key exists: %v", got)
	}

	// Should match case-insensitively
	got = router.Route(chunk.Attributes{"ENV": "anything"})
	if !contains(got, "has-env") {
		t.Errorf("env=* should match ENV key case-insensitively: %v", got)
	}

	// Should not match when key doesn't exist
	got = router.Route(chunk.Attributes{"other": "value"})
	if contains(got, "has-env") {
		t.Errorf("env=* should not match when env key is missing: %v", got)
	}
}

func TestRouterValueExists(t *testing.T) {
	// Route that matches any message with value "error"
	hasError, _ := CompileRoute("has-error", "*=error")
	router := NewRouter([]*CompiledRoute{hasError})

	// Should match when value exists
	got := router.Route(chunk.Attributes{"level": "error"})
	if !contains(got, "has-error") {
		t.Errorf("*=error should match when error value exists: %v", got)
	}

	// Should match case-insensitively
	got = router.Route(chunk.Attributes{"level": "ERROR"})
	if !contains(got, "has-error") {
		t.Errorf("*=error should match ERROR value case-insensitively: %v", got)
	}

	// Should match in any key
	got = router.Route(chunk.Attributes{"status": "error", "env": "prod"})
	if !contains(got, "has-error") {
		t.Errorf("*=error should match error in any key: %v", got)
	}

	// Should not match when value doesn't exist
	got = router.Route(chunk.Attributes{"level": "info"})
	if contains(got, "has-error") {
		t.Errorf("*=error should not match when error value is missing: %v", got)
	}
}

// Helper functions

func sameElements(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[string]int)
	for _, s := range a {
		m[s]++
	}
	for _, s := range b {
		m[s]--
		if m[s] < 0 {
			return false
		}
	}
	return true
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
