package orchestrator

import (
	"testing"

	"gastrolog/internal/chunk"
)

func TestCompileFilter(t *testing.T) {
	tests := []struct {
		name      string
		filter    string
		wantKind  FilterKind
		wantError bool
	}{
		{
			name:     "empty filter",
			filter:   "",
			wantKind: FilterNone,
		},
		{
			name:     "catch-all",
			filter:   "*",
			wantKind: FilterCatchAll,
		},
		{
			name:     "catch-the-rest",
			filter:   "+",
			wantKind: FilterCatchRest,
		},
		{
			name:     "simple kv expression",
			filter:   "env=prod",
			wantKind: FilterExpr,
		},
		{
			name:     "complex expression",
			filter:   "env=prod AND level=error",
			wantKind: FilterExpr,
		},
		{
			name:     "or expression",
			filter:   "env=staging OR env=dev",
			wantKind: FilterExpr,
		},
		{
			name:     "key exists",
			filter:   "env=*",
			wantKind: FilterExpr,
		},
		{
			name:     "value exists",
			filter:   "*=error",
			wantKind: FilterExpr,
		},
		{
			name:     "not expression",
			filter:   "NOT env=prod",
			wantKind: FilterExpr,
		},
		{
			name:      "token predicate rejected",
			filter:    "error",
			wantError: true,
		},
		{
			name:      "token in and expression rejected",
			filter:    "error AND env=prod",
			wantError: true,
		},
		{
			name:      "invalid syntax",
			filter:    "env=prod AND",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := CompileFilter("test-store", tt.filter)
			if tt.wantError {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if filter.Kind != tt.wantKind {
				t.Errorf("got kind %v, want %v", filter.Kind, tt.wantKind)
			}
		})
	}
}

func TestFilterSetMatch(t *testing.T) {
	// Set up filters:
	// - prod-errors: env=prod AND level=error
	// - staging: env=staging
	// - archive: * (catch-all)
	// - unfiltered: + (catch-the-rest)
	// - disabled: (empty, receives nothing)

	prodErrors, _ := CompileFilter("prod-errors", "env=prod AND level=error")
	staging, _ := CompileFilter("staging", "env=staging")
	archive, _ := CompileFilter("archive", "*")
	unfiltered, _ := CompileFilter("unfiltered", "+")
	disabled, _ := CompileFilter("disabled", "")

	filterSet := NewFilterSet([]*CompiledFilter{prodErrors, staging, archive, unfiltered, disabled})

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
			name:       "prod info goes to archive and unfiltered (no expr match)",
			attrs:      chunk.Attributes{"env": "prod", "level": "info"},
			wantStores: []string{"archive", "unfiltered"},
		},
		{
			name:       "staging goes to staging and archive",
			attrs:      chunk.Attributes{"env": "staging", "level": "debug"},
			wantStores: []string{"staging", "archive"},
		},
		{
			name:       "unknown env goes to archive and unfiltered",
			attrs:      chunk.Attributes{"env": "unknown"},
			wantStores: []string{"archive", "unfiltered"},
		},
		{
			name:       "no attrs goes to archive and unfiltered",
			attrs:      chunk.Attributes{},
			wantStores: []string{"archive", "unfiltered"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filterSet.Match(tt.attrs)
			if !sameElements(got, tt.wantStores) {
				t.Errorf("got stores %v, want %v", got, tt.wantStores)
			}
		})
	}
}

func TestFilterSetCatchRestOnlyWhenNoExprMatch(t *testing.T) {
	// catch-the-rest should NOT receive messages when an expression filter matches
	prodOnly, _ := CompileFilter("prod", "env=prod")
	catchRest, _ := CompileFilter("unfiltered", "+")

	filterSet := NewFilterSet([]*CompiledFilter{prodOnly, catchRest})

	// Message matches prod - catchRest should NOT receive it
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if contains(got, "unfiltered") {
		t.Errorf("catch-the-rest should not receive message that matched expression filter: %v", got)
	}
	if !contains(got, "prod") {
		t.Errorf("prod should receive message: %v", got)
	}

	// Message doesn't match any expression - catchRest SHOULD receive it
	got = filterSet.Match(chunk.Attributes{"env": "staging"})
	if !contains(got, "unfiltered") {
		t.Errorf("catch-the-rest should receive unmatched message: %v", got)
	}
	if contains(got, "prod") {
		t.Errorf("prod should not receive non-matching message: %v", got)
	}
}

func TestFilterSetCatchAllDoesNotPreventCatchRest(t *testing.T) {
	// catch-all should NOT prevent catch-the-rest from receiving unmatched messages
	// (catch-all is not an "expression match")
	catchAll, _ := CompileFilter("archive", "*")
	catchRest, _ := CompileFilter("unfiltered", "+")

	filterSet := NewFilterSet([]*CompiledFilter{catchAll, catchRest})

	// No expression filters, so catch-the-rest should receive it too
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if !contains(got, "archive") {
		t.Errorf("catch-all should receive message: %v", got)
	}
	if !contains(got, "unfiltered") {
		t.Errorf("catch-the-rest should receive message when no expression filters match: %v", got)
	}
}

func TestFilterSetEmptyFilterReceivesNothing(t *testing.T) {
	disabled, _ := CompileFilter("disabled", "")
	catchAll, _ := CompileFilter("archive", "*")

	filterSet := NewFilterSet([]*CompiledFilter{disabled, catchAll})

	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if contains(got, "disabled") {
		t.Errorf("empty filter should not receive any messages: %v", got)
	}
	if !contains(got, "archive") {
		t.Errorf("catch-all should receive message: %v", got)
	}
}

func TestFilterSetCaseInsensitiveMatching(t *testing.T) {
	filter, _ := CompileFilter("prod", "env=PROD")
	filterSet := NewFilterSet([]*CompiledFilter{filter})

	// Should match case-insensitively
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if !contains(got, "prod") {
		t.Errorf("should match case-insensitively: %v", got)
	}

	got = filterSet.Match(chunk.Attributes{"ENV": "PROD"})
	if !contains(got, "prod") {
		t.Errorf("should match case-insensitively with uppercase key: %v", got)
	}
}

func TestFilterSetNotExpression(t *testing.T) {
	// Filter that excludes prod
	notProd, _ := CompileFilter("not-prod", "NOT env=prod")
	filterSet := NewFilterSet([]*CompiledFilter{notProd})

	// prod should not match
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if contains(got, "not-prod") {
		t.Errorf("NOT env=prod should not match env=prod: %v", got)
	}

	// staging should match
	got = filterSet.Match(chunk.Attributes{"env": "staging"})
	if !contains(got, "not-prod") {
		t.Errorf("NOT env=prod should match env=staging: %v", got)
	}
}

func TestFilterSetKeyExists(t *testing.T) {
	// Filter that matches any message with an "env" key
	hasEnv, _ := CompileFilter("has-env", "env=*")
	filterSet := NewFilterSet([]*CompiledFilter{hasEnv})

	// Should match when key exists
	got := filterSet.Match(chunk.Attributes{"env": "anything"})
	if !contains(got, "has-env") {
		t.Errorf("env=* should match when env key exists: %v", got)
	}

	// Should match case-insensitively
	got = filterSet.Match(chunk.Attributes{"ENV": "anything"})
	if !contains(got, "has-env") {
		t.Errorf("env=* should match ENV key case-insensitively: %v", got)
	}

	// Should not match when key doesn't exist
	got = filterSet.Match(chunk.Attributes{"other": "value"})
	if contains(got, "has-env") {
		t.Errorf("env=* should not match when env key is missing: %v", got)
	}
}

func TestFilterSetValueExists(t *testing.T) {
	// Filter that matches any message with value "error"
	hasError, _ := CompileFilter("has-error", "*=error")
	filterSet := NewFilterSet([]*CompiledFilter{hasError})

	// Should match when value exists
	got := filterSet.Match(chunk.Attributes{"level": "error"})
	if !contains(got, "has-error") {
		t.Errorf("*=error should match when error value exists: %v", got)
	}

	// Should match case-insensitively
	got = filterSet.Match(chunk.Attributes{"level": "ERROR"})
	if !contains(got, "has-error") {
		t.Errorf("*=error should match ERROR value case-insensitively: %v", got)
	}

	// Should match in any key
	got = filterSet.Match(chunk.Attributes{"status": "error", "env": "prod"})
	if !contains(got, "has-error") {
		t.Errorf("*=error should match error in any key: %v", got)
	}

	// Should not match when value doesn't exist
	got = filterSet.Match(chunk.Attributes{"level": "info"})
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
