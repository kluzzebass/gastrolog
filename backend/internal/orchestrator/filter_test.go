package orchestrator

import (
	"slices"
	"testing"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

func TestCompileFilter(t *testing.T) {
	storeID := uuid.Must(uuid.NewV7())

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
			filter, err := CompileFilter(storeID, tt.filter)
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

	prodErrorsID := uuid.Must(uuid.NewV7())
	stagingID := uuid.Must(uuid.NewV7())
	archiveID := uuid.Must(uuid.NewV7())
	unfilteredID := uuid.Must(uuid.NewV7())
	disabledID := uuid.Must(uuid.NewV7())

	prodErrors, _ := CompileFilter(prodErrorsID, "env=prod AND level=error")
	staging, _ := CompileFilter(stagingID, "env=staging")
	archive, _ := CompileFilter(archiveID, "*")
	unfiltered, _ := CompileFilter(unfilteredID, "+")
	disabled, _ := CompileFilter(disabledID, "")

	filterSet := NewFilterSet([]*CompiledFilter{prodErrors, staging, archive, unfiltered, disabled})

	tests := []struct {
		name       string
		attrs      chunk.Attributes
		wantStores []uuid.UUID
	}{
		{
			name:       "prod error goes to prod-errors and archive",
			attrs:      chunk.Attributes{"env": "prod", "level": "error"},
			wantStores: []uuid.UUID{prodErrorsID, archiveID},
		},
		{
			name:       "prod info goes to archive and unfiltered (no expr match)",
			attrs:      chunk.Attributes{"env": "prod", "level": "info"},
			wantStores: []uuid.UUID{archiveID, unfilteredID},
		},
		{
			name:       "staging goes to staging and archive",
			attrs:      chunk.Attributes{"env": "staging", "level": "debug"},
			wantStores: []uuid.UUID{stagingID, archiveID},
		},
		{
			name:       "unknown env goes to archive and unfiltered",
			attrs:      chunk.Attributes{"env": "unknown"},
			wantStores: []uuid.UUID{archiveID, unfilteredID},
		},
		{
			name:       "no attrs goes to archive and unfiltered",
			attrs:      chunk.Attributes{},
			wantStores: []uuid.UUID{archiveID, unfilteredID},
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
	prodID := uuid.Must(uuid.NewV7())
	unfilteredID := uuid.Must(uuid.NewV7())

	prodOnly, _ := CompileFilter(prodID, "env=prod")
	catchRest, _ := CompileFilter(unfilteredID, "+")

	filterSet := NewFilterSet([]*CompiledFilter{prodOnly, catchRest})

	// Message matches prod - catchRest should NOT receive it
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if containsUUID(got, unfilteredID) {
		t.Errorf("catch-the-rest should not receive message that matched expression filter: %v", got)
	}
	if !containsUUID(got, prodID) {
		t.Errorf("prod should receive message: %v", got)
	}

	// Message doesn't match any expression - catchRest SHOULD receive it
	got = filterSet.Match(chunk.Attributes{"env": "staging"})
	if !containsUUID(got, unfilteredID) {
		t.Errorf("catch-the-rest should receive unmatched message: %v", got)
	}
	if containsUUID(got, prodID) {
		t.Errorf("prod should not receive non-matching message: %v", got)
	}
}

func TestFilterSetCatchAllDoesNotPreventCatchRest(t *testing.T) {
	// catch-all should NOT prevent catch-the-rest from receiving unmatched messages
	// (catch-all is not an "expression match")
	archiveID := uuid.Must(uuid.NewV7())
	unfilteredID := uuid.Must(uuid.NewV7())

	catchAll, _ := CompileFilter(archiveID, "*")
	catchRest, _ := CompileFilter(unfilteredID, "+")

	filterSet := NewFilterSet([]*CompiledFilter{catchAll, catchRest})

	// No expression filters, so catch-the-rest should receive it too
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if !containsUUID(got, archiveID) {
		t.Errorf("catch-all should receive message: %v", got)
	}
	if !containsUUID(got, unfilteredID) {
		t.Errorf("catch-the-rest should receive message when no expression filters match: %v", got)
	}
}

func TestFilterSetEmptyFilterReceivesNothing(t *testing.T) {
	disabledID := uuid.Must(uuid.NewV7())
	archiveID := uuid.Must(uuid.NewV7())

	disabled, _ := CompileFilter(disabledID, "")
	catchAll, _ := CompileFilter(archiveID, "*")

	filterSet := NewFilterSet([]*CompiledFilter{disabled, catchAll})

	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if containsUUID(got, disabledID) {
		t.Errorf("empty filter should not receive any messages: %v", got)
	}
	if !containsUUID(got, archiveID) {
		t.Errorf("catch-all should receive message: %v", got)
	}
}

func TestFilterSetCaseInsensitiveMatching(t *testing.T) {
	prodID := uuid.Must(uuid.NewV7())

	filter, _ := CompileFilter(prodID, "env=PROD")
	filterSet := NewFilterSet([]*CompiledFilter{filter})

	// Should match case-insensitively
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if !containsUUID(got, prodID) {
		t.Errorf("should match case-insensitively: %v", got)
	}

	got = filterSet.Match(chunk.Attributes{"ENV": "PROD"})
	if !containsUUID(got, prodID) {
		t.Errorf("should match case-insensitively with uppercase key: %v", got)
	}
}

func TestFilterSetNotExpression(t *testing.T) {
	notProdID := uuid.Must(uuid.NewV7())

	// Filter that excludes prod
	notProd, _ := CompileFilter(notProdID, "NOT env=prod")
	filterSet := NewFilterSet([]*CompiledFilter{notProd})

	// prod should not match
	got := filterSet.Match(chunk.Attributes{"env": "prod"})
	if containsUUID(got, notProdID) {
		t.Errorf("NOT env=prod should not match env=prod: %v", got)
	}

	// staging should match
	got = filterSet.Match(chunk.Attributes{"env": "staging"})
	if !containsUUID(got, notProdID) {
		t.Errorf("NOT env=prod should match env=staging: %v", got)
	}
}

func TestFilterSetKeyExists(t *testing.T) {
	hasEnvID := uuid.Must(uuid.NewV7())

	// Filter that matches any message with an "env" key
	hasEnv, _ := CompileFilter(hasEnvID, "env=*")
	filterSet := NewFilterSet([]*CompiledFilter{hasEnv})

	// Should match when key exists
	got := filterSet.Match(chunk.Attributes{"env": "anything"})
	if !containsUUID(got, hasEnvID) {
		t.Errorf("env=* should match when env key exists: %v", got)
	}

	// Should match case-insensitively
	got = filterSet.Match(chunk.Attributes{"ENV": "anything"})
	if !containsUUID(got, hasEnvID) {
		t.Errorf("env=* should match ENV key case-insensitively: %v", got)
	}

	// Should not match when key doesn't exist
	got = filterSet.Match(chunk.Attributes{"other": "value"})
	if containsUUID(got, hasEnvID) {
		t.Errorf("env=* should not match when env key is missing: %v", got)
	}
}

func TestFilterSetValueExists(t *testing.T) {
	hasErrorID := uuid.Must(uuid.NewV7())

	// Filter that matches any message with value "error"
	hasError, _ := CompileFilter(hasErrorID, "*=error")
	filterSet := NewFilterSet([]*CompiledFilter{hasError})

	// Should match when value exists
	got := filterSet.Match(chunk.Attributes{"level": "error"})
	if !containsUUID(got, hasErrorID) {
		t.Errorf("*=error should match when error value exists: %v", got)
	}

	// Should match case-insensitively
	got = filterSet.Match(chunk.Attributes{"level": "ERROR"})
	if !containsUUID(got, hasErrorID) {
		t.Errorf("*=error should match ERROR value case-insensitively: %v", got)
	}

	// Should match in any key
	got = filterSet.Match(chunk.Attributes{"status": "error", "env": "prod"})
	if !containsUUID(got, hasErrorID) {
		t.Errorf("*=error should match error in any key: %v", got)
	}

	// Should not match when value doesn't exist
	got = filterSet.Match(chunk.Attributes{"level": "info"})
	if containsUUID(got, hasErrorID) {
		t.Errorf("*=error should not match when error value is missing: %v", got)
	}
}

func TestFilterSetAddOrUpdate(t *testing.T) {
	storeA := uuid.Must(uuid.NewV7())
	storeB := uuid.Must(uuid.NewV7())

	// AddOrUpdate on nil receiver creates a fresh set.
	fs, err := (*FilterSet)(nil).AddOrUpdate(storeA, "env=prod")
	if err != nil {
		t.Fatalf("AddOrUpdate on nil: %v", err)
	}
	got := fs.Match(chunk.Attributes{"env": "prod"})
	if !containsUUID(got, storeA) {
		t.Error("storeA should match after AddOrUpdate")
	}

	// Add a second store.
	fs, err = fs.AddOrUpdate(storeB, "*")
	if err != nil {
		t.Fatalf("AddOrUpdate storeB: %v", err)
	}
	got = fs.Match(chunk.Attributes{"env": "prod"})
	if !containsUUID(got, storeA) || !containsUUID(got, storeB) {
		t.Error("both stores should match")
	}

	// Update storeA's filter.
	fs, err = fs.AddOrUpdate(storeA, "env=staging")
	if err != nil {
		t.Fatalf("AddOrUpdate update storeA: %v", err)
	}
	got = fs.Match(chunk.Attributes{"env": "prod"})
	if containsUUID(got, storeA) {
		t.Error("storeA should no longer match env=prod after update")
	}
	got = fs.Match(chunk.Attributes{"env": "staging"})
	if !containsUUID(got, storeA) {
		t.Error("storeA should match env=staging after update")
	}

	// Invalid expression returns error.
	_, err = fs.AddOrUpdate(storeA, "(unclosed")
	if err == nil {
		t.Error("expected error for invalid expression")
	}
}

func TestFilterSetWithout(t *testing.T) {
	storeA := uuid.Must(uuid.NewV7())
	storeB := uuid.Must(uuid.NewV7())
	storeC := uuid.Must(uuid.NewV7())

	filterA, _ := CompileFilter(storeA, "env=prod")
	filterB, _ := CompileFilter(storeB, "env=staging")
	filterC, _ := CompileFilter(storeC, "*")

	fs := NewFilterSet([]*CompiledFilter{filterA, filterB, filterC})

	// Without a single store.
	fs2 := fs.Without(storeA)
	got := fs2.Match(chunk.Attributes{"env": "prod"})
	if containsUUID(got, storeA) {
		t.Error("storeA should be removed")
	}
	if !containsUUID(got, storeC) {
		t.Error("storeC (catch-all) should remain")
	}

	// Without multiple stores.
	fs3 := fs.Without(storeA, storeB)
	got = fs3.Match(chunk.Attributes{"env": "staging"})
	if containsUUID(got, storeA) || containsUUID(got, storeB) {
		t.Error("storeA and storeB should be removed")
	}
	if !containsUUID(got, storeC) {
		t.Error("storeC should remain")
	}

	// Without all stores returns nil.
	fs4 := fs.Without(storeA, storeB, storeC)
	if fs4 != nil {
		t.Error("expected nil when all stores removed")
	}

	// Without on nil receiver returns nil.
	fs5 := (*FilterSet)(nil).Without(storeA)
	if fs5 != nil {
		t.Error("Without on nil should return nil")
	}
}

// Helper functions

func sameElements(a, b []uuid.UUID) bool {
	if len(a) != len(b) {
		return false
	}
	m := make(map[uuid.UUID]int)
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

func containsUUID(slice []uuid.UUID, id uuid.UUID) bool {
	return slices.Contains(slice, id)
}
