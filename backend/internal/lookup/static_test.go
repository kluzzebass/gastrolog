package lookup_test

import (
	"context"
	"testing"

	"gastrolog/internal/lookup"
)

func TestStaticLookup(t *testing.T) {
	rows := []lookup.StaticRow{
		{Values: map[string]string{"code": "US", "country": "United States", "region": "NA"}},
		{Values: map[string]string{"code": "DE", "country": "Germany", "region": "EU"}},
		{Values: map[string]string{"code": "JP", "country": "Japan", "region": "APAC"}},
	}

	st := lookup.NewStatic("countries", "code", []string{"country", "region"}, rows)

	// Check interface compliance.
	var _ lookup.LookupTable = st

	// Parameters: single-input "value".
	params := st.Parameters()
	if len(params) != 1 || params[0] != "value" {
		t.Fatalf("expected Parameters=[value], got %v", params)
	}

	// Suffixes: the value columns we specified.
	suffixes := st.Suffixes()
	if len(suffixes) != 2 {
		t.Fatalf("expected 2 suffixes, got %v", suffixes)
	}

	ctx := context.Background()

	// Hit.
	result := st.LookupValues(ctx, map[string]string{"value": "US"})
	if result == nil {
		t.Fatal("expected result for US")
	}
	if result["country"] != "United States" {
		t.Errorf("expected country=United States, got %q", result["country"])
	}
	if result["region"] != "NA" {
		t.Errorf("expected region=NA, got %q", result["region"])
	}

	// Miss.
	if got := st.LookupValues(ctx, map[string]string{"value": "XX"}); got != nil {
		t.Errorf("expected nil for missing key, got %v", got)
	}

	// Empty key.
	if got := st.LookupValues(ctx, map[string]string{"value": ""}); got != nil {
		t.Errorf("expected nil for empty key, got %v", got)
	}

	// No "value" param at all.
	if got := st.LookupValues(ctx, map[string]string{}); got != nil {
		t.Errorf("expected nil for missing value param, got %v", got)
	}
}

func TestStaticLookup_DuplicateKeys(t *testing.T) {
	rows := []lookup.StaticRow{
		{Values: map[string]string{"id": "1", "name": "first"}},
		{Values: map[string]string{"id": "1", "name": "second"}},
	}

	st := lookup.NewStatic("dupes", "id", []string{"name"}, rows)
	result := st.LookupValues(context.Background(), map[string]string{"value": "1"})
	if result == nil {
		t.Fatal("expected result")
	}
	if result["name"] != "first" {
		t.Errorf("expected first occurrence to win, got %q", result["name"])
	}
}

func TestStaticLookup_EmptyKeyRows(t *testing.T) {
	rows := []lookup.StaticRow{
		{Values: map[string]string{"id": "", "name": "no-key"}},
		{Values: map[string]string{"id": "ok", "name": "has-key"}},
	}

	st := lookup.NewStatic("empty-keys", "id", []string{"name"}, rows)

	// Row with empty key should be skipped.
	if got := st.LookupValues(context.Background(), map[string]string{"value": ""}); got != nil {
		t.Errorf("expected nil for empty key, got %v", got)
	}

	// Valid row still accessible.
	result := st.LookupValues(context.Background(), map[string]string{"value": "ok"})
	if result == nil || result["name"] != "has-key" {
		t.Errorf("expected name=has-key, got %v", result)
	}
}

func TestStaticLookup_AutoDiscoverColumns(t *testing.T) {
	rows := []lookup.StaticRow{
		{Values: map[string]string{"ip": "10.0.0.1", "host": "web-01", "dc": "us-east"}},
		{Values: map[string]string{"ip": "10.0.0.2", "host": "db-01"}},
	}

	// No explicit valueColumns — should auto-discover from rows.
	st := lookup.NewStatic("auto", "ip", nil, rows)

	suffixes := st.Suffixes()
	if len(suffixes) != 2 {
		t.Fatalf("expected 2 auto-discovered suffixes, got %v", suffixes)
	}

	result := st.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
	if result == nil {
		t.Fatal("expected result")
	}
	if result["host"] != "web-01" {
		t.Errorf("expected host=web-01, got %q", result["host"])
	}
	if result["dc"] != "us-east" {
		t.Errorf("expected dc=us-east, got %q", result["dc"])
	}
}

func TestStaticLookup_EmptyTable(t *testing.T) {
	st := lookup.NewStatic("empty", "key", []string{"val"}, nil)

	if got := st.LookupValues(context.Background(), map[string]string{"value": "anything"}); got != nil {
		t.Errorf("expected nil from empty table, got %v", got)
	}

	suffixes := st.Suffixes()
	if len(suffixes) != 1 || suffixes[0] != "val" {
		t.Errorf("expected suffixes=[val], got %v", suffixes)
	}
}

func TestStaticLookup_ReturnsCopy(t *testing.T) {
	rows := []lookup.StaticRow{
		{Values: map[string]string{"k": "a", "v": "original"}},
	}
	st := lookup.NewStatic("copy-test", "k", []string{"v"}, rows)

	result1 := st.LookupValues(context.Background(), map[string]string{"value": "a"})
	result1["v"] = "mutated"

	result2 := st.LookupValues(context.Background(), map[string]string{"value": "a"})
	if result2["v"] != "original" {
		t.Errorf("mutation leaked: expected original, got %q", result2["v"])
	}
}
