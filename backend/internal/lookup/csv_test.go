package lookup_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/lookup"
)

const testCSV = `ip,hostname,datacenter,owner
10.0.1.1,web-01,us-east,platform
10.0.1.2,web-02,us-east,platform
10.0.2.1,db-01,us-west,storage
192.168.1.1,dev-box,local,eng
`

func TestCSVLookup(t *testing.T) {
	path := writeTestFile(t, "test.csv", testCSV)

	ct := lookup.NewCSV(lookup.CSVConfig{})
	defer ct.Close()

	if err := ct.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Check suffixes — should be all non-key columns.
	suffixes := ct.Suffixes()
	if len(suffixes) != 3 {
		t.Fatalf("expected 3 suffixes, got %v", suffixes)
	}

	// Lookup existing key.
	result := ct.LookupValues(context.Background(), map[string]string{"value": "10.0.1.1"})
	if result == nil {
		t.Fatal("expected result for 10.0.1.1")
	}
	if result["hostname"] != "web-01" {
		t.Errorf("expected hostname=web-01, got %q", result["hostname"])
	}
	if result["datacenter"] != "us-east" {
		t.Errorf("expected datacenter=us-east, got %q", result["datacenter"])
	}
	if result["owner"] != "platform" {
		t.Errorf("expected owner=platform, got %q", result["owner"])
	}

	// Lookup missing key returns nil.
	miss := ct.LookupValues(context.Background(), map[string]string{"value": "99.99.99.99"})
	if miss != nil {
		t.Errorf("expected nil for missing key, got %v", miss)
	}
}

func TestCSVLookupCustomKeyColumn(t *testing.T) {
	path := writeTestFile(t, "test.csv", testCSV)

	ct := lookup.NewCSV(lookup.CSVConfig{KeyColumn: "hostname"})
	defer ct.Close()

	if err := ct.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	result := ct.LookupValues(context.Background(), map[string]string{"value": "db-01"})
	if result == nil {
		t.Fatal("expected result for hostname db-01")
	}
	if result["ip"] != "10.0.2.1" {
		t.Errorf("expected ip=10.0.2.1, got %q", result["ip"])
	}
	if result["datacenter"] != "us-west" {
		t.Errorf("expected datacenter=us-west, got %q", result["datacenter"])
	}
}

func TestCSVLookupValueColumns(t *testing.T) {
	path := writeTestFile(t, "test.csv", testCSV)

	ct := lookup.NewCSV(lookup.CSVConfig{ValueColumns: []string{"hostname", "owner"}})
	defer ct.Close()

	if err := ct.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	suffixes := ct.Suffixes()
	if len(suffixes) != 2 {
		t.Fatalf("expected 2 suffixes, got %v", suffixes)
	}

	result := ct.LookupValues(context.Background(), map[string]string{"value": "192.168.1.1"})
	if result == nil {
		t.Fatal("expected result")
	}
	if result["hostname"] != "dev-box" {
		t.Errorf("expected hostname=dev-box, got %q", result["hostname"])
	}
	if _, hasDC := result["datacenter"]; hasDC {
		t.Error("datacenter should be filtered out by ValueColumns")
	}
}

func TestCSVLookupTSV(t *testing.T) {
	tsv := "code\tdescription\n200\tOK\n404\tNot Found\n500\tInternal Server Error\n"
	path := writeTestFile(t, "test.tsv", tsv)

	ct := lookup.NewCSV(lookup.CSVConfig{Delimiter: '\t'})
	defer ct.Close()

	if err := ct.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	result := ct.LookupValues(context.Background(), map[string]string{"value": "404"})
	if result == nil {
		t.Fatal("expected result for 404")
	}
	if result["description"] != "Not Found" {
		t.Errorf("expected description='Not Found', got %q", result["description"])
	}
}

func TestCSVLookupReload(t *testing.T) {
	path := writeTestFile(t, "test.csv", testCSV)

	ct := lookup.NewCSV(lookup.CSVConfig{})
	defer ct.Close()

	if err := ct.Load(path); err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Verify initial data.
	result := ct.LookupValues(context.Background(), map[string]string{"value": "10.0.1.1"})
	if result == nil || result["hostname"] != "web-01" {
		t.Fatal("expected web-01 before reload")
	}

	// Overwrite file and reload.
	updated := "ip,hostname,datacenter,owner\n10.0.1.1,web-01-v2,eu-west,infra\n"
	if err := os.WriteFile(path, []byte(updated), 0o644); err != nil {
		t.Fatalf("rewrite: %v", err)
	}
	if err := ct.Load(path); err != nil {
		t.Fatalf("reload: %v", err)
	}

	result = ct.LookupValues(context.Background(), map[string]string{"value": "10.0.1.1"})
	if result == nil || result["hostname"] != "web-01-v2" {
		t.Fatal("expected web-01-v2 after reload")
	}

	// Old key should be gone.
	miss := ct.LookupValues(context.Background(), map[string]string{"value": "10.0.2.1"})
	if miss != nil {
		t.Error("expected nil for removed key after reload")
	}
}

func TestCSVLookupErrors(t *testing.T) {
	ct := lookup.NewCSV(lookup.CSVConfig{})

	// Non-existent file.
	if err := ct.Load("/nonexistent.csv"); err == nil {
		t.Error("expected error for missing file")
	}

	// Empty file.
	path := writeTestFile(t, "empty.csv", "")
	if err := ct.Load(path); err == nil {
		t.Error("expected error for empty file")
	}

	// Single column (no value columns).
	path = writeTestFile(t, "one.csv", "key\nfoo\nbar\n")
	if err := ct.Load(path); err == nil {
		t.Error("expected error for single-column CSV")
	}

	// Bad key column name.
	ct2 := lookup.NewCSV(lookup.CSVConfig{KeyColumn: "nonexistent"})
	path = writeTestFile(t, "test.csv", testCSV)
	if err := ct2.Load(path); err == nil {
		t.Error("expected error for nonexistent key column")
	}
}

func writeTestFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
	return path
}
