package lookup

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestJSONFile_ObjectKeyLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{
		"192.168.1.1": {"hostname": "web-1", "env": "prod"},
		"10.0.0.1":    {"hostname": "db-1",  "env": "staging"}
	}`), 0o644)

	// jq: convert top-level object to array of objects with "ip" key.
	jf, err := NewJSONFile(JSONFileConfig{
		Query:     `to_entries | map(.value + {ip: .key})`,
		KeyColumn: "ip",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "192.168.1.1"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["hostname"] != "web-1" {
		t.Errorf("hostname = %q, want web-1", result["hostname"])
	}
	if result["env"] != "prod" {
		t.Errorf("env = %q, want prod", result["env"])
	}

	// Miss returns nil.
	if jf.LookupValues(context.Background(), map[string]string{"value": "unknown"}) != nil {
		t.Error("expected nil for unknown key")
	}

	// Empty value returns nil.
	if jf.LookupValues(context.Background(), map[string]string{"value": ""}) != nil {
		t.Error("expected nil for empty value")
	}
}

func TestJSONFile_ArrayLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "192.168.1.1", "hostname": "web-1", "env": "prod"},
		{"ip": "10.0.0.1",    "hostname": "db-1",  "env": "staging"}
	]}`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{
		Query:     `.hosts`,
		KeyColumn: "ip",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "192.168.1.1"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["hostname"] != "web-1" {
		t.Errorf("hostname = %q, want web-1", result["hostname"])
	}
	if result["env"] != "prod" {
		t.Errorf("env = %q, want prod", result["env"])
	}

	// Miss returns nil.
	if jf.LookupValues(context.Background(), map[string]string{"value": "999.999.999.999"}) != nil {
		t.Error("expected nil for non-matching IP")
	}
}

func TestJSONFile_ValueColumns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "192.168.1.1", "hostname": "web-1", "env": "prod", "role": "web"}
	]}`), 0o644)

	// Only extract hostname and env, not role.
	jf, err := NewJSONFile(JSONFileConfig{
		Query:        `.hosts`,
		KeyColumn:    "ip",
		ValueColumns: []string{"hostname", "env"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "192.168.1.1"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["hostname"] != "web-1" {
		t.Errorf("hostname = %q, want web-1", result["hostname"])
	}
	if result["env"] != "prod" {
		t.Errorf("env = %q, want prod", result["env"])
	}
	// "role" should NOT appear since we restricted value_columns.
	if _, ok := result["role"]; ok {
		t.Error("role should not be in result when value_columns is restricted")
	}
}

func TestJSONFile_Suffixes(t *testing.T) {
	jf, err := NewJSONFile(JSONFileConfig{Query: `.hosts`})
	if err != nil {
		t.Fatal(err)
	}
	if jf.Suffixes() != nil {
		t.Error("suffixes should be nil before load")
	}

	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "10.0.0.1", "hostname": "web-1", "env": "prod"}
	]}`), 0o644)

	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	// Suffixes are discovered at load time (all non-key columns).
	suffixes := jf.Suffixes()
	if len(suffixes) == 0 {
		t.Fatal("expected non-empty suffixes after load")
	}

	// Key column defaults to first (sorted) = "env", so suffixes should include "hostname" and "ip".
	// With default key = "env" (first alphabetically), suffixes = ["hostname", "ip"].
	want := map[string]bool{"hostname": true, "ip": true}
	for _, s := range suffixes {
		if !want[s] {
			t.Errorf("unexpected suffix %q", s)
		}
	}
}

func TestJSONFile_SuffixesWithExplicitKey(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "10.0.0.1", "hostname": "web-1", "env": "prod"}
	]}`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.hosts`, KeyColumn: "ip"})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	suffixes := jf.Suffixes()
	want := map[string]bool{"env": true, "hostname": true}
	if len(suffixes) != len(want) {
		t.Fatalf("suffixes = %v, want %v", suffixes, want)
	}
	for _, s := range suffixes {
		if !want[s] {
			t.Errorf("unexpected suffix %q", s)
		}
	}
}

func TestJSONFile_Parameters(t *testing.T) {
	jf, err := NewJSONFile(JSONFileConfig{Query: `.`})
	if err != nil {
		t.Fatal(err)
	}
	params := jf.Parameters()
	if len(params) != 1 || params[0] != "value" {
		t.Errorf("Parameters() = %v, want [value]", params)
	}
}

func TestJSONFile_DuplicateKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`[
		{"ip": "10.0.0.1", "hostname": "web-1"},
		{"ip": "10.0.0.1", "hostname": "web-2"},
		{"ip": "10.0.0.2", "hostname": "db-1"}
	]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`, KeyColumn: "ip"})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	// First occurrence wins.
	result := jf.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["hostname"] != "web-1" {
		t.Errorf("hostname = %q, want web-1 (first occurrence)", result["hostname"])
	}

	// DuplicateKeys reports the count.
	if dups := jf.DuplicateKeys(); dups != 1 {
		t.Errorf("DuplicateKeys() = %d, want 1", dups)
	}
}

func TestJSONFile_HotReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`[
		{"ip": "10.0.0.1", "val": "original"}
	]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`, KeyColumn: "ip"})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	if err := jf.WatchFile(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	r := jf.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
	if r["val"] != "original" {
		t.Fatalf("val = %q, want original", r["val"])
	}

	// Overwrite the file and wait for the watcher to pick it up.
	os.WriteFile(path, []byte(`[
		{"ip": "10.0.0.1", "val": "reloaded"}
	]`), 0o644)

	// Poll for the reload (fsnotify is async).
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for hot reload")
		default:
			r := jf.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
			if r != nil && r["val"] == "reloaded" {
				return // Success
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestJSONFile_MmapRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`[{"k": "a", "v": "1"}]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}

	// Reload — old mmap should be released.
	os.WriteFile(path, []byte(`[{"k": "b", "v": "2"}]`), 0o644)
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}

	// Close — final mmap should be released.
	jf.Close()

	// If mmap wasn't properly released, the process would leak file descriptors.
	// We can't easily assert this, but the test not crashing is a good sign.
}

func TestJSONFile_NestedValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`[
		{"ip": "10.0.0.1", "name": "web", "tags": ["prod", "us"], "nested": {"deep": "value"}}
	]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`, KeyColumn: "ip"})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "web" {
		t.Errorf("name = %q, want web", result["name"])
	}
	// Nested objects/arrays are JSON-encoded by flattenScalars.
	if result["tags"] == "" {
		t.Error("tags should be JSON-encoded, not empty")
	}
	if result["nested"] == "" {
		t.Error("nested should be JSON-encoded, not empty")
	}
}

func TestJSONFile_JQTransform(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	// Raw data is a nested structure; jq flattens it into a table.
	os.WriteFile(path, []byte(`{"servers": {
		"us-east": [{"name": "web-1", "port": 8080}, {"name": "web-2", "port": 8081}],
		"eu-west": [{"name": "api-1", "port": 9090}]
	}}`), 0o644)

	// jq flattens: for each region, add region to each server object.
	jf, err := NewJSONFile(JSONFileConfig{
		Query:     `.servers | to_entries | map(.key as $r | .value[] | . + {region: $r})`,
		KeyColumn: "name",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "web-1"})
	if result == nil {
		t.Fatal("expected non-nil result for web-1")
	}
	if result["region"] != "us-east" {
		t.Errorf("region = %q, want us-east", result["region"])
	}
	if result["port"] != "8080" {
		t.Errorf("port = %q, want 8080", result["port"])
	}

	result2 := jf.LookupValues(context.Background(), map[string]string{"value": "api-1"})
	if result2 == nil {
		t.Fatal("expected non-nil result for api-1")
	}
	if result2["region"] != "eu-west" {
		t.Errorf("region = %q, want eu-west", result2["region"])
	}
}

func TestJSONFile_DuplicateKeysFirstWins(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`[
		{"id": "a", "val": "first"},
		{"id": "a", "val": "second"}
	]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`, KeyColumn: "id"})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "a"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["val"] != "first" {
		t.Errorf("val = %q, want first (first occurrence wins)", result["val"])
	}
}

func TestJSONFile_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(``), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err == nil {
		t.Error("expected error loading empty file")
	}
}

func TestJSONFile_InvalidJQ(t *testing.T) {
	_, err := NewJSONFile(JSONFileConfig{Query: `.[[[`})
	if err == nil {
		t.Error("expected error for invalid jq expression")
	}
}

func TestJSONFile_NoObjectResults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	// jq expression produces scalars, not objects.
	os.WriteFile(path, []byte(`[1, 2, 3]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.[]`})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err == nil {
		t.Error("expected error when jq produces no object results")
	}
}

func TestJSONFile_LookupReturnsCopy(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`[{"id": "x", "val": "orig"}]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`, KeyColumn: "id"})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	r1 := jf.LookupValues(context.Background(), map[string]string{"value": "x"})
	r1["val"] = "mutated"

	r2 := jf.LookupValues(context.Background(), map[string]string{"value": "x"})
	if r2["val"] != "orig" {
		t.Errorf("mutation leaked: val = %q, want orig", r2["val"])
	}
}

func TestJSONFile_NilValues(t *testing.T) {
	jf, err := NewJSONFile(JSONFileConfig{Query: `.`})
	if err != nil {
		t.Fatal(err)
	}
	if jf.LookupValues(context.Background(), nil) != nil {
		t.Error("expected nil for nil values")
	}
}

func TestJSONFile_DefaultKeyColumn(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	// With no explicit key column, the lexicographically first field is used.
	// Fields: "alpha", "beta" -> key = "alpha"
	os.WriteFile(path, []byte(`[
		{"alpha": "key1", "beta": "val1"},
		{"alpha": "key2", "beta": "val2"}
	]`), 0o644)

	jf, err := NewJSONFile(JSONFileConfig{Query: `.`})
	if err != nil {
		t.Fatal(err)
	}
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "key1"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["beta"] != "val1" {
		t.Errorf("beta = %q, want val1", result["beta"])
	}
}
