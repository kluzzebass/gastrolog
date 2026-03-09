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

	// Query: use top-level object key matching via $['{value}'].
	jf := NewJSONFile(JSONFileConfig{Query: "$['{value}']"})
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

func TestJSONFile_FilterExpression(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "192.168.1.1", "hostname": "web-1", "env": "prod"},
		{"ip": "10.0.0.1",    "hostname": "db-1",  "env": "staging"}
	]}`), 0o644)

	jf := NewJSONFile(JSONFileConfig{Query: "$.hosts[?(@.ip == '{value}')]"})
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

func TestJSONFile_ComplexFilter(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"users": [
		{"name": "Alice", "role": "admin", "active": true, "team": "platform"},
		{"name": "Bob",   "role": "user",  "active": true, "team": "data"},
		{"name": "Charlie", "role": "admin", "active": false, "team": "security"}
	]}`), 0o644)

	// Filter: active admins, extract name.
	jf := NewJSONFile(JSONFileConfig{
		Query:         "$.users[?(@.role == 'admin' && @.active == true)]",
		ResponsePaths: []string{"$.name", "$.team"},
	})
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	// Static query (no {value} placeholder) — value is ignored but must be non-empty.
	result := jf.LookupValues(context.Background(), map[string]string{"value": "any"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %q, want Alice", result["name"])
	}
	if result["team"] != "platform" {
		t.Errorf("team = %q, want platform", result["team"])
	}
}

func TestJSONFile_ResponsePaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "192.168.1.1", "meta": {"region": "us-east", "az": "1a"}, "role": "web"}
	]}`), 0o644)

	// Query matches the host, response_paths extract only from $.meta.
	jf := NewJSONFile(JSONFileConfig{
		Query:         "$.hosts[?(@.ip == '{value}')]",
		ResponsePaths: []string{"$.meta"},
	})
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	result := jf.LookupValues(context.Background(), map[string]string{"value": "192.168.1.1"})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["region"] != "us-east" {
		t.Errorf("region = %q, want us-east", result["region"])
	}
	if result["az"] != "1a" {
		t.Errorf("az = %q, want 1a", result["az"])
	}
	// "role" should NOT appear since we're extracting from $.meta only.
	if _, ok := result["role"]; ok {
		t.Error("role should not be in result when response_paths = $.meta")
	}
}

func TestJSONFile_Suffixes(t *testing.T) {
	jf := NewJSONFile(JSONFileConfig{Query: "$.hosts[?(@.ip == '{value}')]"})
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

	// Suffixes are discovered on first successful lookup.
	jf.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})

	suffixes := jf.Suffixes()
	if len(suffixes) == 0 {
		t.Fatal("expected non-empty suffixes after successful lookup")
	}
}

func TestJSONFile_CacheHit(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "10.0.0.1", "hostname": "web-1"}
	]}`), 0o644)

	jf := NewJSONFile(JSONFileConfig{Query: "$.hosts[?(@.ip == '{value}')]"})
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	r1 := jf.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})
	r2 := jf.LookupValues(context.Background(), map[string]string{"value": "10.0.0.1"})

	if r1 == nil || r2 == nil {
		t.Fatal("expected non-nil results")
	}
	if r1["hostname"] != r2["hostname"] {
		t.Error("cache should return same result")
	}
}

func TestJSONFile_HotReload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "10.0.0.1", "val": "original"}
	]}`), 0o644)

	jf := NewJSONFile(JSONFileConfig{Query: "$.hosts[?(@.ip == '{value}')]"})
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
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "10.0.0.1", "val": "reloaded"}
	]}`), 0o644)

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
	os.WriteFile(path, []byte(`{"k": "v"}`), 0o644)

	jf := NewJSONFile(JSONFileConfig{Query: "$"})
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}

	// Reload — old mmap should be released.
	os.WriteFile(path, []byte(`{"k": "v2"}`), 0o644)
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}

	// Close — final mmap should be released.
	jf.Close()

	// If mmap wasn't properly released, the process would leak file descriptors.
	// We can't easily assert this, but the test not crashing is a good sign.
}

func TestJSONFile_MultiValueLookup(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"users": [
		{"name": "Alice", "role": "admin", "team": "platform", "active": true},
		{"name": "Bob",   "role": "admin", "team": "data",     "active": true},
		{"name": "Charlie", "role": "user", "team": "platform", "active": true}
	]}`), 0o644)

	jf := NewJSONFile(JSONFileConfig{
		Query:      "$.users[?(@.role == '{role}' && @.team == '{team}')]",
		Parameters: []string{"role", "team"},
	})
	if err := jf.Load(path); err != nil {
		t.Fatal(err)
	}
	defer jf.Close()

	// Verify interface.
	params := jf.Parameters()
	if len(params) != 2 || params[0] != "role" || params[1] != "team" {
		t.Fatalf("Parameters() = %v, want [role, team]", params)
	}

	result := jf.LookupValues(context.Background(), map[string]string{
		"role": "admin",
		"team": "platform",
	})
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %q, want Alice", result["name"])
	}

	// Different param combination.
	result2 := jf.LookupValues(context.Background(), map[string]string{
		"role": "admin",
		"team": "data",
	})
	if result2 == nil {
		t.Fatal("expected non-nil result")
	}
	if result2["name"] != "Bob" {
		t.Errorf("name = %q, want Bob", result2["name"])
	}

	// No match.
	result3 := jf.LookupValues(context.Background(), map[string]string{
		"role": "admin",
		"team": "nonexistent",
	})
	if result3 != nil {
		t.Error("expected nil for non-matching values")
	}

	// Empty values.
	if jf.LookupValues(context.Background(), nil) != nil {
		t.Error("expected nil for empty values")
	}
}

func TestJSONFile_NestedValues(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "data.json")
	os.WriteFile(path, []byte(`{"hosts": [
		{"ip": "10.0.0.1", "name": "web", "tags": ["prod", "us"], "nested": {"deep": "value"}}
	]}`), 0o644)

	jf := NewJSONFile(JSONFileConfig{Query: "$.hosts[?(@.ip == '{value}')]"})
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
