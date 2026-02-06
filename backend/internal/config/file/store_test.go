package file

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/config"
)

func TestStoreLoadNotExist(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	s := NewStore(path)
	cfg, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config, got %+v", cfg)
	}
}

func TestStoreSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	s := NewStore(path)
	ctx := context.Background()

	original := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: "syslog1", Type: "syslog-udp", Params: map[string]string{"port": "514"}},
			{ID: "file1", Type: "file", Params: map[string]string{"path": "/var/log/app.log"}},
		},
		Stores: []config.StoreConfig{
			{ID: "main", Type: "file", Route: "*", Params: map[string]string{"dir": "/var/log/gastrolog"}},
		},
	}

	if err := s.Save(ctx, original); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Verify file exists.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file should exist: %v", err)
	}

	// Load from same store.
	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded == nil {
		t.Fatal("expected config, got nil")
	}

	// Verify ingesters.
	if len(loaded.Ingesters) != 2 {
		t.Fatalf("expected 2 ingesters, got %d", len(loaded.Ingesters))
	}
	if loaded.Ingesters[0].ID != "syslog1" {
		t.Errorf("ingester[0] ID: expected %q, got %q", "syslog1", loaded.Ingesters[0].ID)
	}
	if loaded.Ingesters[0].Params["port"] != "514" {
		t.Errorf("ingester[0] Params[port]: expected %q, got %q", "514", loaded.Ingesters[0].Params["port"])
	}

	// Verify stores.
	if len(loaded.Stores) != 1 {
		t.Fatalf("expected 1 store, got %d", len(loaded.Stores))
	}
	if loaded.Stores[0].Route != "*" {
		t.Errorf("store Route: expected %q, got %q", "*", loaded.Stores[0].Route)
	}
}

func TestStoreReloadFromDisk(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	s1 := NewStore(path)
	ctx := context.Background()

	original := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: "r1", Type: "test"},
		},
	}

	if err := s1.Save(ctx, original); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Create new store pointing at same file.
	s2 := NewStore(path)
	loaded, err := s2.Load(ctx)
	if err != nil {
		t.Fatalf("load from new store: %v", err)
	}

	if len(loaded.Ingesters) != 1 {
		t.Fatalf("expected 1 ingester, got %d", len(loaded.Ingesters))
	}
	if loaded.Ingesters[0].ID != "r1" {
		t.Errorf("expected ID %q, got %q", "r1", loaded.Ingesters[0].ID)
	}
}

func TestStoreSaveOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	s := NewStore(path)
	ctx := context.Background()

	cfg1 := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: "r1", Type: "t1"},
		},
	}

	cfg2 := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: "r2", Type: "t2"},
			{ID: "r3", Type: "t3"},
		},
	}

	if err := s.Save(ctx, cfg1); err != nil {
		t.Fatalf("save cfg1: %v", err)
	}

	if err := s.Save(ctx, cfg2); err != nil {
		t.Fatalf("save cfg2: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if len(loaded.Ingesters) != 2 {
		t.Fatalf("expected 2 ingesters, got %d", len(loaded.Ingesters))
	}
	if loaded.Ingesters[0].ID != "r2" {
		t.Errorf("expected ingester ID %q, got %q", "r2", loaded.Ingesters[0].ID)
	}
}

func TestStoreCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "nested", "config.json")

	s := NewStore(path)
	ctx := context.Background()

	cfg := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: "r1", Type: "test"},
		},
	}

	if err := s.Save(ctx, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file should exist: %v", err)
	}
}

func TestStoreEmptyConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	s := NewStore(path)
	ctx := context.Background()

	cfg := &config.Config{}

	if err := s.Save(ctx, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded == nil {
		t.Fatal("expected empty config, got nil")
	}
}

func TestStoreInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	// Write invalid JSON.
	if err := os.WriteFile(path, []byte("{invalid}"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := NewStore(path)
	_, err := s.Load(context.Background())
	if err == nil {
		t.Fatal("expected error loading invalid JSON, got nil")
	}
}

func TestStoreJSONIsHumanReadable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	s := NewStore(path)
	ctx := context.Background()

	cfg := &config.Config{
		Ingesters: []config.IngesterConfig{
			{ID: "syslog1", Type: "syslog-udp", Params: map[string]string{"port": "514"}},
		},
	}

	if err := s.Save(ctx, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	// Check that it's indented (human-readable).
	content := string(data)
	if content[0] != '{' {
		t.Errorf("expected JSON to start with '{', got %q", content[0])
	}
	if len(content) < 10 {
		t.Errorf("JSON seems too short: %q", content)
	}
	// Indented JSON should contain newlines and spaces.
	if !contains(content, "\n") {
		t.Error("expected indented JSON with newlines")
	}
}

func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
