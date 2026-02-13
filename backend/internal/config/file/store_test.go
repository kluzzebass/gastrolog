package file

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gastrolog/internal/config"
	"gastrolog/internal/config/storetest"
)

// newTestStore creates a Store with config and users paths in the given directory.
func newTestStore(dir string) *Store {
	return NewStore(
		filepath.Join(dir, "config.json"),
		filepath.Join(dir, "users.json"),
	)
}

func TestConformance(t *testing.T) {
	storetest.TestStore(t, func(t *testing.T) config.Store {
		return newTestStore(t.TempDir())
	})
}

func TestStoreCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "subdir", "nested")
	configPath := filepath.Join(dir, "config.json")

	s := NewStore(configPath, filepath.Join(dir, "users.json"))
	ctx := context.Background()

	if err := s.PutIngester(ctx, config.IngesterConfig{ID: "r1", Type: "test"}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	if _, err := os.Stat(configPath); err != nil {
		t.Fatalf("config file should exist: %v", err)
	}
}

func TestStoreInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	if err := os.WriteFile(configPath, []byte("{invalid}"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := newTestStore(dir)
	_, err := s.Load(context.Background())
	if err == nil {
		t.Fatal("expected error loading invalid JSON, got nil")
	}
}

func TestStoreUnversionedFile(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	// Write a legacy unversioned config (no "version" field).
	data := `{"ingesters": [{"id": "r1", "type": "test"}]}`
	if err := os.WriteFile(configPath, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := newTestStore(dir)
	_, err := s.Load(context.Background())
	if err == nil {
		t.Fatal("expected error for unversioned config, got nil")
	}
	if !strings.Contains(err.Error(), "unversioned") {
		t.Errorf("expected error mentioning 'unversioned', got: %v", err)
	}
}

func TestStoreJSONIsHumanReadable(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")

	s := newTestStore(dir)
	ctx := context.Background()

	if err := s.PutIngester(ctx, config.IngesterConfig{
		ID: "syslog1", Type: "syslog-udp", Params: map[string]string{"port": "514"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "\n") {
		t.Error("expected indented JSON with newlines")
	}
	if !strings.Contains(content, `"version"`) {
		t.Error("expected versioned envelope with 'version' field")
	}
}

func TestStoreReloadFromDisk(t *testing.T) {
	dir := t.TempDir()

	s1 := newTestStore(dir)
	ctx := context.Background()

	if err := s1.PutIngester(ctx, config.IngesterConfig{ID: "r1", Type: "test"}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// Create new store pointing at same files.
	s2 := newTestStore(dir)
	got, err := s2.GetIngester(ctx, "r1")
	if err != nil {
		t.Fatalf("GetIngester from new store: %v", err)
	}
	if got == nil {
		t.Fatal("expected ingester from new store, got nil")
	}
	if got.ID != "r1" {
		t.Errorf("expected ID %q, got %q", "r1", got.ID)
	}
}
