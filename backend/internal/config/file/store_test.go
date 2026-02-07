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

func TestConformance(t *testing.T) {
	storetest.TestStore(t, func(t *testing.T) config.Store {
		return NewStore(filepath.Join(t.TempDir(), "config.json"))
	})
}

func TestStoreCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "nested", "config.json")

	s := NewStore(path)
	ctx := context.Background()

	if err := s.PutIngester(ctx, config.IngesterConfig{ID: "r1", Type: "test"}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("config file should exist: %v", err)
	}
}

func TestStoreInvalidJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	if err := os.WriteFile(path, []byte("{invalid}"), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := NewStore(path)
	_, err := s.Load(context.Background())
	if err == nil {
		t.Fatal("expected error loading invalid JSON, got nil")
	}
}

func TestStoreUnversionedFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")

	// Write a legacy unversioned config (no "version" field).
	data := `{"ingesters": [{"id": "r1", "type": "test"}]}`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatalf("write: %v", err)
	}

	s := NewStore(path)
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
	path := filepath.Join(dir, "config.json")

	s := NewStore(path)
	ctx := context.Background()

	if err := s.PutIngester(ctx, config.IngesterConfig{
		ID: "syslog1", Type: "syslog-udp", Params: map[string]string{"port": "514"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	data, err := os.ReadFile(path)
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
	path := filepath.Join(dir, "config.json")

	s1 := NewStore(path)
	ctx := context.Background()

	if err := s1.PutIngester(ctx, config.IngesterConfig{ID: "r1", Type: "test"}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// Create new store pointing at same file.
	s2 := NewStore(path)
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
