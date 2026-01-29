package source_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/source"
)

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.bin")

	store := source.NewFileStore(path)

	// Create and save a source.
	src := &source.Source{
		ID:         chunk.NewSourceID(),
		Attributes: map[string]string{"host": "server1", "app": "nginx"},
		CreatedAt:  time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC),
	}

	if err := store.Save(src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Load from fresh store.
	store2 := source.NewFileStore(path)
	sources, err := store2.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}

	loaded := sources[0]
	if loaded.ID != src.ID {
		t.Errorf("ID mismatch: got %v, want %v", loaded.ID, src.ID)
	}
	if loaded.Attributes["host"] != "server1" {
		t.Errorf("host mismatch: got %q", loaded.Attributes["host"])
	}
	if loaded.Attributes["app"] != "nginx" {
		t.Errorf("app mismatch: got %q", loaded.Attributes["app"])
	}
	if !loaded.CreatedAt.Equal(src.CreatedAt) {
		t.Errorf("CreatedAt mismatch: got %v, want %v", loaded.CreatedAt, src.CreatedAt)
	}
}

func TestFileStoreMultipleSources(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.bin")

	store := source.NewFileStore(path)

	// Save multiple sources.
	for i := 0; i < 3; i++ {
		src := &source.Source{
			ID:         chunk.NewSourceID(),
			Attributes: map[string]string{"index": string(rune('a' + i))},
			CreatedAt:  time.Now(),
		}
		if err := store.Save(src); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}
	}

	// Load all.
	sources, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if len(sources) != 3 {
		t.Errorf("expected 3 sources, got %d", len(sources))
	}
}

func TestFileStoreUpdateSource(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.bin")

	store := source.NewFileStore(path)

	id := chunk.NewSourceID()

	// Save initial.
	src1 := &source.Source{
		ID:         id,
		Attributes: map[string]string{"version": "1"},
		CreatedAt:  time.Now(),
	}
	if err := store.Save(src1); err != nil {
		t.Fatalf("Save 1: %v", err)
	}

	// Save update (same ID).
	src2 := &source.Source{
		ID:         id,
		Attributes: map[string]string{"version": "2"},
		CreatedAt:  src1.CreatedAt,
	}
	if err := store.Save(src2); err != nil {
		t.Fatalf("Save 2: %v", err)
	}

	// Load and verify only one source with updated attributes.
	sources, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}

	if sources[0].Attributes["version"] != "2" {
		t.Errorf("expected version=2, got %q", sources[0].Attributes["version"])
	}
}

func TestFileStoreEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.bin")

	store := source.NewFileStore(path)

	// LoadAll on non-existent file.
	sources, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if len(sources) != 0 {
		t.Errorf("expected 0 sources, got %d", len(sources))
	}
}

func TestFileStoreAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.bin")

	store := source.NewFileStore(path)

	// Save a source.
	src := &source.Source{
		ID:         chunk.NewSourceID(),
		Attributes: map[string]string{"host": "server1"},
		CreatedAt:  time.Now(),
	}
	if err := store.Save(src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify no temp file left behind.
	tmpPath := path + ".tmp"
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("temp file should not exist after successful save")
	}

	// Verify main file exists.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("main file should exist after save")
	}
}

func TestFileStoreEmptyAttributes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.bin")

	store := source.NewFileStore(path)

	src := &source.Source{
		ID:         chunk.NewSourceID(),
		Attributes: map[string]string{},
		CreatedAt:  time.Now(),
	}

	if err := store.Save(src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	sources, err := store.LoadAll()
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}

	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}

	if len(sources[0].Attributes) != 0 {
		t.Errorf("expected empty attributes, got %v", sources[0].Attributes)
	}
}

func TestFileStoreCreatesDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "subdir", "nested", "sources.bin")

	store := source.NewFileStore(path)

	src := &source.Source{
		ID:         chunk.NewSourceID(),
		Attributes: map[string]string{"key": "value"},
		CreatedAt:  time.Now(),
	}

	if err := store.Save(src); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Verify file was created.
	if _, err := os.Stat(path); os.IsNotExist(err) {
		t.Error("file should exist after save")
	}
}

func TestFileStoreWithRegistry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sources.bin")

	store := source.NewFileStore(path)

	// Create registry with file store.
	reg1, _ := source.NewRegistry(source.Config{Store: store})
	id := reg1.Resolve(map[string]string{"host": "server1", "app": "nginx"})
	reg1.Close()
	time.Sleep(20 * time.Millisecond) // Allow async persist.

	// Create new registry with fresh file store (simulates restart).
	store2 := source.NewFileStore(path)
	reg2, _ := source.NewRegistry(source.Config{Store: store2})
	defer reg2.Close()

	// Verify source was persisted and reloaded.
	src, ok := reg2.Get(id)
	if !ok {
		t.Fatal("source not found after restart")
	}
	if src.Attributes["host"] != "server1" {
		t.Errorf("host=%q, want server1", src.Attributes["host"])
	}
	if src.Attributes["app"] != "nginx" {
		t.Errorf("app=%q, want nginx", src.Attributes["app"])
	}

	// Resolve same attrs should return same ID.
	id2 := reg2.Resolve(map[string]string{"host": "server1", "app": "nginx"})
	if id != id2 {
		t.Errorf("ID changed after restart: %v vs %v", id, id2)
	}
}
