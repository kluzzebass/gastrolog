package source_test

import (
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/source"
	sourcemem "gastrolog/internal/source/memory"
)

func TestResolveCreatesNewSource(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	attrs := map[string]string{"host": "server1", "app": "nginx"}
	id := reg.Resolve(attrs)

	if id == (chunk.SourceID{}) {
		t.Fatal("expected non-zero SourceID")
	}

	src, ok := reg.Get(id)
	if !ok {
		t.Fatal("Get returned false for newly created source")
	}
	if src.Attributes["host"] != "server1" {
		t.Errorf("got host=%q, want server1", src.Attributes["host"])
	}
	if src.Attributes["app"] != "nginx" {
		t.Errorf("got app=%q, want nginx", src.Attributes["app"])
	}
}

func TestResolveReturnsSameID(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	attrs := map[string]string{"host": "server1"}

	id1 := reg.Resolve(attrs)
	id2 := reg.Resolve(attrs)

	if id1 != id2 {
		t.Errorf("Resolve returned different IDs: %v vs %v", id1, id2)
	}
}

func TestResolveDifferentAttrsGetDifferentIDs(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	id1 := reg.Resolve(map[string]string{"host": "server1"})
	id2 := reg.Resolve(map[string]string{"host": "server2"})

	if id1 == id2 {
		t.Error("different attributes should produce different IDs")
	}
}

func TestResolveAttributeOrderDoesNotMatter(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	// Same attributes, different insertion order.
	attrs1 := map[string]string{"z": "last", "a": "first", "m": "middle"}
	attrs2 := map[string]string{"a": "first", "m": "middle", "z": "last"}

	id1 := reg.Resolve(attrs1)
	id2 := reg.Resolve(attrs2)

	if id1 != id2 {
		t.Errorf("same attributes in different order should produce same ID: %v vs %v", id1, id2)
	}
}

func TestResolveEmptyAttrs(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	id1 := reg.Resolve(map[string]string{})
	id2 := reg.Resolve(map[string]string{})

	if id1 != id2 {
		t.Error("empty attributes should produce same ID")
	}
	if id1 == (chunk.SourceID{}) {
		t.Error("expected non-zero ID for empty attributes")
	}
}

func TestResolveNilAttrs(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	id1 := reg.Resolve(nil)
	id2 := reg.Resolve(nil)

	if id1 != id2 {
		t.Error("nil attributes should produce same ID")
	}
}

func TestGetNotFound(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	_, ok := reg.Get(chunk.NewSourceID())
	if ok {
		t.Error("Get should return false for unknown ID")
	}
}

func TestGetReturnsCopy(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	id := reg.Resolve(map[string]string{"key": "value"})

	src1, _ := reg.Get(id)
	src1.Attributes["key"] = "modified"

	src2, _ := reg.Get(id)
	if src2.Attributes["key"] != "value" {
		t.Error("Get should return a copy, not allow mutation")
	}
}

func TestQueryMatchesAll(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	reg.Resolve(map[string]string{"host": "server1", "app": "nginx"})
	reg.Resolve(map[string]string{"host": "server2", "app": "nginx"})
	reg.Resolve(map[string]string{"host": "server3", "app": "apache"})

	// Query for all nginx sources.
	ids := reg.Query(map[string]string{"app": "nginx"})
	if len(ids) != 2 {
		t.Errorf("expected 2 matches, got %d", len(ids))
	}
}

func TestQueryMultipleFilters(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	reg.Resolve(map[string]string{"host": "server1", "app": "nginx", "env": "prod"})
	reg.Resolve(map[string]string{"host": "server2", "app": "nginx", "env": "dev"})
	reg.Resolve(map[string]string{"host": "server3", "app": "apache", "env": "prod"})

	// Query for nginx in prod.
	ids := reg.Query(map[string]string{"app": "nginx", "env": "prod"})
	if len(ids) != 1 {
		t.Errorf("expected 1 match, got %d", len(ids))
	}
}

func TestQueryNoMatches(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	reg.Resolve(map[string]string{"host": "server1"})

	ids := reg.Query(map[string]string{"host": "nonexistent"})
	if len(ids) != 0 {
		t.Errorf("expected 0 matches, got %d", len(ids))
	}
}

func TestQueryEmptyFilter(t *testing.T) {
	reg, err := source.NewRegistry(source.Config{})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	reg.Resolve(map[string]string{"host": "server1"})
	reg.Resolve(map[string]string{"host": "server2"})

	// Empty filter matches all.
	ids := reg.Query(map[string]string{})
	if len(ids) != 2 {
		t.Errorf("expected 2 matches, got %d", len(ids))
	}
}

func TestCreatedAtIsSet(t *testing.T) {
	now := time.Date(2025, 1, 15, 10, 30, 0, 0, time.UTC)
	reg, err := source.NewRegistry(source.Config{
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}
	defer reg.Close()

	id := reg.Resolve(map[string]string{"host": "server1"})
	src, _ := reg.Get(id)

	if !src.CreatedAt.Equal(now) {
		t.Errorf("CreatedAt=%v, want %v", src.CreatedAt, now)
	}
}

func TestPersistenceOnNewSource(t *testing.T) {
	store := sourcemem.NewStore()
	reg, err := source.NewRegistry(source.Config{
		Store: store,
	})
	if err != nil {
		t.Fatalf("NewRegistry: %v", err)
	}

	reg.Resolve(map[string]string{"host": "server1"})

	// Close to flush persistence queue.
	reg.Close()

	// Give async persist time to complete.
	time.Sleep(10 * time.Millisecond)

	if store.Count() != 1 {
		t.Errorf("expected 1 persisted source, got %d", store.Count())
	}
}

func TestLoadOnStartup(t *testing.T) {
	store := sourcemem.NewStore()

	// Create registry, add source, close.
	reg1, _ := source.NewRegistry(source.Config{Store: store})
	id1 := reg1.Resolve(map[string]string{"host": "server1"})
	reg1.Close()
	time.Sleep(10 * time.Millisecond)

	// Create new registry with same store.
	reg2, _ := source.NewRegistry(source.Config{Store: store})
	defer reg2.Close()

	// Should be able to get the source by ID.
	src, ok := reg2.Get(id1)
	if !ok {
		t.Fatal("source not loaded from store")
	}
	if src.Attributes["host"] != "server1" {
		t.Errorf("got host=%q, want server1", src.Attributes["host"])
	}

	// Resolve with same attrs should return same ID.
	id2 := reg2.Resolve(map[string]string{"host": "server1"})
	if id1 != id2 {
		t.Errorf("Resolve after reload returned different ID: %v vs %v", id1, id2)
	}
}
