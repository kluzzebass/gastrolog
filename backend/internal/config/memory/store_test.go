package memory

import (
	"context"
	"testing"

	"gastrolog/internal/config"
	"gastrolog/internal/config/storetest"
)

func TestConformance(t *testing.T) {
	storetest.TestStore(t, func(t *testing.T) config.Store {
		return NewStore()
	})
}

func TestStoreIsolation(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	if err := s.PutIngester(ctx, config.IngesterConfig{
		ID: "r1", Type: "test", Params: map[string]string{"key": "value"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// Load and modify.
	got, err := s.GetIngester(ctx, "r1")
	if err != nil {
		t.Fatalf("GetIngester: %v", err)
	}
	got.ID = "modified"
	got.Params["key"] = "modified"

	// Get again should return unmodified copy.
	got2, err := s.GetIngester(ctx, "r1")
	if err != nil {
		t.Fatalf("GetIngester: %v", err)
	}
	if got2.ID != "r1" {
		t.Errorf("expected ID %q, got %q", "r1", got2.ID)
	}
	if got2.Params["key"] != "value" {
		t.Errorf("expected Params[key] %q, got %q", "value", got2.Params["key"])
	}
}
