package memory

import (
	"context"
	"gastrolog/internal/glid"
	"testing"

	"gastrolog/internal/system"
	"gastrolog/internal/system/storetest"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	storetest.TestStore(t, func(t *testing.T) system.Store {
		return NewStore()
	})
}

func TestStoreIsolation(t *testing.T) {
	t.Parallel()
	s := NewStore()
	ctx := context.Background()

	ingesterID := glid.New()
	if err := s.PutIngester(ctx, system.IngesterConfig{
		ID: ingesterID, Type: "test", Params: map[string]string{"key": "value"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// Load and modify.
	got, err := s.GetIngester(ctx, ingesterID)
	if err != nil {
		t.Fatalf("GetIngester: %v", err)
	}
	modifiedID := glid.New()
	got.ID = modifiedID
	got.Params["key"] = "modified"

	// Get again should return unmodified copy.
	got2, err := s.GetIngester(ctx, ingesterID)
	if err != nil {
		t.Fatalf("GetIngester: %v", err)
	}
	if got2.ID != ingesterID {
		t.Errorf("expected ID %v, got %v", ingesterID, got2.ID)
	}
	if got2.Params["key"] != "value" {
		t.Errorf("expected Params[key] %q, got %q", "value", got2.Params["key"])
	}
}
