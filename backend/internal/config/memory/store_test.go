package memory

import (
	"context"
	"testing"

	"gastrolog/internal/config"
	"gastrolog/internal/config/storetest"

	"github.com/google/uuid"
)

func TestConformance(t *testing.T) {
	storetest.TestStore(t, func(t *testing.T) config.Store {
		return NewStore()
	})
}

func TestStoreIsolation(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	ingesterID := uuid.Must(uuid.NewV7())
	if err := s.PutIngester(ctx, config.IngesterConfig{
		ID: ingesterID, Type: "test", Params: map[string]string{"key": "value"},
	}); err != nil {
		t.Fatalf("PutIngester: %v", err)
	}

	// Load and modify.
	got, err := s.GetIngester(ctx, ingesterID)
	if err != nil {
		t.Fatalf("GetIngester: %v", err)
	}
	modifiedID := uuid.Must(uuid.NewV7())
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
