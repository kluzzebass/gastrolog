package memory

import (
	"context"
	"testing"

	"gastrolog/internal/config"
)

func TestStoreLoadEmpty(t *testing.T) {
	s := NewStore()
	cfg, err := s.Load(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config, got %+v", cfg)
	}
}

func TestStoreSaveLoad(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	original := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "syslog1", Type: "syslog-udp", Params: map[string]string{"port": "514"}},
		},
		Stores: []config.StoreConfig{
			{ID: "main", Type: "file", Params: map[string]string{"dir": "/var/log/gastrolog"}},
		},
		Routes: []config.RouteConfig{
			{ReceiverID: "syslog1", StoreID: "main"},
		},
	}

	if err := s.Save(ctx, original); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded == nil {
		t.Fatal("expected config, got nil")
	}

	// Verify receivers.
	if len(loaded.Receivers) != 1 {
		t.Fatalf("expected 1 receiver, got %d", len(loaded.Receivers))
	}
	if loaded.Receivers[0].ID != "syslog1" {
		t.Errorf("receiver ID: expected %q, got %q", "syslog1", loaded.Receivers[0].ID)
	}
	if loaded.Receivers[0].Type != "syslog-udp" {
		t.Errorf("receiver Type: expected %q, got %q", "syslog-udp", loaded.Receivers[0].Type)
	}
	if loaded.Receivers[0].Params["port"] != "514" {
		t.Errorf("receiver Params[port]: expected %q, got %q", "514", loaded.Receivers[0].Params["port"])
	}

	// Verify stores.
	if len(loaded.Stores) != 1 {
		t.Fatalf("expected 1 store, got %d", len(loaded.Stores))
	}
	if loaded.Stores[0].ID != "main" {
		t.Errorf("store ID: expected %q, got %q", "main", loaded.Stores[0].ID)
	}

	// Verify routes.
	if len(loaded.Routes) != 1 {
		t.Fatalf("expected 1 route, got %d", len(loaded.Routes))
	}
	if loaded.Routes[0].ReceiverID != "syslog1" {
		t.Errorf("route ReceiverID: expected %q, got %q", "syslog1", loaded.Routes[0].ReceiverID)
	}
	if loaded.Routes[0].StoreID != "main" {
		t.Errorf("route StoreID: expected %q, got %q", "main", loaded.Routes[0].StoreID)
	}
}

func TestStoreIsolation(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	original := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "r1", Type: "test", Params: map[string]string{"key": "value"}},
		},
	}

	if err := s.Save(ctx, original); err != nil {
		t.Fatalf("save: %v", err)
	}

	// Modify original after save.
	original.Receivers[0].ID = "modified"
	original.Receivers[0].Params["key"] = "modified"

	// Load should return unmodified copy.
	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded.Receivers[0].ID != "r1" {
		t.Errorf("expected ID %q, got %q", "r1", loaded.Receivers[0].ID)
	}
	if loaded.Receivers[0].Params["key"] != "value" {
		t.Errorf("expected Params[key] %q, got %q", "value", loaded.Receivers[0].Params["key"])
	}

	// Modify loaded config.
	loaded.Receivers[0].ID = "also-modified"

	// Load again should return fresh copy.
	loaded2, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded2.Receivers[0].ID != "r1" {
		t.Errorf("expected ID %q, got %q", "r1", loaded2.Receivers[0].ID)
	}
}

func TestStoreSaveOverwrite(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	cfg1 := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "r1", Type: "t1"},
		},
	}

	cfg2 := &config.Config{
		Receivers: []config.ReceiverConfig{
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

	if len(loaded.Receivers) != 2 {
		t.Fatalf("expected 2 receivers, got %d", len(loaded.Receivers))
	}
	if loaded.Receivers[0].ID != "r2" {
		t.Errorf("expected receiver ID %q, got %q", "r2", loaded.Receivers[0].ID)
	}
}

func TestStoreEmptyConfig(t *testing.T) {
	s := NewStore()
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
	if len(loaded.Receivers) != 0 {
		t.Errorf("expected 0 receivers, got %d", len(loaded.Receivers))
	}
	if len(loaded.Stores) != 0 {
		t.Errorf("expected 0 stores, got %d", len(loaded.Stores))
	}
	if len(loaded.Routes) != 0 {
		t.Errorf("expected 0 routes, got %d", len(loaded.Routes))
	}
}

func TestStoreNilParams(t *testing.T) {
	s := NewStore()
	ctx := context.Background()

	cfg := &config.Config{
		Receivers: []config.ReceiverConfig{
			{ID: "r1", Type: "test", Params: nil},
		},
		Stores: []config.StoreConfig{
			{ID: "s1", Type: "test", Params: nil},
		},
	}

	if err := s.Save(ctx, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	if loaded.Receivers[0].Params != nil {
		t.Errorf("expected nil Params, got %v", loaded.Receivers[0].Params)
	}
	if loaded.Stores[0].Params != nil {
		t.Errorf("expected nil Params, got %v", loaded.Stores[0].Params)
	}
}
