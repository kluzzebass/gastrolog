package config_test

import (
	"context"
	"testing"

	"gastrolog/internal/config"
	"gastrolog/internal/config/memory"
)

func TestDefaultConfig(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if len(cfg.RotationPolicies) != 1 {
		t.Errorf("expected 1 rotation policy, got %d", len(cfg.RotationPolicies))
	}
	rp, ok := cfg.RotationPolicies["default"]
	if !ok {
		t.Fatal("expected 'default' rotation policy")
	}
	if rp.MaxAge == nil || *rp.MaxAge != "5m" {
		t.Errorf("expected MaxAge '5m', got %v", rp.MaxAge)
	}
	if len(cfg.Stores) != 1 {
		t.Errorf("expected 1 store, got %d", len(cfg.Stores))
	}
	if cfg.Stores[0].Type != "memory" {
		t.Errorf("expected store type 'memory', got %q", cfg.Stores[0].Type)
	}
	if len(cfg.Ingesters) != 1 {
		t.Errorf("expected 1 ingester, got %d", len(cfg.Ingesters))
	}
	if cfg.Ingesters[0].ID != "chatterbox" {
		t.Errorf("expected ingester ID 'chatterbox', got %q", cfg.Ingesters[0].ID)
	}
}

func TestBootstrap(t *testing.T) {
	s := memory.NewStore()
	ctx := context.Background()

	// Before bootstrap, Load returns nil.
	cfg, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg != nil {
		t.Fatal("expected nil before bootstrap")
	}

	// Bootstrap.
	if err := config.Bootstrap(ctx, s); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	// After bootstrap, Load returns the default config.
	cfg, err = s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected config after bootstrap, got nil")
	}
	if len(cfg.RotationPolicies) != 1 {
		t.Errorf("expected 1 rotation policy, got %d", len(cfg.RotationPolicies))
	}
	if len(cfg.Stores) != 1 {
		t.Errorf("expected 1 store, got %d", len(cfg.Stores))
	}
	if len(cfg.Ingesters) != 1 {
		t.Errorf("expected 1 ingester, got %d", len(cfg.Ingesters))
	}
}
