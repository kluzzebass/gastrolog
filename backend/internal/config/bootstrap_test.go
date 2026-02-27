package config_test

import (
	"context"
	"reflect"
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
	rp := cfg.RotationPolicies[0]
	if rp.Name != "default" {
		t.Fatalf("expected rotation policy name 'default', got %q", rp.Name)
	}
	if rp.MaxAge == nil || *rp.MaxAge != "5m" {
		t.Errorf("expected MaxAge '5m', got %v", rp.MaxAge)
	}
	if len(cfg.Vaults) != 1 {
		t.Errorf("expected 1 vault, got %d", len(cfg.Vaults))
	}
	if cfg.Vaults[0].Type != "memory" {
		t.Errorf("expected vault type 'memory', got %q", cfg.Vaults[0].Type)
	}
	if len(cfg.Ingesters) != 1 {
		t.Errorf("expected 1 ingester, got %d", len(cfg.Ingesters))
	}
	if cfg.Ingesters[0].Name != "chatterbox" {
		t.Errorf("expected ingester name 'chatterbox', got %q", cfg.Ingesters[0].Name)
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
	if len(cfg.Vaults) != 1 {
		t.Errorf("expected 1 vault, got %d", len(cfg.Vaults))
	}
	if len(cfg.Ingesters) != 1 {
		t.Errorf("expected 1 ingester, got %d", len(cfg.Ingesters))
	}

	// Verify server settings were written with a JWT secret.
	ss, err := s.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("LoadServerSettings after bootstrap: %v", err)
	}
	if ss.Auth.JWTSecret == "" {
		t.Error("expected non-empty JWT secret after bootstrap")
	}
	if ss.Auth.TokenDuration != "15m" {
		t.Errorf("expected token duration 15m, got %q", ss.Auth.TokenDuration)
	}
	if ss.Auth.RefreshTokenDuration != "168h" {
		t.Errorf("expected refresh token duration 168h, got %q", ss.Auth.RefreshTokenDuration)
	}
}

func TestLoadServerSettingsEmptyReturnsZero(t *testing.T) {
	s := memory.NewStore()
	ctx := context.Background()

	ss, err := config.LoadServerSettings(ctx, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ss.Auth.JWTSecret != "" {
		t.Errorf("expected empty JWT secret, got %q", ss.Auth.JWTSecret)
	}
}

func TestSaveLoadServerSettingsRoundTrip(t *testing.T) {
	s := memory.NewStore()
	ctx := context.Background()

	want := config.ServerSettings{
		Auth: config.AuthConfig{
			JWTSecret:     "test-secret-key",
			TokenDuration: "24h",
			PasswordPolicy: config.PasswordPolicy{
				MinLength: 12,
			},
		},
		Scheduler: config.SchedulerConfig{
			MaxConcurrentJobs: 8,
		},
		TLS: config.TLSConfig{
			TLSEnabled:  true,
			DefaultCert: "cert-id-123",
		},
	}

	if err := config.SaveServerSettings(ctx, s, want); err != nil {
		t.Fatalf("SaveServerSettings: %v", err)
	}

	got, err := config.LoadServerSettings(ctx, s)
	if err != nil {
		t.Fatalf("LoadServerSettings: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip mismatch:\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestSaveServerSettingsOverwritePreservesLatest(t *testing.T) {
	s := memory.NewStore()
	ctx := context.Background()

	if err := config.SaveServerSettings(ctx, s, config.ServerSettings{Auth: config.AuthConfig{JWTSecret: "first"}}); err != nil {
		t.Fatalf("save first: %v", err)
	}
	if err := config.SaveServerSettings(ctx, s, config.ServerSettings{Auth: config.AuthConfig{JWTSecret: "second"}}); err != nil {
		t.Fatalf("save second: %v", err)
	}

	ss, err := config.LoadServerSettings(ctx, s)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if ss.Auth.JWTSecret != "second" {
		t.Errorf("got %q, want %q", ss.Auth.JWTSecret, "second")
	}
}
