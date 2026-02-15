package config_test

import (
	"context"
	"encoding/json"
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
	if len(cfg.Stores) != 1 {
		t.Errorf("expected 1 store, got %d", len(cfg.Stores))
	}
	if cfg.Stores[0].Type != "memory" {
		t.Errorf("expected store type 'memory', got %q", cfg.Stores[0].Type)
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
	if len(cfg.Stores) != 1 {
		t.Errorf("expected 1 store, got %d", len(cfg.Stores))
	}
	if len(cfg.Ingesters) != 1 {
		t.Errorf("expected 1 ingester, got %d", len(cfg.Ingesters))
	}

	// Verify server config was written with a JWT secret.
	sc, err := config.LoadServerConfig(ctx, s)
	if err != nil {
		t.Fatalf("LoadServerConfig after bootstrap: %v", err)
	}
	if sc.Auth.JWTSecret == "" {
		t.Error("expected non-empty JWT secret after bootstrap")
	}
	if sc.Auth.TokenDuration != "168h" {
		t.Errorf("expected token duration 168h, got %q", sc.Auth.TokenDuration)
	}
}

func TestLoadSaveServerConfig(t *testing.T) {
	s := memory.NewStore()
	ctx := context.Background()

	t.Run("load empty returns zero value", func(t *testing.T) {
		sc, err := config.LoadServerConfig(ctx, s)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if sc.Auth.JWTSecret != "" {
			t.Errorf("expected empty JWT secret, got %q", sc.Auth.JWTSecret)
		}
	})

	t.Run("round trip", func(t *testing.T) {
		want := config.ServerConfig{
			Auth: config.AuthConfig{
				JWTSecret:         "test-secret-key",
				TokenDuration:     "24h",
				MinPasswordLength: 12,
			},
			Scheduler: config.SchedulerConfig{
				MaxConcurrentJobs: 8,
			},
			TLS: config.TLSConfig{
				TLSEnabled: true,
				DefaultCert: "cert-id-123",
			},
		}

		if err := config.SaveServerConfig(ctx, s, want); err != nil {
			t.Fatalf("SaveServerConfig: %v", err)
		}

		got, err := config.LoadServerConfig(ctx, s)
		if err != nil {
			t.Fatalf("LoadServerConfig: %v", err)
		}

		if got.Auth.JWTSecret != want.Auth.JWTSecret {
			t.Errorf("JWTSecret: got %q, want %q", got.Auth.JWTSecret, want.Auth.JWTSecret)
		}
		if got.Auth.TokenDuration != want.Auth.TokenDuration {
			t.Errorf("TokenDuration: got %q, want %q", got.Auth.TokenDuration, want.Auth.TokenDuration)
		}
		if got.Auth.MinPasswordLength != want.Auth.MinPasswordLength {
			t.Errorf("MinPasswordLength: got %d, want %d", got.Auth.MinPasswordLength, want.Auth.MinPasswordLength)
		}
		if got.Scheduler.MaxConcurrentJobs != want.Scheduler.MaxConcurrentJobs {
			t.Errorf("MaxConcurrentJobs: got %d, want %d", got.Scheduler.MaxConcurrentJobs, want.Scheduler.MaxConcurrentJobs)
		}
		if got.TLS.TLSEnabled != want.TLS.TLSEnabled {
			t.Errorf("TLSEnabled: got %v, want %v", got.TLS.TLSEnabled, want.TLS.TLSEnabled)
		}
		if got.TLS.DefaultCert != want.TLS.DefaultCert {
			t.Errorf("DefaultCert: got %q, want %q", got.TLS.DefaultCert, want.TLS.DefaultCert)
		}
	})

	t.Run("load invalid JSON", func(t *testing.T) {
		// Write invalid JSON directly to settings.
		if err := s.PutSetting(ctx, "server", "not-valid-json"); err != nil {
			t.Fatalf("PutSetting: %v", err)
		}
		_, err := config.LoadServerConfig(ctx, s)
		if err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})

	t.Run("overwrite preserves only latest", func(t *testing.T) {
		first := config.ServerConfig{
			Auth: config.AuthConfig{JWTSecret: "first"},
		}
		second := config.ServerConfig{
			Auth: config.AuthConfig{JWTSecret: "second"},
		}

		if err := config.SaveServerConfig(ctx, s, first); err != nil {
			t.Fatalf("save first: %v", err)
		}
		if err := config.SaveServerConfig(ctx, s, second); err != nil {
			t.Fatalf("save second: %v", err)
		}

		got, err := config.LoadServerConfig(ctx, s)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if got.Auth.JWTSecret != "second" {
			t.Errorf("got %q, want %q", got.Auth.JWTSecret, "second")
		}
	})
}

func TestSaveServerConfigJSON(t *testing.T) {
	// Verify SaveServerConfig produces valid JSON.
	s := memory.NewStore()
	ctx := context.Background()

	cfg := config.ServerConfig{
		Auth: config.AuthConfig{JWTSecret: "abc"},
	}
	if err := config.SaveServerConfig(ctx, s, cfg); err != nil {
		t.Fatalf("save: %v", err)
	}

	raw, err := s.GetSetting(ctx, "server")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if raw == nil {
		t.Fatal("expected non-nil setting")
	}

	// Ensure it is valid JSON.
	if !json.Valid([]byte(*raw)) {
		t.Errorf("stored value is not valid JSON: %s", *raw)
	}
}
