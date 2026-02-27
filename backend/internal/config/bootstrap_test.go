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
	auth, _, _, _, _, _, err := s.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("LoadServerSettings after bootstrap: %v", err)
	}
	if auth.JWTSecret == "" {
		t.Error("expected non-empty JWT secret after bootstrap")
	}
	if auth.TokenDuration != "15m" {
		t.Errorf("expected token duration 15m, got %q", auth.TokenDuration)
	}
	if auth.RefreshTokenDuration != "168h" {
		t.Errorf("expected refresh token duration 168h, got %q", auth.RefreshTokenDuration)
	}
}

func TestLoadSaveServerSettings(t *testing.T) {
	s := memory.NewStore()
	ctx := context.Background()

	t.Run("load empty returns zero value", func(t *testing.T) {
		auth, _, _, _, _, _, err := config.LoadServerSettings(ctx, s)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if auth.JWTSecret != "" {
			t.Errorf("expected empty JWT secret, got %q", auth.JWTSecret)
		}
	})

	t.Run("round trip", func(t *testing.T) {
		wantAuth := config.AuthConfig{
			JWTSecret:     "test-secret-key",
			TokenDuration: "24h",
			PasswordPolicy: config.PasswordPolicy{
				MinLength: 12,
			},
		}
		wantSched := config.SchedulerConfig{
			MaxConcurrentJobs: 8,
		}
		wantTLS := config.TLSConfig{
			TLSEnabled:  true,
			DefaultCert: "cert-id-123",
		}

		if err := config.SaveServerSettings(ctx, s, wantAuth, config.QueryConfig{}, wantSched, wantTLS, config.LookupConfig{}, false); err != nil {
			t.Fatalf("SaveServerSettings: %v", err)
		}

		gotAuth, _, gotSched, gotTLS, _, _, err := config.LoadServerSettings(ctx, s)
		if err != nil {
			t.Fatalf("LoadServerSettings: %v", err)
		}

		if gotAuth.JWTSecret != wantAuth.JWTSecret {
			t.Errorf("JWTSecret: got %q, want %q", gotAuth.JWTSecret, wantAuth.JWTSecret)
		}
		if gotAuth.TokenDuration != wantAuth.TokenDuration {
			t.Errorf("TokenDuration: got %q, want %q", gotAuth.TokenDuration, wantAuth.TokenDuration)
		}
		if gotAuth.PasswordPolicy.MinLength != wantAuth.PasswordPolicy.MinLength {
			t.Errorf("MinLength: got %d, want %d", gotAuth.PasswordPolicy.MinLength, wantAuth.PasswordPolicy.MinLength)
		}
		if gotSched.MaxConcurrentJobs != wantSched.MaxConcurrentJobs {
			t.Errorf("MaxConcurrentJobs: got %d, want %d", gotSched.MaxConcurrentJobs, wantSched.MaxConcurrentJobs)
		}
		if gotTLS.TLSEnabled != wantTLS.TLSEnabled {
			t.Errorf("TLSEnabled: got %v, want %v", gotTLS.TLSEnabled, wantTLS.TLSEnabled)
		}
		if gotTLS.DefaultCert != wantTLS.DefaultCert {
			t.Errorf("DefaultCert: got %q, want %q", gotTLS.DefaultCert, wantTLS.DefaultCert)
		}
	})

	t.Run("overwrite preserves only latest", func(t *testing.T) {
		if err := config.SaveServerSettings(ctx, s, config.AuthConfig{JWTSecret: "first"}, config.QueryConfig{}, config.SchedulerConfig{}, config.TLSConfig{}, config.LookupConfig{}, false); err != nil {
			t.Fatalf("save first: %v", err)
		}
		if err := config.SaveServerSettings(ctx, s, config.AuthConfig{JWTSecret: "second"}, config.QueryConfig{}, config.SchedulerConfig{}, config.TLSConfig{}, config.LookupConfig{}, false); err != nil {
			t.Fatalf("save second: %v", err)
		}

		gotAuth, _, _, _, _, _, err := config.LoadServerSettings(ctx, s)
		if err != nil {
			t.Fatalf("load: %v", err)
		}
		if gotAuth.JWTSecret != "second" {
			t.Errorf("got %q, want %q", gotAuth.JWTSecret, "second")
		}
	})
}
