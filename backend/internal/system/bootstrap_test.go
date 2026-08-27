package system_test

import (
	"context"
	"reflect"
	"testing"

	"gastrolog/internal/system"
	"gastrolog/internal/system/memory"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()
	cfg := system.DefaultConfig()
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
		t.Errorf("expected MaxAge 5m, got %v", rp.MaxAge)
	}
	if len(cfg.Vaults) != 1 {
		t.Errorf("expected 1 vault, got %d", len(cfg.Vaults))
	}
	// The default vault should be of type "memory".
	if cfg.Vaults[0].Type != system.VaultTypeMemory {
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
	t.Parallel()
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
	if err := system.Bootstrap(ctx, s); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}

	// After bootstrap, Load returns the default system.
	sys, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if sys == nil {
		t.Fatal("expected config after bootstrap, got nil")
	}
	if len(sys.Config.RotationPolicies) != 1 {
		t.Errorf("expected 1 rotation policy, got %d", len(sys.Config.RotationPolicies))
	}
	if len(sys.Config.Vaults) != 1 {
		t.Errorf("expected 1 vault, got %d", len(sys.Config.Vaults))
	}
	if len(sys.Config.Ingesters) != 1 {
		t.Errorf("expected 1 ingester, got %d", len(sys.Config.Ingesters))
	}

	// Verify server settings were written with a JWT secret.
	ss, err := s.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("LoadServerSettings after bootstrap: %v", err)
	}
	if ss.Auth.JWTSecret == "" {
		t.Error("expected non-empty JWT secret after bootstrap")
	}
	if ss.Auth.TokenDuration != "1h" {
		t.Errorf("expected token duration 1h, got %q", ss.Auth.TokenDuration)
	}
	if ss.Auth.RefreshTokenDuration != "168h" {
		t.Errorf("expected refresh token duration 168h, got %q", ss.Auth.RefreshTokenDuration)
	}
}

func TestLoadServerSettingsEmptyReturnsZero(t *testing.T) {
	t.Parallel()
	s := memory.NewStore()
	ctx := context.Background()

	ss, err := system.LoadServerSettings(ctx, s)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ss.Auth.JWTSecret != "" {
		t.Errorf("expected empty JWT secret, got %q", ss.Auth.JWTSecret)
	}
}

func TestSaveLoadServerSettingsRoundTrip(t *testing.T) {
	t.Parallel()
	s := memory.NewStore()
	ctx := context.Background()

	want := system.ServerSettings{
		Auth: system.AuthConfig{
			JWTSecret:     "test-secret-key",
			TokenDuration: "24h",
			PasswordPolicy: system.PasswordPolicy{
				MinLength: 12,
			},
		},
		Scheduler: system.SchedulerConfig{
			MaxConcurrentJobs: 8,
		},
		TLS: system.TLSConfig{
			TLSEnabled:  true,
			DefaultCert: "cert-id-123",
		},
	}

	if err := system.SaveServerSettings(ctx, s, want); err != nil {
		t.Fatalf("SaveServerSettings: %v", err)
	}

	got, err := system.LoadServerSettings(ctx, s)
	if err != nil {
		t.Fatalf("LoadServerSettings: %v", err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("round trip mismatch:\ngot:  %+v\nwant: %+v", got, want)
	}
}

func TestSaveServerSettingsOverwritePreservesLatest(t *testing.T) {
	t.Parallel()
	s := memory.NewStore()
	ctx := context.Background()

	if err := system.SaveServerSettings(ctx, s, system.ServerSettings{Auth: system.AuthConfig{JWTSecret: "first"}}); err != nil {
		t.Fatalf("save first: %v", err)
	}
	if err := system.SaveServerSettings(ctx, s, system.ServerSettings{Auth: system.AuthConfig{JWTSecret: "second"}}); err != nil {
		t.Fatalf("save second: %v", err)
	}

	ss, err := system.LoadServerSettings(ctx, s)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if ss.Auth.JWTSecret != "second" {
		t.Errorf("got %q, want %q", ss.Auth.JWTSecret, "second")
	}
}

// A settings document written before max_result_count existed is
// indistinguishable from one storing 0, because the field is omitempty. If the
// default only applied at bootstrap, every already-running cluster would keep
// an uncapped result count — and with it an uncapped sort working set.
func TestEffectiveMaxResultCountResolvesUnsetToDefault(t *testing.T) {
	tests := []struct {
		name   string
		stored int
		want   int
	}{
		{"unset settings document", 0, system.DefaultMaxResultCount},
		{"negative is not a licence to be unbounded", -1, system.DefaultMaxResultCount},
		{"an operator's explicit cap is honoured", 250, 250},
		{"an operator may raise it", 1_000_000, 1_000_000},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := system.EffectiveMaxResultCount(tc.stored); got != tc.want {
				t.Errorf("EffectiveMaxResultCount(%d) = %d, want %d", tc.stored, got, tc.want)
			}
		})
	}
}

// Bootstrap writes the cap so a fresh install is capped without relying on the
// read-time fallback.
func TestBootstrapWritesMaxResultCount(t *testing.T) {
	for _, tc := range []struct {
		name string
		boot func(context.Context, system.Store) error
	}{
		{"full", system.Bootstrap},
		{"minimal", system.BootstrapMinimal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := memory.NewStore()
			if err := tc.boot(context.Background(), store); err != nil {
				t.Fatalf("bootstrap: %v", err)
			}
			ss, err := store.LoadServerSettings(context.Background())
			if err != nil {
				t.Fatalf("load settings: %v", err)
			}
			if ss.Query.MaxResultCount != system.DefaultMaxResultCount {
				t.Errorf("MaxResultCount = %d, want %d", ss.Query.MaxResultCount, system.DefaultMaxResultCount)
			}
		})
	}
}
