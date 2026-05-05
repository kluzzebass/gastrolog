package system

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"gastrolog/internal/glid"
)

// DefaultConfig returns the bootstrap configuration for first-run.
// The default vault is always in-memory; file-backed vaults are only created
// when the user explicitly configures one.
func DefaultConfig() *Config {
	filterID := glid.New()
	rotationID := glid.New()
	retentionID := glid.New()
	tierID := glid.New()
	vaultID := glid.New()
	routeID := glid.New()
	ingesterID := glid.New()

	return &Config{
		Filters: []FilterConfig{
			{ID: filterID, Name: "catch-all", Expression: "*"},
		},
		RotationPolicies: []RotationPolicyConfig{
			{ID: rotationID, Name: "default", MaxAge: new("5m")},
		},
		RetentionPolicies: []RetentionPolicyConfig{
			{ID: retentionID, Name: "default", MaxChunks: new(int64(10))},
		},
		Tiers: []TierConfig{
			{
				ID:               tierID,
				Name:             "default",
				Type:             TierTypeMemory,
				VaultID:          vaultID,
				Position:         0,
				RotationPolicyID: new(rotationID),
				RetentionRules: []RetentionRule{
					{RetentionPolicyID: retentionID, Action: RetentionActionExpire},
				},
			},
		},
		Vaults: []VaultConfig{
			{
				ID:      vaultID,
				Name:    "default",
				Enabled: true,
			},
		},
		Routes: []RouteConfig{
			{
				ID:           routeID,
				Name:         "default",
				FilterID:     new(filterID),
				Destinations: []glid.GLID{vaultID},
				Distribution: DistributionFanout,
				Enabled:      true,
			},
		},
		Ingesters: []IngesterConfig{
			{
				ID:      ingesterID,
				Name:    "chatterbox",
				Type:    "chatterbox",
				Enabled: true,
				Params: map[string]string{
					"minInterval": "1s",
					"maxInterval": "5s",
					"formats":     "plain,json,kv,access,syslog",
					"instance":    "bootstrap",
				},
			},
		},
	}
}

// Bootstrap writes the default configuration to a store using individual
// CRUD operations. Call this when Load returns nil (no config exists).
func Bootstrap(ctx context.Context, store Store) error {
	cfg := DefaultConfig()

	for _, fc := range cfg.Filters {
		if err := store.PutFilter(ctx, fc); err != nil {
			return err
		}
	}
	for _, rp := range cfg.RotationPolicies {
		if err := store.PutRotationPolicy(ctx, rp); err != nil {
			return err
		}
	}
	for _, rp := range cfg.RetentionPolicies {
		if err := store.PutRetentionPolicy(ctx, rp); err != nil {
			return err
		}
	}
	for _, tier := range cfg.Tiers {
		if err := store.PutTier(ctx, tier); err != nil {
			return err
		}
	}
	// Populate the merged storage/lifecycle fields on each VaultConfig from
	// its (single) tier in cfg.Tiers, so vaults written to the store carry
	// the post-tier shape. The tier list is still seeded above for consumers
	// that haven't migrated yet (gastrolog-257l7 — vault refactor in progress).
	for _, v := range cfg.Vaults {
		v = MergeVaultFromTiers(v, cfg.Tiers)
		if err := store.PutVault(ctx, v); err != nil {
			return err
		}
	}
	for _, rt := range cfg.Routes {
		if err := store.PutRoute(ctx, rt); err != nil {
			return err
		}
	}
	for _, ing := range cfg.Ingesters {
		if err := store.PutIngester(ctx, ing); err != nil {
			return err
		}
	}

	// Generate a random JWT secret and store it as server system.
	//
	// Security note: the JWT secret is stored as base64 in the config store
	// (Raft snapshot or in-memory). It is NOT encrypted at rest. An attacker
	// with read access to the config store can forge authentication tokens.
	//
	// Mitigations: restrict filesystem permissions on the config vault
	// (e.g. 0600 / owner-only) and use full-disk encryption where possible.
	// Application-level encryption was considered but only shifts the problem
	// to key management without meaningful security gain in this deployment model.
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	authCfg := AuthConfig{
		JWTSecret:            base64.StdEncoding.EncodeToString(secret),
		TokenDuration:        "1h",
		RefreshTokenDuration: "168h", // 7 days
	}
	queryCfg := QueryConfig{
		Timeout:           "30s",
		MaxFollowDuration: "4h",
	}
	if err := store.SaveServerSettings(ctx, ServerSettings{Auth: authCfg, Query: queryCfg}); err != nil {
		return err
	}

	return nil
}

// BootstrapMinimal writes only the server config (JWT secret + token duration)
// to the store. This allows auth to work without creating any vaults, filters,
// policies, or ingesters -- suitable for first-time setup via the wizard UI.
func BootstrapMinimal(ctx context.Context, store Store) error {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	authCfg := AuthConfig{
		JWTSecret:            base64.StdEncoding.EncodeToString(secret),
		TokenDuration:        "1h",
		RefreshTokenDuration: "168h", // 7 days
	}
	queryCfg := QueryConfig{
		Timeout:           "30s",
		MaxFollowDuration: "4h",
	}
	return store.SaveServerSettings(ctx, ServerSettings{Auth: authCfg, Query: queryCfg})
}
