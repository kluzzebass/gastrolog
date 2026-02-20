package config

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

// DefaultConfig returns the bootstrap configuration for first-run.
// The default store is always in-memory; file-backed stores are only created
// when the user explicitly configures one.
func DefaultConfig() *Config {
	filterID := uuid.Must(uuid.NewV7())
	rotationID := uuid.Must(uuid.NewV7())
	retentionID := uuid.Must(uuid.NewV7())
	storeID := uuid.Must(uuid.NewV7())
	ingesterID := uuid.Must(uuid.NewV7())

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
		Stores: []StoreConfig{
			{
				ID:        storeID,
				Name:      "default",
				Type:      "memory",
				Enabled:   true,
				Filter:    new(filterID),
				Policy:    new(rotationID),
				Retention: new(retentionID),
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
	for _, st := range cfg.Stores {
		if err := store.PutStore(ctx, st); err != nil {
			return err
		}
	}
	for _, ing := range cfg.Ingesters {
		if err := store.PutIngester(ctx, ing); err != nil {
			return err
		}
	}

	// Generate a random JWT secret and store it as server config.
	//
	// Security note: the JWT secret is stored as base64 in the config store
	// (SQLite DB or JSON file). It is NOT encrypted at rest. An attacker with
	// read access to the config store can forge authentication tokens.
	//
	// Mitigations: restrict filesystem permissions on the config store
	// (e.g. 0600 / owner-only) and use full-disk encryption where possible.
	// Application-level encryption was considered but only shifts the problem
	// to key management without meaningful security gain in this deployment model.
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	serverCfg := ServerConfig{
		Auth: AuthConfig{
			JWTSecret:            base64.StdEncoding.EncodeToString(secret),
			TokenDuration:        "15m",
			RefreshTokenDuration: "168h", // 7 days
		},
		Query: QueryConfig{
			Timeout:           "30s",
			MaxFollowDuration: "4h",
		},
	}
	serverJSON, err := json.Marshal(serverCfg)
	if err != nil {
		return fmt.Errorf("marshal server config: %w", err)
	}
	if err := store.PutSetting(ctx, "server", string(serverJSON)); err != nil {
		return err
	}

	return nil
}

// BootstrapMinimal writes only the server config (JWT secret + token duration)
// to the store. This allows auth to work without creating any stores, filters,
// policies, or ingesters — suitable for first-time setup via the wizard UI.
func BootstrapMinimal(ctx context.Context, store Store) error {
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	serverCfg := ServerConfig{
		Auth: AuthConfig{
			JWTSecret:            base64.StdEncoding.EncodeToString(secret),
			TokenDuration:        "15m",
			RefreshTokenDuration: "168h", // 7 days
		},
		Query: QueryConfig{
			Timeout:           "30s",
			MaxFollowDuration: "4h",
		},
	}
	serverJSON, err := json.Marshal(serverCfg)
	if err != nil {
		return fmt.Errorf("marshal server config: %w", err)
	}
	if err := store.PutSetting(ctx, "server", string(serverJSON)); err != nil {
		return err
	}
	return nil
}
