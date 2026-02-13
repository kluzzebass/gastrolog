package config

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

// DefaultConfig returns the bootstrap configuration for first-run.
// If dataDir is non-empty, the default store uses file-backed storage under
// <dataDir>/stores/default/. Otherwise it uses an in-memory store.
func DefaultConfig(dataDir string) *Config {
	defaultStore := StoreConfig{
		ID:        "default",
		Filter:    StringPtr("catch-all"),
		Policy:    StringPtr("default"),
		Retention: StringPtr("default"),
	}
	if dataDir != "" {
		defaultStore.Type = "file"
		defaultStore.Params = map[string]string{
			"dir": dataDir + "/stores/default",
		}
	} else {
		defaultStore.Type = "memory"
	}

	return &Config{
		Filters: map[string]FilterConfig{
			"catch-all": {Expression: "*"},
		},
		RotationPolicies: map[string]RotationPolicyConfig{
			"default": {MaxAge: StringPtr("5m")},
		},
		RetentionPolicies: map[string]RetentionPolicyConfig{
			"default": {MaxChunks: Int64Ptr(10)},
		},
		Stores:    []StoreConfig{defaultStore},
		Ingesters: []IngesterConfig{
			{
				ID:   "chatterbox",
				Type: "chatterbox",
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
// dataDir is passed through to DefaultConfig to determine store backing.
func Bootstrap(ctx context.Context, store Store, dataDir string) error {
	cfg := DefaultConfig(dataDir)

	for id, fc := range cfg.Filters {
		if err := store.PutFilter(ctx, id, fc); err != nil {
			return err
		}
	}
	for id, rp := range cfg.RotationPolicies {
		if err := store.PutRotationPolicy(ctx, id, rp); err != nil {
			return err
		}
	}
	for id, rp := range cfg.RetentionPolicies {
		if err := store.PutRetentionPolicy(ctx, id, rp); err != nil {
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
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		return fmt.Errorf("generate JWT secret: %w", err)
	}
	serverCfg := ServerConfig{
		Auth: AuthConfig{
			JWTSecret:     base64.StdEncoding.EncodeToString(secret),
			TokenDuration: "168h", // 7 days
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
