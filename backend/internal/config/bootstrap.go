package config

import "context"

// DefaultConfig returns the bootstrap configuration for first-run.
// This sets up a chatterbox ingester routing to an in-memory store
// with a 5-minute rotation policy.
func DefaultConfig() *Config {
	return &Config{
		Filters: map[string]FilterConfig{
			"catch-all": {Expression: "*"},
		},
		RotationPolicies: map[string]RotationPolicyConfig{
			"default": {MaxAge: StringPtr("5m")},
		},
		Stores: []StoreConfig{
			{
				ID:     "default",
				Type:   "memory",
				Filter: StringPtr("catch-all"),
				Policy: StringPtr("default"),
			},
		},
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
func Bootstrap(ctx context.Context, store Store) error {
	cfg := DefaultConfig()

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
	return nil
}
