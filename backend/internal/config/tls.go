package config

import (
	"context"
)

// LoadTLSConfig reads TLS config from the store.
func LoadTLSConfig(ctx context.Context, store Store) (*TLSConfig, error) {
	cfg, err := store.GetTLSConfig(ctx)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return &TLSConfig{Certs: make(map[string]CertPEM)}, nil
	}
	if cfg.Certs == nil {
		cfg.Certs = make(map[string]CertPEM)
	}
	return cfg, nil
}

// SaveTLSConfig writes TLS config to the store.
func SaveTLSConfig(ctx context.Context, store Store, cfg *TLSConfig) error {
	return store.PutTLSConfig(ctx, cfg)
}
