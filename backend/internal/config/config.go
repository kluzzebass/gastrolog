// Package config provides configuration persistence for the system.
//
// ConfigStore persists and reloads the desired system configuration across
// restarts. This is control-plane state, not data-plane state.
//
// ConfigStore is a first-class component at the same level as ChunkManager,
// IndexManager, QueryEngine, SourceRegistry, and Orchestrator.
//
// ConfigStore does not:
//   - Inspect records
//   - Perform routing
//   - Manage lifecycle
//   - Watch for live changes (v1 is load-on-start only)
package config

import "context"

// Store persists and loads system configuration.
//
// Config describes the desired system shape. Orchestrator loads config at
// startup and instantiates components. Config changes are not hot-reloaded
// in v1.
//
// Store is not accessed on the ingest or query hot path. Persistence must
// not block ingestion or queries.
type Store interface {
	// Load reads the configuration. Returns nil config if none exists.
	Load(ctx context.Context) (*Config, error)

	// Save persists the configuration.
	Save(ctx context.Context, cfg *Config) error
}

// Config describes the desired system shape.
// It is declarative: it defines what should exist, not how to create it.
type Config struct {
	Receivers []ReceiverConfig
	Stores    []StoreConfig
	Routes    []RouteConfig
}

// ReceiverConfig describes a receiver to instantiate.
type ReceiverConfig struct {
	// ID is a unique identifier for this receiver.
	ID string

	// Type identifies the receiver implementation (e.g., "syslog-udp", "file").
	Type string

	// Params contains type-specific configuration.
	Params map[string]string
}

// StoreConfig describes a storage backend to instantiate.
type StoreConfig struct {
	// ID is a unique identifier for this store.
	ID string

	// Type identifies the store implementation (e.g., "file", "memory").
	Type string

	// Params contains type-specific configuration.
	Params map[string]string
}

// RouteConfig describes a routing rule from receiver to store.
type RouteConfig struct {
	// ReceiverID references a receiver by ID.
	ReceiverID string

	// StoreID references a store by ID.
	StoreID string
}
