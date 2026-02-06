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

import (
	"context"
	"fmt"
	"gastrolog/internal/chunk"
	"strconv"
	"strings"
	"time"
)

// Store persists and loads system configuration.
//
// Config describes the desired system shape. Orchestrator loads config at
// startup and instantiates components. Config changes are not hot-reloaded
// in v1.
//
// Store is not accessed on the ingest or query hot path. Persistence must
// not block ingestion or queries.
//
// Validation: Store does not validate config semantics. It only ensures
// the data can be serialized/deserialized. Semantic validation (duplicate
// IDs, unknown types, dangling route references) is the responsibility of
// the component that consumes the config (e.g., Orchestrator at startup).
//
// Note on duplicate JSON keys: Go's encoding/json silently accepts duplicate
// keys and uses the last value. Detecting this would require a custom parser.
// This is a known limitation of the JSON format/decoder.
type Store interface {
	// Load reads the configuration. Returns nil config if none exists.
	Load(ctx context.Context) (*Config, error)

	// Save persists the configuration.
	Save(ctx context.Context, cfg *Config) error
}

// Config describes the desired system shape.
// It is declarative: it defines what should exist, not how to create it.
type Config struct {
	RotationPolicies map[string]RotationPolicyConfig `json:"rotationPolicies,omitempty"`
	Ingesters        []IngesterConfig                `json:"ingesters,omitempty"`
	Stores           []StoreConfig                   `json:"stores,omitempty"`
}

// RotationPolicyConfig defines when chunks should be rotated.
// Multiple conditions can be specified; rotation occurs when ANY condition is met.
type RotationPolicyConfig struct {
	// MaxBytes rotates when chunk size exceeds this value.
	// Supports suffixes: B, KB, MB, GB (e.g., "64MB", "1GB").
	MaxBytes string `json:"maxBytes,omitempty"`

	// MaxAge rotates when chunk age exceeds this duration.
	// Uses Go duration format (e.g., "1h", "30m", "24h").
	MaxAge string `json:"maxAge,omitempty"`

	// MaxRecords rotates when record count exceeds this value.
	MaxRecords int64 `json:"maxRecords,omitempty"`
}

// IngesterConfig describes a ingester to instantiate.
type IngesterConfig struct {
	// ID is a unique identifier for this ingester.
	ID string `json:"id"`

	// Type identifies the ingester implementation (e.g., "syslog-udp", "file").
	Type string `json:"type"`

	// Params contains type-specific configuration as opaque string key-value pairs.
	// Parsing and validation are the responsibility of the factory that consumes
	// the params. There is no schema enforcement at the ConfigStore level.
	Params map[string]string `json:"params,omitempty"`
}

// ToRotationPolicy converts a RotationPolicyConfig to a chunk.RotationPolicy.
// Returns nil if no conditions are specified.
func (c RotationPolicyConfig) ToRotationPolicy() (chunk.RotationPolicy, error) {
	var policies []chunk.RotationPolicy

	if c.MaxBytes != "" {
		bytes, err := parseBytes(c.MaxBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid maxBytes: %w", err)
		}
		policies = append(policies, chunk.NewSizePolicy(bytes))
	}

	if c.MaxAge != "" {
		d, err := time.ParseDuration(c.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid maxAge: %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewAgePolicy(d, nil))
	}

	if c.MaxRecords > 0 {
		policies = append(policies, chunk.NewRecordCountPolicy(uint64(c.MaxRecords)))
	}

	if len(policies) == 0 {
		return nil, nil
	}

	if len(policies) == 1 {
		return policies[0], nil
	}

	return chunk.NewCompositePolicy(policies...), nil
}

// parseBytes parses a byte size string with optional suffix (B, KB, MB, GB).
func parseBytes(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}

	s = strings.ToUpper(s)

	var multiplier uint64 = 1
	var numStr string

	switch {
	case strings.HasSuffix(s, "GB"):
		multiplier = 1024 * 1024 * 1024
		numStr = strings.TrimSuffix(s, "GB")
	case strings.HasSuffix(s, "MB"):
		multiplier = 1024 * 1024
		numStr = strings.TrimSuffix(s, "MB")
	case strings.HasSuffix(s, "KB"):
		multiplier = 1024
		numStr = strings.TrimSuffix(s, "KB")
	case strings.HasSuffix(s, "B"):
		numStr = strings.TrimSuffix(s, "B")
	default:
		numStr = s
	}

	numStr = strings.TrimSpace(numStr)
	n, err := strconv.ParseUint(numStr, 10, 64)
	if err != nil {
		return 0, err
	}

	return n * multiplier, nil
}

// StoreConfig describes a storage backend to instantiate.
type StoreConfig struct {
	// ID is a unique identifier for this store.
	ID string `json:"id"`

	// Type identifies the store implementation (e.g., "file", "memory").
	Type string `json:"type"`

	// Route defines which messages this store receives based on attributes.
	// Special values:
	//   - "" (empty): receives nothing (safe default for unconfigured stores)
	//   - "*": catch-all, receives all messages
	//   - "+": catch-the-rest, receives messages that matched no other route
	//   - any other value: querylang expression matched against message attrs
	//     (e.g., "env=prod AND level=error")
	// Token predicates are not allowed in routes (only attr-based filtering).
	Route string `json:"route,omitempty"`

	// Policy references a named rotation policy from Config.RotationPolicies.
	// If empty, the store uses a default policy (type-specific).
	Policy string `json:"policy,omitempty"`

	// Params contains type-specific configuration as opaque string key-value pairs.
	// Parsing and validation are the responsibility of the factory that consumes
	// the params. There is no schema enforcement at the ConfigStore level.
	Params map[string]string `json:"params,omitempty"`
}
