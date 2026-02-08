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

	"github.com/go-co-op/gocron/v2"
)

// Store persists and loads system configuration with granular CRUD operations.
//
// Config describes the desired system shape. Orchestrator loads config at
// startup and instantiates components.
//
// Store is not accessed on the ingest or query hot path. Persistence must
// not block ingestion or queries.
//
// Validation: Store does not validate config semantics. It only ensures
// the data can be serialized/deserialized. Semantic validation (duplicate
// IDs, unknown types, dangling filter references) is the responsibility of
// the component that consumes the config (e.g., Orchestrator at startup).
type Store interface {
	// Load reads the full configuration. Returns nil if nothing exists (bootstrap signal).
	Load(ctx context.Context) (*Config, error)

	// Filters
	GetFilter(ctx context.Context, id string) (*FilterConfig, error)
	ListFilters(ctx context.Context) (map[string]FilterConfig, error)
	PutFilter(ctx context.Context, id string, cfg FilterConfig) error
	DeleteFilter(ctx context.Context, id string) error

	// Rotation policies
	GetRotationPolicy(ctx context.Context, id string) (*RotationPolicyConfig, error)
	ListRotationPolicies(ctx context.Context) (map[string]RotationPolicyConfig, error)
	PutRotationPolicy(ctx context.Context, id string, cfg RotationPolicyConfig) error
	DeleteRotationPolicy(ctx context.Context, id string) error

	// Retention policies
	GetRetentionPolicy(ctx context.Context, id string) (*RetentionPolicyConfig, error)
	ListRetentionPolicies(ctx context.Context) (map[string]RetentionPolicyConfig, error)
	PutRetentionPolicy(ctx context.Context, id string, cfg RetentionPolicyConfig) error
	DeleteRetentionPolicy(ctx context.Context, id string) error

	// Stores
	GetStore(ctx context.Context, id string) (*StoreConfig, error)
	ListStores(ctx context.Context) ([]StoreConfig, error)
	PutStore(ctx context.Context, cfg StoreConfig) error
	DeleteStore(ctx context.Context, id string) error

	// Ingesters
	GetIngester(ctx context.Context, id string) (*IngesterConfig, error)
	ListIngesters(ctx context.Context) ([]IngesterConfig, error)
	PutIngester(ctx context.Context, cfg IngesterConfig) error
	DeleteIngester(ctx context.Context, id string) error
}

// Config describes the desired system shape.
// It is declarative: it defines what should exist, not how to create it.
type Config struct {
	Filters           map[string]FilterConfig          `json:"filters,omitempty"`
	RotationPolicies  map[string]RotationPolicyConfig  `json:"rotationPolicies,omitempty"`
	RetentionPolicies map[string]RetentionPolicyConfig `json:"retentionPolicies,omitempty"`
	Ingesters         []IngesterConfig                 `json:"ingesters,omitempty"`
	Stores            []StoreConfig                    `json:"stores,omitempty"`
}

// FilterConfig defines a named filter expression.
// Stores reference filters by ID to determine which messages they receive.
type FilterConfig struct {
	// Expression is the filter expression string.
	// Special values:
	//   - "*": catch-all, receives all messages
	//   - "+": catch-the-rest, receives messages that matched no other filter
	//   - any other value: querylang expression matched against message attrs
	//     (e.g., "env=prod AND level=error")
	// Empty expression means the store receives nothing.
	Expression string `json:"expression"`
}

// RotationPolicyConfig defines when chunks should be rotated.
// Multiple conditions can be specified; rotation occurs when ANY condition is met.
// All fields are optional (nil = not set).
type RotationPolicyConfig struct {
	// MaxBytes rotates when chunk size exceeds this value.
	// Supports suffixes: B, KB, MB, GB (e.g., "64MB", "1GB").
	MaxBytes *string `json:"maxBytes,omitempty"`

	// MaxAge rotates when chunk age exceeds this duration.
	// Uses Go duration format (e.g., "1h", "30m", "24h").
	MaxAge *string `json:"maxAge,omitempty"`

	// MaxRecords rotates when record count exceeds this value.
	MaxRecords *int64 `json:"maxRecords,omitempty"`

	// Cron rotates on a fixed schedule using cron syntax.
	// Supports standard 5-field (minute-level) or 6-field (second-level) expressions.
	// 5-field: "0 * * * *" (every hour at minute 0)
	// 6-field: "30 0 * * * *" (every hour at second 30 of minute 0)
	// This runs as a background job, independent of the per-append threshold checks.
	Cron *string `json:"cron,omitempty"`
}

// ValidateCron checks whether the Cron field contains a valid cron expression.
// Supports both 5-field (minute-level) and 6-field (second-level) syntax.
// Returns nil if Cron is nil or valid, an error otherwise.
func (c RotationPolicyConfig) ValidateCron() error {
	if c.Cron == nil || *c.Cron == "" {
		return nil
	}
	cr := gocron.NewDefaultCron(true)
	if err := cr.IsValid(*c.Cron, time.UTC, time.Now()); err != nil {
		return fmt.Errorf("invalid cron expression: %w", err)
	}
	return nil
}

// StringPtr returns a pointer to s.
func StringPtr(s string) *string { return &s }

// Int64Ptr returns a pointer to n.
func Int64Ptr(n int64) *int64 { return &n }

// RetentionPolicyConfig defines when sealed chunks should be deleted.
// Multiple conditions can be specified; a chunk is deleted if ANY condition is met.
// All fields are optional (nil = not set).
type RetentionPolicyConfig struct {
	// MaxAge deletes sealed chunks older than this duration.
	// Uses Go duration format (e.g., "720h", "24h").
	MaxAge *string `json:"maxAge,omitempty"`

	// MaxBytes deletes oldest sealed chunks when total store size exceeds this value.
	// Supports suffixes: B, KB, MB, GB (e.g., "10GB", "500MB").
	MaxBytes *string `json:"maxBytes,omitempty"`

	// MaxChunks keeps at most this many sealed chunks, deleting the oldest.
	MaxChunks *int64 `json:"maxChunks,omitempty"`
}

// ToRetentionPolicy converts a RetentionPolicyConfig to a chunk.RetentionPolicy.
// Returns nil if no conditions are specified.
func (c RetentionPolicyConfig) ToRetentionPolicy() (chunk.RetentionPolicy, error) {
	var policies []chunk.RetentionPolicy

	if c.MaxAge != nil {
		d, err := time.ParseDuration(*c.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid maxAge: %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewTTLRetentionPolicy(d))
	}

	if c.MaxBytes != nil {
		bytes, err := ParseBytes(*c.MaxBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid maxBytes: %w", err)
		}
		policies = append(policies, chunk.NewSizeRetentionPolicy(int64(bytes)))
	}

	if c.MaxChunks != nil {
		if *c.MaxChunks <= 0 {
			return nil, fmt.Errorf("invalid maxChunks: must be positive")
		}
		policies = append(policies, chunk.NewCountRetentionPolicy(int(*c.MaxChunks)))
	}

	if len(policies) == 0 {
		return nil, nil
	}

	if len(policies) == 1 {
		return policies[0], nil
	}

	return chunk.NewCompositeRetentionPolicy(policies...), nil
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

	if c.MaxBytes != nil {
		bytes, err := ParseBytes(*c.MaxBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid maxBytes: %w", err)
		}
		policies = append(policies, chunk.NewSizePolicy(bytes))
	}

	if c.MaxAge != nil {
		d, err := time.ParseDuration(*c.MaxAge)
		if err != nil {
			return nil, fmt.Errorf("invalid maxAge: %w", err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewAgePolicy(d, nil))
	}

	if c.MaxRecords != nil {
		policies = append(policies, chunk.NewRecordCountPolicy(uint64(*c.MaxRecords)))
	}

	if len(policies) == 0 {
		return nil, nil
	}

	if len(policies) == 1 {
		return policies[0], nil
	}

	return chunk.NewCompositePolicy(policies...), nil
}

// ParseBytes parses a byte size string with optional suffix (B, KB, MB, GB).
func ParseBytes(s string) (uint64, error) {
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

	// Filter references a named filter from Config.Filters by ID.
	// Nil means no filter (store receives nothing).
	Filter *string `json:"filter,omitempty"`

	// Policy references a named rotation policy from Config.RotationPolicies.
	// Nil means no policy (type-specific default).
	Policy *string `json:"policy,omitempty"`

	// Retention references a named retention policy from Config.RetentionPolicies.
	// Nil means no retention policy (chunks are kept indefinitely, or type-specific default).
	Retention *string `json:"retention,omitempty"`

	// Params contains type-specific configuration as opaque string key-value pairs.
	// Parsing and validation are the responsibility of the factory that consumes
	// the params. There is no schema enforcement at the ConfigStore level.
	Params map[string]string `json:"params,omitempty"`
}
