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
	"encoding/json"
	"fmt"
	"gastrolog/internal/chunk"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
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
	GetFilter(ctx context.Context, id uuid.UUID) (*FilterConfig, error)
	ListFilters(ctx context.Context) ([]FilterConfig, error)
	PutFilter(ctx context.Context, cfg FilterConfig) error
	DeleteFilter(ctx context.Context, id uuid.UUID) error

	// Rotation policies
	GetRotationPolicy(ctx context.Context, id uuid.UUID) (*RotationPolicyConfig, error)
	ListRotationPolicies(ctx context.Context) ([]RotationPolicyConfig, error)
	PutRotationPolicy(ctx context.Context, cfg RotationPolicyConfig) error
	DeleteRotationPolicy(ctx context.Context, id uuid.UUID) error

	// Retention policies
	GetRetentionPolicy(ctx context.Context, id uuid.UUID) (*RetentionPolicyConfig, error)
	ListRetentionPolicies(ctx context.Context) ([]RetentionPolicyConfig, error)
	PutRetentionPolicy(ctx context.Context, cfg RetentionPolicyConfig) error
	DeleteRetentionPolicy(ctx context.Context, id uuid.UUID) error

	// Stores
	GetStore(ctx context.Context, id uuid.UUID) (*StoreConfig, error)
	ListStores(ctx context.Context) ([]StoreConfig, error)
	PutStore(ctx context.Context, cfg StoreConfig) error
	DeleteStore(ctx context.Context, id uuid.UUID) error

	// Ingesters
	GetIngester(ctx context.Context, id uuid.UUID) (*IngesterConfig, error)
	ListIngesters(ctx context.Context) ([]IngesterConfig, error)
	PutIngester(ctx context.Context, cfg IngesterConfig) error
	DeleteIngester(ctx context.Context, id uuid.UUID) error

	// Settings (server-level key-value configuration)
	// Values are opaque JSON text; the Store does not interpret them.
	GetSetting(ctx context.Context, key string) (*string, error)
	PutSetting(ctx context.Context, key string, value string) error
	DeleteSetting(ctx context.Context, key string) error

	// Certificates (dedicated storage, not in Settings KV)
	ListCertificates(ctx context.Context) ([]CertPEM, error)
	GetCertificate(ctx context.Context, id uuid.UUID) (*CertPEM, error)
	PutCertificate(ctx context.Context, cert CertPEM) error
	DeleteCertificate(ctx context.Context, id uuid.UUID) error

	// Users
	CreateUser(ctx context.Context, user User) error
	GetUser(ctx context.Context, id uuid.UUID) (*User, error)
	GetUserByUsername(ctx context.Context, username string) (*User, error)
	ListUsers(ctx context.Context) ([]User, error)
	UpdatePassword(ctx context.Context, id uuid.UUID, passwordHash string) error
	UpdateUserRole(ctx context.Context, id uuid.UUID, role string) error
	DeleteUser(ctx context.Context, id uuid.UUID) error
	CountUsers(ctx context.Context) (int, error)
}

// Config describes the desired system shape.
// It is declarative: it defines what should exist, not how to create it.
type Config struct {
	Filters           []FilterConfig          `json:"filters,omitempty"`
	RotationPolicies  []RotationPolicyConfig  `json:"rotationPolicies,omitempty"`
	RetentionPolicies []RetentionPolicyConfig `json:"retentionPolicies,omitempty"`
	Ingesters         []IngesterConfig        `json:"ingesters,omitempty"`
	Stores            []StoreConfig           `json:"stores,omitempty"`
	Settings          map[string]string       `json:"settings,omitempty"`
	Certs             []CertPEM               `json:"certs,omitempty"`
}

// ServerConfig holds server-level configuration, organized by concern.
// It is serialized as JSON and stored under the "server" settings key.
type ServerConfig struct {
	Auth      AuthConfig      `json:"auth,omitempty"`
	Scheduler SchedulerConfig `json:"scheduler,omitempty"`
	TLS      TLSConfig       `json:"tls,omitempty"`
}

// TLSConfig holds TLS server settings.
// Certificate data is stored separately via the Store certificate CRUD methods.
type TLSConfig struct {
	// DefaultCert is the cert ID used for TLS-wrapping the Connect RPC / web endpoint.
	DefaultCert string `json:"default_cert,omitempty"`
	// TLSEnabled turns on HTTPS when a default cert exists. Falls back to HTTP if no cert.
	TLSEnabled bool `json:"tls_enabled,omitempty"`
	// HTTPToHTTPSRedirect redirects HTTP requests to HTTPS when both listeners are active.
	HTTPToHTTPSRedirect bool `json:"http_to_https_redirect,omitempty"`
}

// CertPEM holds certificate content. Either stored PEM or file paths (directory monitoring).
// When both are set, file paths take precedence and are watched for changes.
type CertPEM struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	CertPEM  string `json:"cert_pem,omitempty"`
	KeyPEM   string `json:"key_pem,omitempty"`
	CertFile string `json:"cert_file,omitempty"`
	KeyFile  string `json:"key_file,omitempty"`
}

// SchedulerConfig holds configuration for the job scheduler.
type SchedulerConfig struct {
	MaxConcurrentJobs int `json:"max_concurrent_jobs,omitempty"` // default 4
}

// AuthConfig holds configuration for user authentication.
type AuthConfig struct {
	JWTSecret         string `json:"jwt_secret,omitempty"`
	TokenDuration     string `json:"token_duration,omitempty"`      // Go duration, e.g. "168h"
	MinPasswordLength int    `json:"min_password_length,omitempty"` // default 8
}

// User represents a user account.
type User struct {
	ID           uuid.UUID `json:"id"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	Role         string    `json:"role"` // "admin" or "user"
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// FilterConfig defines a named filter expression.
// Stores reference filters by UUID to determine which messages they receive.
type FilterConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

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
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

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

// UUIDPtr returns a pointer to id.
func UUIDPtr(id uuid.UUID) *uuid.UUID { return &id }

// Int64Ptr returns a pointer to n.
func Int64Ptr(n int64) *int64 { return &n }

// RetentionPolicyConfig defines when sealed chunks should be deleted.
// Multiple conditions can be specified; a chunk is deleted if ANY condition is met.
// All fields are optional (nil = not set).
type RetentionPolicyConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

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
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// Type identifies the ingester implementation (e.g., "syslog-udp", "file").
	Type string `json:"type"`

	// Enabled controls whether the ingester is started. When false, the
	// configuration is preserved but the ingester does not run.
	Enabled bool `json:"enabled"`

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
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// Type identifies the store implementation (e.g., "file", "memory").
	Type string `json:"type"`

	// Filter references a filter by UUID.
	// Nil means no filter (store receives nothing).
	Filter *uuid.UUID `json:"filter,omitempty"`

	// Policy references a rotation policy by UUID.
	// Nil means no policy (type-specific default).
	Policy *uuid.UUID `json:"policy,omitempty"`

	// Retention references a retention policy by UUID.
	// Nil means no retention policy (chunks are kept indefinitely, or type-specific default).
	Retention *uuid.UUID `json:"retention,omitempty"`

	// Enabled indicates whether ingestion is enabled for this store.
	// When false, the store will not receive new records from the ingest pipeline.
	Enabled bool `json:"enabled,omitempty"`

	// Params contains type-specific configuration as opaque string key-value pairs.
	// Parsing and validation are the responsibility of the factory that consumes
	// the params. There is no schema enforcement at the ConfigStore level.
	Params map[string]string `json:"params,omitempty"`
}

// LoadServerConfig reads and parses the ServerConfig from the "server" settings key.
// Returns a zero-value ServerConfig if no setting exists.
func LoadServerConfig(ctx context.Context, store Store) (ServerConfig, error) {
	raw, err := store.GetSetting(ctx, "server")
	if err != nil {
		return ServerConfig{}, fmt.Errorf("get server setting: %w", err)
	}
	var sc ServerConfig
	if raw != nil {
		if err := json.Unmarshal([]byte(*raw), &sc); err != nil {
			return ServerConfig{}, fmt.Errorf("parse server config: %w", err)
		}
	}
	return sc, nil
}

// SaveServerConfig marshals and writes the ServerConfig to the "server" settings key.
func SaveServerConfig(ctx context.Context, store Store, sc ServerConfig) error {
	data, err := json.Marshal(sc)
	if err != nil {
		return fmt.Errorf("marshal server config: %w", err)
	}
	return store.PutSetting(ctx, "server", string(data))
}
