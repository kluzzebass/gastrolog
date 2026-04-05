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
//   - Perform filtering
//   - Manage lifecycle
//   - Watch for live changes (v1 is load-on-start only)
package config

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/chunk"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// Store interface
// ---------------------------------------------------------------------------

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

	// Vaults
	GetVault(ctx context.Context, id uuid.UUID) (*VaultConfig, error)
	ListVaults(ctx context.Context) ([]VaultConfig, error)
	PutVault(ctx context.Context, cfg VaultConfig) error
	DeleteVault(ctx context.Context, id uuid.UUID, deleteData bool) error

	// Ingesters
	GetIngester(ctx context.Context, id uuid.UUID) (*IngesterConfig, error)
	ListIngesters(ctx context.Context) ([]IngesterConfig, error)
	PutIngester(ctx context.Context, cfg IngesterConfig) error
	DeleteIngester(ctx context.Context, id uuid.UUID) error

	// Routes
	GetRoute(ctx context.Context, id uuid.UUID) (*RouteConfig, error)
	ListRoutes(ctx context.Context) ([]RouteConfig, error)
	PutRoute(ctx context.Context, cfg RouteConfig) error
	DeleteRoute(ctx context.Context, id uuid.UUID) error

	// Managed files
	GetManagedFile(ctx context.Context, id uuid.UUID) (*ManagedFileConfig, error)
	ListManagedFiles(ctx context.Context) ([]ManagedFileConfig, error)
	PutManagedFile(ctx context.Context, cfg ManagedFileConfig) error
	DeleteManagedFile(ctx context.Context, id uuid.UUID) error

	// Server settings — typed access to Auth, Query, Scheduler, TLS, Lookup, SetupWizardDismissed.
	LoadServerSettings(ctx context.Context) (ServerSettings, error)
	SaveServerSettings(ctx context.Context, ss ServerSettings) error

	// Nodes (cluster node identity)
	GetNode(ctx context.Context, id uuid.UUID) (*NodeConfig, error)
	ListNodes(ctx context.Context) ([]NodeConfig, error)
	PutNode(ctx context.Context, node NodeConfig) error
	DeleteNode(ctx context.Context, id uuid.UUID) error

	// Cluster TLS (mTLS material for cluster port).
	// Read via Load() → Config.ClusterTLS; PutClusterTLS is the Raft write path.
	PutClusterTLS(ctx context.Context, tls ClusterTLS) error

	// Certificates
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
	UpdateUsername(ctx context.Context, id uuid.UUID, username string) error
	DeleteUser(ctx context.Context, id uuid.UUID) error
	InvalidateTokens(ctx context.Context, id uuid.UUID, at time.Time) error
	CountUsers(ctx context.Context) (int, error)
	GetUserPreferences(ctx context.Context, id uuid.UUID) (*string, error)
	PutUserPreferences(ctx context.Context, id uuid.UUID, prefs string) error

	// Refresh tokens
	CreateRefreshToken(ctx context.Context, token RefreshToken) error
	GetRefreshTokenByHash(ctx context.Context, tokenHash string) (*RefreshToken, error)
	ListRefreshTokens(ctx context.Context) ([]RefreshToken, error)
	DeleteRefreshToken(ctx context.Context, id uuid.UUID) error
	DeleteUserRefreshTokens(ctx context.Context, userID uuid.UUID) error

	// Cloud services (cluster-wide)
	GetCloudService(ctx context.Context, id uuid.UUID) (*CloudService, error)
	ListCloudServices(ctx context.Context) ([]CloudService, error)
	PutCloudService(ctx context.Context, svc CloudService) error
	DeleteCloudService(ctx context.Context, id uuid.UUID) error

	// Tiers
	GetTier(ctx context.Context, id uuid.UUID) (*TierConfig, error)
	ListTiers(ctx context.Context) ([]TierConfig, error)
	PutTier(ctx context.Context, tier TierConfig) error
	DeleteTier(ctx context.Context, id uuid.UUID, drain bool) error

	// Node storage (per-node)
	GetNodeStorageConfig(ctx context.Context, nodeID string) (*NodeStorageConfig, error)
	ListNodeStorageConfigs(ctx context.Context) ([]NodeStorageConfig, error)
	SetNodeStorageConfig(ctx context.Context, cfg NodeStorageConfig) error
}

// LoadServerSettings reads the server-level settings from the store.
// Returns zero values if no settings exist.
func LoadServerSettings(ctx context.Context, store Store) (ServerSettings, error) {
	return store.LoadServerSettings(ctx)
}

// SaveServerSettings persists the server-level settings to the store.
func SaveServerSettings(ctx context.Context, store Store, ss ServerSettings) error {
	return store.SaveServerSettings(ctx, ss)
}

// ServerSettings groups the server-level settings that are loaded/saved
// atomically via LoadServerSettings / SaveServerSettings.
type ServerSettings struct {
	Auth                 AuthConfig      `json:"auth,omitzero"`
	Query                QueryConfig     `json:"query,omitzero"`
	Scheduler            SchedulerConfig `json:"scheduler,omitzero"`
	TLS                  TLSConfig       `json:"tls,omitzero"`
	Lookup               LookupConfig    `json:"lookup,omitzero"`
	Cluster              ClusterConfig   `json:"cluster,omitzero"`
	MaxMind              MaxMindConfig   `json:"maxmind,omitzero"`
	SetupWizardDismissed bool            `json:"setup_wizard_dismissed,omitempty"`
}

// ClusterConfig holds cluster-wide settings.
type ClusterConfig struct {
	BroadcastInterval string `json:"broadcast_interval,omitempty"` // Go duration string, e.g. "5s"
}

// ---------------------------------------------------------------------------
// Config — top-level configuration tree
// ---------------------------------------------------------------------------

// Config describes the desired system shape.
// It is declarative: it defines what should exist, not how to create it.
//
// Server-level settings (Auth, Query, Scheduler, TLS, Lookup, SetupWizardDismissed)
// live directly on Config rather than in a separate wrapper.
// The Store interface provides typed Load/SaveServerSettings methods for
// persisting these fields independently of entity CRUD.
type Config struct {
	// Entity collections.
	Filters           []FilterConfig          `json:"filters,omitempty"`
	RotationPolicies  []RotationPolicyConfig  `json:"rotationPolicies,omitempty"`
	RetentionPolicies []RetentionPolicyConfig `json:"retentionPolicies,omitempty"`
	Ingesters         []IngesterConfig        `json:"ingesters,omitempty"`
	Vaults            []VaultConfig           `json:"vaults,omitempty"`
	Routes            []RouteConfig           `json:"routes,omitempty"`
	Certs             []CertPEM               `json:"certs,omitempty"`
	Nodes             []NodeConfig            `json:"nodes,omitempty"`
	ManagedFiles       []ManagedFileConfig      `json:"managedFiles,omitempty"`
	CloudServices      []CloudService           `json:"cloudServices,omitempty"`
	NodeStorageConfigs []NodeStorageConfig      `json:"nodeStorageConfigs,omitempty"`
	Tiers              []TierConfig             `json:"tiers,omitempty"`

	// Server-level settings.
	Auth                 AuthConfig      `json:"auth,omitzero"`
	Query                QueryConfig     `json:"query,omitzero"`
	Scheduler            SchedulerConfig `json:"scheduler,omitzero"`
	TLS                  TLSConfig       `json:"tls,omitzero"`
	Lookup               LookupConfig    `json:"lookup,omitzero"`
	Cluster              ClusterConfig   `json:"cluster,omitzero"`
	MaxMind              MaxMindConfig   `json:"maxmind,omitzero"`
	SetupWizardDismissed bool            `json:"setup_wizard_dismissed,omitempty"`

	// Cluster TLS material (mTLS certs for cluster gRPC port).
	// Nil when running in single-node mode or before cluster-init.
	ClusterTLS *ClusterTLS `json:"cluster_tls,omitempty"`
}

// ---------------------------------------------------------------------------
// Server-level settings types
// ---------------------------------------------------------------------------

// AuthConfig holds configuration for user authentication.
type AuthConfig struct {
	JWTSecret            string         `json:"jwt_secret,omitempty"` //nolint:gosec // G117: config field, not a hardcoded credential
	TokenDuration        string         `json:"token_duration,omitempty"`          // Go duration, e.g. "168h"
	RefreshTokenDuration string         `json:"refresh_token_duration,omitempty"` // Go duration, e.g. "168h"
	PasswordPolicy       PasswordPolicy `json:"password_policy,omitzero"`
}

// PasswordPolicy holds password complexity rules.
type PasswordPolicy struct {
	MinLength             int  `json:"min_password_length,omitempty"`     // default 8
	RequireMixedCase      bool `json:"require_mixed_case,omitempty"`      // require upper and lowercase
	RequireDigit          bool `json:"require_digit,omitempty"`           // require at least one digit
	RequireSpecial        bool `json:"require_special,omitempty"`         // require at least one special char
	MaxConsecutiveRepeats int  `json:"max_consecutive_repeats,omitempty"` // 0 = no limit
	ForbidAnimalNoise     bool `json:"forbid_animal_noise,omitempty"`     // forbid animal noises as passwords
}

// QueryConfig holds configuration for the query engine.
type QueryConfig struct {
	// Timeout is the maximum duration for a single query (Search, Histogram, GetContext).
	// Uses Go duration format (e.g., "30s", "1m"). Empty disables the timeout.
	Timeout string `json:"timeout,omitempty"`

	// MaxFollowDuration is the maximum lifetime for a Follow stream.
	// Uses Go duration format (e.g., "4h"). Empty disables the limit.
	MaxFollowDuration string `json:"max_follow_duration,omitempty"`

	// MaxResultCount caps the number of records a single Search request can return.
	// 0 means unlimited (no cap). Default: 10000.
	MaxResultCount int `json:"max_result_count,omitempty"`
}

// SchedulerConfig holds configuration for the job scheduler.
type SchedulerConfig struct {
	MaxConcurrentJobs int `json:"max_concurrent_jobs,omitempty"` // default 4
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
	// HTTPSPort is the port for the HTTPS listener. Empty means HTTP port + 1.
	HTTPSPort string `json:"https_port,omitempty"`
}

// LookupConfig holds configuration for lookup tables.
type LookupConfig struct {
	HTTPLookups     []HTTPLookupConfig     `json:"http_lookups,omitempty"`
	JSONFileLookups []JSONFileLookupConfig `json:"json_file_lookups,omitempty"`
	MMDBLookups     []MMDBLookupConfig     `json:"mmdb_lookups,omitempty"`
	CSVLookups      []CSVLookupConfig      `json:"csv_lookups,omitempty"`
}

// MMDBLookupConfig defines a named MMDB-backed lookup table (GeoIP City or ASN).
type MMDBLookupConfig struct {
	Name   string `json:"name"`              // registry name (e.g. "geoip", "asn")
	DBType string `json:"db_type"`           // "city" or "asn"
	FileID string `json:"file_id,omitempty"` // managed file ID; empty = use auto-downloaded
}

// HTTPLookupParam defines a named parameter for URL template substitution.
type HTTPLookupParam struct {
	Name        string `json:"name"`                  // URL template placeholder, e.g. "lat"
	Description string `json:"description,omitempty"` // human-readable description
}

// HTTPLookupConfig defines an HTTP API lookup table that enriches records
// by calling an external HTTP endpoint.
type HTTPLookupConfig struct {
	Name          string            `json:"name"`                     // registry name (e.g. "users")
	URLTemplate   string            `json:"url_template"`             // e.g. "http://api/weather?lat={lat}&lon={lon}"
	Headers       map[string]string `json:"headers,omitempty"`        // optional auth headers
	ResponsePaths []string          `json:"response_paths,omitempty"` // JSONPath expressions, e.g. ["$.data.user"]
	Parameters    []HTTPLookupParam `json:"parameters,omitempty"`     // ordered params for URL template
	Timeout       string            `json:"timeout,omitempty"`        // Go duration string, optional
	CacheTTL      string            `json:"cache_ttl,omitempty"`      // Go duration string, optional
	CacheSize     int               `json:"cache_size,omitempty"`     // optional, default 10000
}

// JSONFileLookupConfig defines a JSON file-backed lookup table.
type JSONFileLookupConfig struct {
	Name          string            `json:"name"`                      // registry name (e.g. "hosts")
	FileID        string            `json:"file_id"`                   // managed file ID (UUID)
	Query         string            `json:"query"`                     // JSONPath query template with {name} placeholders
	ResponsePaths []string          `json:"response_paths,omitempty"`  // JSONPath expressions to extract from results
	Parameters    []HTTPLookupParam `json:"parameters,omitempty"`      // ordered params for query template placeholders
}

// CSVLookupConfig defines a CSV file-backed lookup table.
type CSVLookupConfig struct {
	Name         string   `json:"name"`                    // registry name (e.g. "assets")
	FileID       string   `json:"file_id"`                 // managed file ID (UUID)
	KeyColumn    string   `json:"key_column,omitempty"`    // column header for lookup key; empty = first column
	ValueColumns []string `json:"value_columns,omitempty"` // columns to include in output; empty = all non-key
}

// MaxMindConfig holds credentials and state for automatic MaxMind database downloading.
type MaxMindConfig struct {
	AutoDownload bool      `json:"auto_download,omitempty"`
	AccountID    string    `json:"account_id,omitempty"`
	LicenseKey   string    `json:"license_key,omitempty"`
	LastUpdate   time.Time `json:"last_update,omitzero"`
}

// ---------------------------------------------------------------------------
// Entity config types
// ---------------------------------------------------------------------------

// FilterConfig defines a named filter expression.
// Vaults reference filters by UUID to determine which messages they receive.
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
	// Empty expression means the vault receives nothing.
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
			return nil, errors.New("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewAgePolicy(d, nil))
	}

	if c.MaxRecords != nil {
		policies = append(policies, chunk.NewRecordCountPolicy(uint64(*c.MaxRecords))) //nolint:gosec // G115: maxRecords is a positive config value
	}

	if len(policies) == 0 {
		return nil, nil
	}

	if len(policies) == 1 {
		return policies[0], nil
	}

	return chunk.NewCompositePolicy(policies...), nil
}

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

	// MaxBytes deletes oldest sealed chunks when total vault size exceeds this value.
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
			return nil, errors.New("invalid maxAge: must be positive")
		}
		policies = append(policies, chunk.NewTTLRetentionPolicy(d))
	}

	if c.MaxBytes != nil {
		bytes, err := ParseBytes(*c.MaxBytes)
		if err != nil {
			return nil, fmt.Errorf("invalid maxBytes: %w", err)
		}
		policies = append(policies, chunk.NewSizeRetentionPolicy(int64(bytes))) //nolint:gosec // G115: parsed byte count is always reasonable
	}

	if c.MaxChunks != nil {
		if *c.MaxChunks <= 0 {
			return nil, errors.New("invalid maxChunks: must be positive")
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

// RetentionAction describes what happens when a retention policy matches chunks.
type RetentionAction string

const (
	// RetentionActionExpire deletes matching chunks (the default behavior).
	RetentionActionExpire RetentionAction = "expire"
	// RetentionActionEject streams matching chunks' records through named routes.
	RetentionActionEject RetentionAction = "eject"
	// RetentionActionTransition streams matching chunks' records to the next tier in the vault's chain.
	RetentionActionTransition RetentionAction = "transition"
)

// RetentionRule pairs a retention policy with an action.
type RetentionRule struct {
	RetentionPolicyID uuid.UUID       `json:"retentionPolicyId"`
	Action            RetentionAction `json:"action"`
	EjectRouteIDs     []uuid.UUID     `json:"ejectRouteIds,omitempty"` // target routes, only for eject
}

// VaultConfig describes a storage backend to instantiate.
type VaultConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// Enabled indicates whether ingestion is enabled for this vault.
	// When false, the vault will not receive new records from the ingest pipeline.
	Enabled bool `json:"enabled,omitempty"`
}

// VaultTierIDs returns the ordered tier IDs for a vault by filtering tiers
// with matching VaultID and sorting by Position. This replaces the old
// VaultConfig.TierIDs field — tier ownership now lives on TierConfig.
func VaultTierIDs(tiers []TierConfig, vaultID uuid.UUID) []uuid.UUID {
	type entry struct {
		id  uuid.UUID
		pos uint32
	}
	var matched []entry
	for _, t := range tiers {
		if t.VaultID == vaultID {
			matched = append(matched, entry{t.ID, t.Position})
		}
	}
	slices.SortFunc(matched, func(a, b entry) int {
		return cmp.Compare(a.pos, b.pos)
	})
	ids := make([]uuid.UUID, len(matched))
	for i, e := range matched {
		ids[i] = e.id
	}
	return ids
}

// VaultTiers returns the ordered tier configs for a vault.
func VaultTiers(tiers []TierConfig, vaultID uuid.UUID) []TierConfig {
	var matched []TierConfig
	for _, t := range tiers {
		if t.VaultID == vaultID {
			matched = append(matched, t)
		}
	}
	slices.SortFunc(matched, func(a, b TierConfig) int {
		return cmp.Compare(a.Position, b.Position)
	})
	return matched
}

// DistributionMode controls how messages are distributed across route destinations.
type DistributionMode string

const (
	// DistributionFanout sends every message to all destinations.
	DistributionFanout DistributionMode = "fanout"
	// DistributionRoundRobin rotates messages across destinations one at a time.
	DistributionRoundRobin DistributionMode = "round-robin"
	// DistributionFailover sends to the first healthy destination only.
	DistributionFailover DistributionMode = "failover"
)

// RouteConfig defines a named routing rule that connects a filter to one or more
// destination vaults. Routes decouple log routing from storage, enabling
// multi-destination routing with distribution modes.
type RouteConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID uuid.UUID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// FilterID references a FilterConfig by UUID.
	// Nil means no filter (route receives nothing).
	FilterID *uuid.UUID `json:"filterId,omitempty"`

	// Destinations lists the vault IDs that this route sends messages to.
	Destinations []uuid.UUID `json:"destinations"`

	// Distribution controls how messages are distributed to destinations.
	// "fanout" (default): send to all destinations.
	// "round-robin": send to one destination at a time, rotating.
	Distribution DistributionMode `json:"distribution,omitempty"`

	// Enabled controls whether this route is active.
	Enabled bool `json:"enabled,omitempty"`

	// EjectOnly marks this route as excluded from live ingestion.
	// When false (default), the route participates in the FilterSet.
	// When true, the route is excluded from live ingestion and can only
	// be used as an eject target in retention rules.
	EjectOnly bool `json:"ejectOnly,omitempty"`
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
	// the params. There is no schema enforcement at the ConfigVault level.
	Params map[string]string `json:"params,omitempty"`

	// NodeID is the raft server ID of the node that owns this ingester.
	// Empty means unscoped (legacy/migration compatibility).
	NodeID string `json:"nodeId,omitempty"`
}

// CertPEM holds certificate content. Either stored PEM or file paths (directory monitoring).
// When both are set, file paths take precedence and are watched for changes.
type CertPEM struct {
	ID       uuid.UUID `json:"id"`
	Name     string    `json:"name"`
	CertPEM  string    `json:"cert_pem,omitempty"`
	KeyPEM   string    `json:"key_pem,omitempty"`
	CertFile string    `json:"cert_file,omitempty"`
	KeyFile  string    `json:"key_file,omitempty"`
}

// NodeConfig represents a cluster node configuration with its human-readable name.
type NodeConfig struct {
	ID   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

// FileStorage defines a local file storage on a node.
type FileStorage struct {
	ID                uuid.UUID `json:"id"`
	StorageClass      uint32    `json:"storageClass"`
	Name              string    `json:"name"`
	Path              string    `json:"path,omitempty"`
	MemoryBudgetBytes uint64    `json:"memoryBudgetBytes,omitempty"`
}

// NodeStorageConfig defines the file storages for a specific cluster node.
type NodeStorageConfig struct {
	NodeID string        `json:"nodeId"`
	FileStorages []FileStorage `json:"fileStorages"`
}

// CloudService defines a cluster-wide cloud storage endpoint.
// CloudStorageTransition defines a single step in an archival lifecycle chain.
type CloudStorageTransition struct {
	After        string `json:"after"`        // duration string (e.g. "30s", "7d", "2w", "360d")
	StorageClass string `json:"storageClass"` // empty = delete (expiry)
}

type CloudService struct {
	ID               uuid.UUID `json:"id"`
	Name             string    `json:"name"`
	Provider         string    `json:"provider"`
	Bucket           string    `json:"bucket"`
	Region           string    `json:"region,omitempty"`
	Endpoint         string    `json:"endpoint,omitempty"`
	AccessKey        string    `json:"accessKey,omitempty"` //nolint:gosec // G117: config field, not a hardcoded credential
	SecretKey        string    `json:"secretKey,omitempty"` //nolint:gosec // G117: config field, not a hardcoded credential
	Container        string    `json:"container,omitempty"`
	ConnectionString string    `json:"connectionString,omitempty"`
	CredentialsJSON  string    `json:"credentialsJson,omitempty"`
	StorageClass     uint32    `json:"storageClass,omitempty"`

	// Archival lifecycle.
	ArchivalMode      string                   `json:"archivalMode,omitempty"`      // "none" or "active"
	Transitions       []CloudStorageTransition  `json:"transitions,omitempty"`       // ordered by After duration
	RestoreTier       string                   `json:"restoreTier,omitempty"`       // default restore speed
	RestoreDays       uint32                   `json:"restoreDays,omitempty"`       // S3 restore window
	SuspectGraceDays  uint32                   `json:"suspectGraceDays,omitempty"`  // default 7
	ReconcileSchedule string                   `json:"reconcileSchedule,omitempty"` // default "0 3 * * *"
}

// TierType identifies the storage medium for a tier.
type TierType string

const (
	TierTypeMemory TierType = "memory"
	TierTypeFile   TierType = "file"
	TierTypeCloud  TierType = "cloud"
	TierTypeJSONL  TierType = "jsonl"
)

// TierConfig defines a storage tier owned by exactly one vault. Tiers are
// ordered within a vault by their Position field (0 = hottest / first).
type TierConfig struct {
	ID                uuid.UUID       `json:"id"`
	Name              string          `json:"name"`
	Type              TierType        `json:"type"`
	VaultID           uuid.UUID       `json:"vaultId"`             // owning vault
	Position          uint32          `json:"position"`            // 0-based order in vault's tier chain
	RotationPolicyID  *uuid.UUID      `json:"rotationPolicyId,omitempty"`
	RetentionRules    []RetentionRule `json:"retentionRules,omitempty"`
	MemoryBudgetBytes uint64          `json:"memoryBudgetBytes,omitempty"`
	StorageClass      uint32          `json:"storageClass,omitempty"`
	CloudServiceID    *uuid.UUID      `json:"cloudServiceId,omitempty"`
	ActiveChunkClass  uint32          `json:"activeChunkClass,omitempty"`
	CacheClass        uint32          `json:"cacheClass,omitempty"`
	Path              string          `json:"path,omitempty"`              // direct path for JSONL sinks
	ReplicationFactor uint32          `json:"replicationFactor,omitempty"` // desired RF (1 = no replication)
	Placements        []TierPlacement `json:"placements,omitempty"`        // system-managed: storage assignments
	CacheEviction string `json:"cacheEviction,omitempty"` // "lru" (default) or "ttl"
	CacheBudget   string `json:"cacheBudget,omitempty"`   // max cache size (e.g. "1GB", "500MB", default: "1GiB")
	CacheTTL      string `json:"cacheTtl,omitempty"`      // duration for TTL mode (e.g. "1h", "7d")
}

// TierPlacement assigns one replica of a tier to a specific file storage.
// The node is derived from the file storage's NodeStorageConfig.
type TierPlacement struct {
	StorageID  string `json:"storageId"`
	Leader bool   `json:"leader"`
}

// LeaderStorageID returns the storage ID of the leader placement, or empty if unplaced.
func (t TierConfig) LeaderStorageID() string {
	for _, p := range t.Placements {
		if p.Leader {
			return p.StorageID
		}
	}
	return ""
}

// FollowerStorageIDs returns the storage IDs of all follower placements.
func (t TierConfig) FollowerStorageIDs() []string {
	var ids []string
	for _, p := range t.Placements {
		if !p.Leader {
			ids = append(ids, p.StorageID)
		}
	}
	return ids
}

// StorageIDs returns all placed storage IDs (leader first, then followers).
func (t TierConfig) StorageIDs() []string {
	var ids []string
	for _, p := range t.Placements {
		if p.Leader {
			ids = append([]string{p.StorageID}, ids...)
		} else {
			ids = append(ids, p.StorageID)
		}
	}
	return ids
}

// SyntheticStoragePrefix is the prefix for synthetic storage IDs used when a node has
// no file storages (e.g. memory tiers). Format: "node:<nodeID>".
const SyntheticStoragePrefix = "node:"

// SyntheticStorageID returns a synthetic storage ID for a node without file storages.
func SyntheticStorageID(nodeID string) string { return SyntheticStoragePrefix + nodeID }

// NodeIDForStorage resolves a storage ID to its node ID using the provided storage configs.
// Handles synthetic storage IDs of the form "node:<nodeID>" for nodes without file storages.
func NodeIDForStorage(storageID string, nscs []NodeStorageConfig) string {
	// Check synthetic storage IDs first (used for memory tiers on nodes without file storages).
	if strings.HasPrefix(storageID, SyntheticStoragePrefix) {
		return storageID[len(SyntheticStoragePrefix):]
	}
	for _, nsc := range nscs {
		for _, fs := range nsc.FileStorages {
			if fs.ID.String() == storageID {
				return nsc.NodeID
			}
		}
	}
	return ""
}

// StorageIDForNode returns the best storage ID on a given node for a tier.
// For file/cloud tiers, matches the required storage class.
// Returns a synthetic storage ID for memory tiers on nodes without matching file storages.
func StorageIDForNode(nodeID string, tier TierConfig, nscs []NodeStorageConfig) string {
	idx := slices.IndexFunc(nscs, func(n NodeStorageConfig) bool { return n.NodeID == nodeID })
	if idx < 0 {
		// Node has no storage config — use synthetic storage ID.
		return SyntheticStorageID(nodeID)
	}

	nsc := nscs[idx]
	var requiredClass uint32
	switch tier.Type {
	case TierTypeFile:
		requiredClass = tier.StorageClass
	case TierTypeCloud:
		requiredClass = tier.ActiveChunkClass
	case TierTypeMemory, TierTypeJSONL:
		// No storage class — pick any storage, or synthetic if none.
		if len(nsc.FileStorages) > 0 {
			return nsc.FileStorages[0].ID.String()
		}
		return SyntheticStorageID(nodeID)
	}

	for _, fs := range nsc.FileStorages {
		if fs.StorageClass == requiredClass {
			return fs.ID.String()
		}
	}
	// Fallback for follower replicas on nodes without exact class match.
	if len(nsc.FileStorages) > 0 {
		return nsc.FileStorages[0].ID.String()
	}
	return SyntheticStorageID(nodeID)
}

// LeaderNodeID derives the leader node from placements + storage configs.
func (t TierConfig) LeaderNodeID(nscs []NodeStorageConfig) string {
	storageID := t.LeaderStorageID()
	if storageID == "" {
		return ""
	}
	return NodeIDForStorage(storageID, nscs)
}

// FollowerNodeIDs derives unique follower node IDs from placements + storage configs.
// Multiple same-node placements are deduplicated. Use FollowerTargets for
// storage-level granularity.
func (t TierConfig) FollowerNodeIDs(nscs []NodeStorageConfig) []string {
	var nodeIDs []string
	seen := make(map[string]bool)
	for _, storageID := range t.FollowerStorageIDs() {
		nid := NodeIDForStorage(storageID, nscs)
		if nid != "" && !seen[nid] {
			seen[nid] = true
			nodeIDs = append(nodeIDs, nid)
		}
	}
	return nodeIDs
}

// ReplicationTarget identifies a specific storage on a specific node.
type ReplicationTarget struct {
	NodeID    string
	StorageID string
}

// FollowerTargets returns one target per follower placement — NOT deduplicated
// by node. Multiple placements on the same node produce multiple targets,
// enabling same-node replication across different file storages.
func (t TierConfig) FollowerTargets(nscs []NodeStorageConfig) []ReplicationTarget {
	var targets []ReplicationTarget
	for _, storageID := range t.FollowerStorageIDs() {
		nid := NodeIDForStorage(storageID, nscs)
		if nid != "" {
			targets = append(targets, ReplicationTarget{NodeID: nid, StorageID: storageID})
		}
	}
	return targets
}

// ClusterTLS holds mTLS material for the cluster gRPC port.
// All fields are PEM-encoded except JoinToken which is a hex string.
// Stored atomically via a single Raft command to prevent inconsistent states.
type ClusterTLS struct {
	CACertPEM      string `json:"ca_cert_pem"`
	CAKeyPEM       string `json:"ca_key_pem"`
	ClusterCertPEM string `json:"cluster_cert_pem"`
	ClusterKeyPEM  string `json:"cluster_key_pem"`
	JoinToken      string `json:"join_token"`
}

// ---------------------------------------------------------------------------
// Identity types (users, tokens)
// ---------------------------------------------------------------------------

// User represents a user account.
type User struct {
	ID                 uuid.UUID `json:"id"`
	Username           string    `json:"username"`
	PasswordHash       string    `json:"password_hash"`
	Role               string    `json:"role"` // "admin" or "user"
	Preferences        string    `json:"preferences,omitempty"`          // opaque JSON blob
	TokenInvalidatedAt time.Time `json:"token_invalidated_at,omitzero"` // tokens issued before this are invalid
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// RefreshToken represents a stored refresh token (hash only, not the opaque token itself).
type RefreshToken struct {
	ID        uuid.UUID `json:"id"`
	UserID    uuid.UUID `json:"user_id"`
	TokenHash string    `json:"token_hash"` // SHA-256 of the opaque token
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

// ParseBytes parses a byte size string with optional suffix (B, KB, MB, GB).
func ParseBytes(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty value")
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

// StringPtr returns a pointer to s.
//
//go:fix inline
func StringPtr(s string) *string { return new(s) }

// UUIDPtr returns a pointer to id.
//
//go:fix inline
func UUIDPtr(id uuid.UUID) *uuid.UUID { return new(id) }

// ManagedFileConfig describes an uploaded managed file managed by the system.
// Only metadata is stored in the config; the file itself lives on disk at
// <home>/lookups/<ID>/<Name>.
type ManagedFileConfig struct {
	ID         uuid.UUID `json:"id"`
	Name       string    `json:"name"`       // original filename
	SHA256     string    `json:"sha256"`     // hex-encoded content hash
	Size       int64     `json:"size"`       // file size in bytes
	UploadedAt time.Time `json:"uploadedAt"` // upload timestamp
}

