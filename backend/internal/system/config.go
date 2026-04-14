package system

import (
	"gastrolog/internal/glid"
	"errors"
	"strconv"
	"strings"
	"time"

)


// ServerSettings groups the server-level settings that are loaded/saved
// atomically via LoadServerSettings / SaveServerSettings.
type ServerSettings struct {
	Auth      AuthConfig      `json:"auth,omitzero"`
	Query     QueryConfig     `json:"query,omitzero"`
	Scheduler SchedulerConfig `json:"scheduler,omitzero"`
	TLS       TLSConfig       `json:"tls,omitzero"`
	Lookup    LookupConfig    `json:"lookup,omitzero"`
	Cluster   ClusterConfig   `json:"cluster,omitzero"`
	MaxMind   MaxMindConfig   `json:"maxmind,omitzero"`
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
// System is the top-level state managed by the system Raft group.
// Config holds operator-controlled settings. Runtime holds cluster-managed state.
type System struct {
	Config  Config  `json:"config"`
	Runtime Runtime `json:"runtime"`
}

// Config holds operator-controlled settings — things the operator creates,
// edits, and deletes via the CLI or UI.
type Config struct {
	// Entity collections.
	Filters           []FilterConfig          `json:"filters,omitempty"`
	RotationPolicies  []RotationPolicyConfig  `json:"rotationPolicies,omitempty"`
	RetentionPolicies []RetentionPolicyConfig `json:"retentionPolicies,omitempty"`
	Ingesters         []IngesterConfig        `json:"ingesters,omitempty"`
	Vaults            []VaultConfig           `json:"vaults,omitempty"`
	Routes            []RouteConfig           `json:"routes,omitempty"`
	Certs             []CertPEM               `json:"certs,omitempty"`
	ManagedFiles      []ManagedFileConfig     `json:"managedFiles,omitempty"`
	CloudServices     []CloudService          `json:"cloudServices,omitempty"`
	Tiers             []TierConfig            `json:"tiers,omitempty"`

	// Server-level settings.
	Auth      AuthConfig      `json:"auth,omitzero"`
	Query     QueryConfig     `json:"query,omitzero"`
	Scheduler SchedulerConfig `json:"scheduler,omitzero"`
	TLS       TLSConfig       `json:"tls,omitzero"`
	Lookup    LookupConfig    `json:"lookup,omitzero"`
	Cluster   ClusterConfig   `json:"cluster,omitzero"`
	MaxMind   MaxMindConfig   `json:"maxmind,omitzero"`
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
	ID                 glid.GLID `json:"id"`
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
	ID        glid.GLID `json:"id"`
	UserID    glid.GLID `json:"user_id"`
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
func UUIDPtr(id glid.GLID) *glid.GLID { return new(id) }

// ManagedFileConfig describes an uploaded managed file managed by the system.
// Only metadata is stored in the config; the file itself lives on disk at
// <home>/lookups/<ID>/<Name>.
type ManagedFileConfig struct {
	ID         glid.GLID `json:"id"`
	Name       string    `json:"name"`       // original filename
	SHA256     string    `json:"sha256"`     // hex-encoded content hash
	Size       int64     `json:"size"`       // file size in bytes
	UploadedAt time.Time `json:"uploadedAt"` // upload timestamp
}

