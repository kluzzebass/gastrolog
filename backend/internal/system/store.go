package system

import (
	"context"
	"time"

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
	// Load reads the full system state (config + runtime). Returns nil if nothing exists (bootstrap signal).
	Load(ctx context.Context) (*System, error)

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

	// Tier placements (runtime — system-managed, not operator-edited)
	GetTierPlacements(ctx context.Context, tierID uuid.UUID) ([]TierPlacement, error)
	SetTierPlacements(ctx context.Context, tierID uuid.UUID, placements []TierPlacement) error

	// Node storage (per-node, runtime — discovered at enrollment)
	GetNodeStorageConfig(ctx context.Context, nodeID string) (*NodeStorageConfig, error)
	ListNodeStorageConfigs(ctx context.Context) ([]NodeStorageConfig, error)
	SetNodeStorageConfig(ctx context.Context, cfg NodeStorageConfig) error

	// Setup wizard (runtime — UI state)
	GetSetupWizardDismissed(ctx context.Context) (bool, error)
	SetSetupWizardDismissed(ctx context.Context, dismissed bool) error
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
