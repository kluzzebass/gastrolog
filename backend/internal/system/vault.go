package system

import (
	"cmp"
	"gastrolog/internal/glid"
	"slices"
)

// VaultConfig describes a vault — the unit of independent storage and the
// only abstraction over the chunk layer (post-tier model).
//
// Pre-refactor, the storage/lifecycle fields lived on TierConfig and a vault
// owned 1..N tiers. During the vault refactor (gastrolog-257l7), VaultConfig
// absorbs every tier field; once consumers migrate, TierConfig is deleted.
//
// All new fields are JSON-omitempty so existing serialized data
// (which only has id/name/enabled) still deserializes cleanly.
type VaultConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID glid.GLID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// Enabled indicates whether ingestion is enabled for this vault.
	// When false, the vault will not receive new records from the ingest pipeline.
	Enabled bool `json:"enabled,omitempty"`

	// Type is the storage shape (memory / file / jsonl). Cloud-backed vaults
	// are file vaults with CloudServiceID set; there is no "cloud" type.
	Type TierType `json:"type,omitempty"`

	// RotationPolicyID references a RotationPolicyConfig.
	RotationPolicyID *glid.GLID `json:"rotationPolicyId,omitempty"`

	// RetentionRules are evaluated in order on chunk-age events.
	RetentionRules []RetentionRule `json:"retentionRules,omitempty"`

	// MemoryBudgetBytes caps in-memory storage for memory-typed vaults.
	MemoryBudgetBytes uint64 `json:"memoryBudgetBytes,omitempty"`

	// StorageClass selects which file storage class on a node hosts this vault.
	StorageClass uint32 `json:"storageClass,omitempty"`

	// CloudServiceID, when non-nil, marks this vault as cloud-backed.
	CloudServiceID *glid.GLID `json:"cloudServiceId,omitempty"`

	// ReplicationFactor is the desired RF (1 = no replication, default).
	ReplicationFactor uint32 `json:"replicationFactor,omitempty"`

	// Path is the direct path for JSONL sinks.
	Path string `json:"path,omitempty"`

	// Placements are system-managed file storage assignments.
	Placements []TierPlacement `json:"placements,omitempty"`

	// CacheEviction is "lru" (default) or "ttl" — only meaningful for cloud-backed.
	CacheEviction string `json:"cacheEviction,omitempty"`

	// CacheBudget caps the local cache size (e.g. "1GB", "500MB", default: "1GiB").
	CacheBudget string `json:"cacheBudget,omitempty"`

	// CacheTTL is the eviction TTL duration (e.g. "1h", "7d") for ttl mode.
	CacheTTL string `json:"cacheTtl,omitempty"`
}

// VaultTierIDs returns the ordered tier IDs for a vault by filtering tiers
// with matching VaultID and sorting by Position. This replaces the old
// VaultConfig.TierIDs field — tier ownership now lives on TierConfig.
func VaultTierIDs(tiers []TierConfig, vaultID glid.GLID) []glid.GLID {
	type entry struct {
		id  glid.GLID
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
	ids := make([]glid.GLID, len(matched))
	for i, e := range matched {
		ids[i] = e.id
	}
	return ids
}

// VaultTiers returns the ordered tier configs for a vault.
func VaultTiers(tiers []TierConfig, vaultID glid.GLID) []TierConfig {
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

// VaultPlacements returns the placements for a vault by looking up its
// (single) tier and returning that tier's placements. Bridge helper used
// during the vault refactor (gastrolog-257l7) so callers can express
// placement queries by vaultID instead of tierID. Once tiers go away,
// placements will be stored vault-keyed and this helper collapses to a
// direct lookup.
//
// In the 1:N tier-vault model that still exists during the migration,
// VaultPlacements returns the placements of the vault's first tier
// (lowest Position). If the vault has no tiers, returns nil.
func VaultPlacements(tiers []TierConfig, placementsByTier map[glid.GLID][]TierPlacement, vaultID glid.GLID) []TierPlacement {
	tierIDs := VaultTierIDs(tiers, vaultID)
	if len(tierIDs) == 0 {
		return nil
	}
	return placementsByTier[tierIDs[0]]
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
	ID glid.GLID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// FilterID references a FilterConfig by UUID.
	// Nil means no filter (route receives nothing).
	FilterID *glid.GLID `json:"filterId,omitempty"`

	// Destinations lists the vault IDs that this route sends messages to.
	Destinations []glid.GLID `json:"destinations"`

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
	ID glid.GLID `json:"id"`

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

	// NodeIDs lists the raft server IDs of nodes allowed to run this ingester.
	// Parallel ingesters run on all listed nodes. Singleton ingesters run on one.
	NodeIDs []string `json:"nodeIds,omitempty"`

	// Singleton selects HA semantics. When false (default), the ingester runs
	// on every node in NodeIDs (parallel). When true, the placement manager
	// Raft-assigns it to exactly one alive node with automatic failover. Only
	// takes effect when the registered ingester type has SingletonSupported.
	Singleton bool `json:"singleton,omitempty"`
}

// CertPEM holds certificate content. Either stored PEM or file paths (directory monitoring).
// When both are set, file paths take precedence and are watched for changes.
type CertPEM struct {
	ID       glid.GLID `json:"id"`
	Name     string    `json:"name"`
	CertPEM  string    `json:"cert_pem,omitempty"`
	KeyPEM   string    `json:"key_pem,omitempty"`
	CertFile string    `json:"cert_file,omitempty"`
	KeyFile  string    `json:"key_file,omitempty"`
}
