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
	Type VaultType `json:"type,omitempty"`

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
	Placements []VaultPlacement `json:"placements,omitempty"`

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

// IsCloud reports whether this vault is cloud-backed (CloudServiceID set).
// Mirrors TierConfig.IsCloud() now that VaultConfig absorbs the storage
// fields. Once TierConfig is deleted, IsCloud lives only here.
func (v VaultConfig) IsCloud() bool {
	return v.CloudServiceID != nil
}

// VaultType is the new canonical name for the storage-shape enum during
// the vault refactor (gastrolog-257l7). Alias of TierType for now —
// once consumers migrate, TierType and its constants are deleted and
// this becomes the only name.
type VaultType = TierType

const (
	VaultTypeMemory = TierTypeMemory
	VaultTypeFile   = TierTypeFile
	VaultTypeJSONL  = TierTypeJSONL
)

// VaultPlacement is the new canonical name for storage assignments
// during the refactor. Alias of TierPlacement; once consumers migrate,
// TierPlacement is deleted and this becomes the only name.
type VaultPlacement = TierPlacement

// MergeVaultFromTiers populates v's merged storage/lifecycle fields from
// its (single) tier in tiers, returning the merged copy. Used during the
// vault refactor (gastrolog-257l7) to ensure VaultConfig values written
// to the store carry the post-tier shape, even when the source data still
// comes from a separate TierConfig list. The original v is not mutated.
//
// If the vault has no tiers, returns v unchanged. If the vault has multiple
// tiers, the lowest-position tier's fields win (matches the eventual
// "one storage shape per vault" model). Fields explicitly set on v are
// not overwritten.
func MergeVaultFromTiers(v VaultConfig, tiers []TierConfig) VaultConfig {
	matched := VaultTiers(tiers, v.ID)
	if len(matched) == 0 {
		return v
	}
	t := matched[0]
	if v.Type == "" {
		v.Type = t.Type
	}
	if v.RotationPolicyID == nil && t.RotationPolicyID != nil {
		id := *t.RotationPolicyID
		v.RotationPolicyID = &id
	}
	if len(v.RetentionRules) == 0 && len(t.RetentionRules) > 0 {
		v.RetentionRules = make([]RetentionRule, len(t.RetentionRules))
		for i, r := range t.RetentionRules {
			v.RetentionRules[i] = RetentionRule{
				RetentionPolicyID: r.RetentionPolicyID,
				Action:            r.Action,
				EjectRouteIDs:     append([]glid.GLID(nil), r.EjectRouteIDs...),
			}
		}
	}
	if v.MemoryBudgetBytes == 0 {
		v.MemoryBudgetBytes = t.MemoryBudgetBytes
	}
	if v.StorageClass == 0 {
		v.StorageClass = t.StorageClass
	}
	if v.CloudServiceID == nil && t.CloudServiceID != nil {
		id := *t.CloudServiceID
		v.CloudServiceID = &id
	}
	if v.ReplicationFactor == 0 {
		v.ReplicationFactor = t.ReplicationFactor
	}
	if v.Path == "" {
		v.Path = t.Path
	}
	if v.CacheEviction == "" {
		v.CacheEviction = t.CacheEviction
	}
	if v.CacheBudget == "" {
		v.CacheBudget = t.CacheBudget
	}
	if v.CacheTTL == "" {
		v.CacheTTL = t.CacheTTL
	}
	return v
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
