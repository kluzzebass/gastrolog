package system

import (
	"gastrolog/internal/glid"
	"cmp"
	"slices"

)

// VaultConfig describes a storage backend to instantiate.
type VaultConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID glid.GLID `json:"id"`

	// Name is the human-readable display name (unique).
	Name string `json:"name"`

	// Enabled indicates whether ingestion is enabled for this vault.
	// When false, the vault will not receive new records from the ingest pipeline.
	Enabled bool `json:"enabled,omitempty"`
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

	// NodeID is the raft server ID of the node that owns this ingester.
	// Empty means unscoped (legacy/migration compatibility).
	NodeID string `json:"nodeId,omitempty"`
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
