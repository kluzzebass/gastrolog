package system

import (
	"fmt"

	"gastrolog/internal/glid"
)

// VaultConfig describes a vault — the unit of independent storage and the
// only abstraction over the chunk layer.
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

	// WriteModel selects the vault data-plane write path. Empty and "v1" use
	// the current leader-driven active-chunk path (default). "v2" opts into
	// destination-vault sequencing and spool-first accepted writes (fan-out V2).
	// Per-vault opt-in; see docs/fan-out/v2/implementation-plan.md.
	WriteModel string `json:"writeModel,omitempty"`

	// RetentionDisposition decides what happens to records as retention
	// ages chunks out of this vault. "delete" (default) drops the records
	// and frees storage immediately, never touching the routing engine.
	// "route" feeds the records back through the routing engine with
	// _source = "retention" so operator-configured routes can forward
	// them to archive vaults, cold storage, etc.
	//
	// Default is "delete" because the routing path is fail-open: an
	// accidental match on a retention-source route can create a cascade
	// (re-ingested records produce new chunks that themselves expire on
	// the next sweep). Operators who want forwarding must opt in
	// explicitly. See gastrolog-18du3.
	RetentionDisposition string `json:"retentionDisposition,omitempty"`
}

// VaultWriteModel names a vault's data-plane write path.
type VaultWriteModel string

const (
	// VaultWriteModelV1 is the default: synchronous active-chunk append with
	// leader-coordinated chunk identity on the write path.
	VaultWriteModelV1 VaultWriteModel = "v1"
	// VaultWriteModelV2 is fan-out V2: destination-vault sequencing, spool
	// segments, and asynchronous materialization (gated per vault).
	VaultWriteModelV2 VaultWriteModel = "v2"
)

// ResolveWriteModel returns the effective write model for this vault.
// Empty and unrecognized values resolve to V1 so existing configs stay stable.
func (v VaultConfig) ResolveWriteModel() VaultWriteModel {
	switch v.WriteModel {
	case string(VaultWriteModelV2):
		return VaultWriteModelV2
	case "", string(VaultWriteModelV1):
		return VaultWriteModelV1
	default:
		return VaultWriteModelV1
	}
}

// UsesV2WriteModel reports whether this vault is opted into the V2 write path.
func (v VaultConfig) UsesV2WriteModel() bool {
	return v.ResolveWriteModel() == VaultWriteModelV2
}

// ValidateWriteModel rejects unknown writeModel config values.
func (v VaultConfig) ValidateWriteModel() error {
	switch v.WriteModel {
	case "", string(VaultWriteModelV1), string(VaultWriteModelV2):
		return nil
	default:
		return fmt.Errorf("vault %q: invalid writeModel %q (want %q, %q, or empty)",
			v.Name, v.WriteModel, VaultWriteModelV1, VaultWriteModelV2)
	}
}

// Canonical values for VaultConfig.RetentionDisposition.
const (
	// RetentionDispositionDelete drops records when retention triggers.
	// Storage is freed immediately; the routing engine is not invoked.
	RetentionDispositionDelete = "delete"
	// RetentionDispositionRoute streams retention output through the
	// routing engine so operator-configured routes can forward records
	// to other destinations.
	RetentionDispositionRoute = "route"
)

// ResolveRetentionDisposition returns the effective retention disposition
// for this vault. Empty/unrecognized values resolve to "delete" — the
// safe default. Callers that need to branch on disposition should use
// this rather than reading the raw field, so the empty-string sentinel
// is centralized in one place.
func (v VaultConfig) ResolveRetentionDisposition() string {
	switch v.RetentionDisposition {
	case RetentionDispositionRoute:
		return RetentionDispositionRoute
	default:
		return RetentionDispositionDelete
	}
}

// IsCloud reports whether this vault is cloud-backed (CloudServiceID set).
func (v VaultConfig) IsCloud() bool {
	return v.CloudServiceID != nil
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

// RouteStage is one step in a route's pipeline. Phase 5 (gastrolog-4kkoo)
// introduces the stages model; today's only variant is Match. Future
// stages (enrich, redact, sample, fork, route_by_field) per
// gastrolog-5e85x (Programmable Ingestion) plug in here as additional
// kinds.
type RouteStage struct {
	// Match is the boolean filter expression that gates the route.
	// Exactly one stage variant must be set per stage; today only
	// Match exists.
	Match *MatchStage `json:"match,omitempty"`
}

// MatchStage gates the route on a boolean filter expression. The
// expression is evaluated against the record (which carries
// system-injected synthetic attributes via reserved-prefix keys:
// _source, _ingester, _vault, _reason).
type MatchStage struct {
	Expression string `json:"expression"`
}

// RouteConfig is a row in the cluster-wide routing table. Phase 5
// (gastrolog-4kkoo) collapsed source predicates and content filters
// into one expression-language model and dropped FilterConfig as a
// separate entity. Routes are evaluated in priority order; first
// match wins; no-match drops silently.
type RouteConfig struct {
	// ID is the unique identifier (UUIDv7).
	ID glid.GLID `json:"id"`

	// Name is the human-readable display name (unique). Used as the
	// deterministic tiebreaker when two routes share a priority.
	Name string `json:"name"`

	// Priority orders routes for first-match-wins evaluation. Lower
	// fires first. Routes at the same priority are tiebroken by Name
	// lexicographically.
	Priority int32 `json:"priority,omitempty"`

	// Stages is the route's pipeline. Today: a single MatchStage.
	// Future phases (gastrolog-5e85x) add transform stages.
	Stages []RouteStage `json:"stages,omitempty"`

	// Destinations lists the vault IDs that this route sends matched
	// records to.
	Destinations []glid.GLID `json:"destinations"`

	// Distribution controls how matched records are distributed across
	// destinations: "fanout" (default), "round-robin", or "failover".
	// Only meaningful when len(Destinations) > 1.
	Distribution DistributionMode `json:"distribution,omitempty"`

	// Enabled controls whether this route is active in the routing table.
	Enabled bool `json:"enabled,omitempty"`
}

// MatchExpression returns the route's match-stage expression, or "" if
// the route has no match stage. Convenience accessor — the match stage
// is the gating predicate today (Phase 5 ships only one stage type).
func (r *RouteConfig) MatchExpression() string {
	for _, s := range r.Stages {
		if s.Match != nil {
			return s.Match.Expression
		}
	}
	return ""
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
	// Honored only when AllNodes is false. Parallel ingesters run on every
	// listed node; singleton ingesters run on one of them.
	NodeIDs []string `json:"nodeIds,omitempty"`

	// Singleton selects HA semantics. When false (default), the ingester runs
	// on every eligible node (parallel). When true, the placement manager
	// Raft-assigns it to exactly one alive node with automatic failover. Only
	// takes effect when the registered ingester type has SingletonSupported.
	Singleton bool `json:"singleton,omitempty"`

	// AllNodes, when true, makes the eligible-node set the entire cluster
	// (re-evaluated on cluster-membership changes — joiners automatically
	// pick up the ingester). When false, eligibility is determined by NodeIDs
	// (literal pin). The dispatcher must consult cluster membership on every
	// tick when AllNodes is true, not snapshot at config-write time.
	AllNodes bool `json:"allNodes,omitempty"`
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
