package system

import (
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

	// MemoryBudget caps in-memory storage for memory-typed vaults, as a size
	// expression ("512MiB"). Empty = unset (defaulted at creation); resolve
	// with SizeOrDefault, never by parsing here.
	MemoryBudget string `json:"memoryBudget,omitempty"`

	// StorageClass selects which file storage class on a node hosts this vault.
	StorageClass uint32 `json:"storageClass,omitempty"`

	// CloudServiceID, when non-nil, marks this vault as cloud-backed.
	CloudServiceID *glid.GLID `json:"cloudServiceId,omitempty"`

	// ReplicationFactor is the desired RF (1 = no replication, default).
	ReplicationFactor uint32 `json:"replicationFactor,omitempty"`

	// Path is the direct path for JSONL sinks.
	Path string `json:"path,omitempty"`

	// CacheEviction is "lru" (default) or "ttl" — only meaningful for cloud-backed.
	CacheEviction string `json:"cacheEviction,omitempty"`

	// CacheBudget is the warm-cache soft cap for cloud-backed chunks, as a size
	// expression ("1GiB"). Empty = unset (defaulted at creation for cloud
	// vaults); non-cloud vaults have no warm cache and leave it empty.
	CacheBudget string `json:"cacheBudget,omitempty"`

	// CacheTTL is the warm-cache eviction age (ttl mode only) as a duration
	// expression ("1h", "7d"). Empty = unset.
	CacheTTL string `json:"cacheTtl,omitempty"`

	// RetentionDisposition decides what happens to records as retention
	// ages chunks out of this vault. "delete" (default) drops the records
	// and frees storage immediately, never touching the routing engine.
	// "route" feeds the records back through the routing engine with
	// _source = "retention" so operator-configured routes can forward
	// them to archive vaults, cold storage, etc. "transfer" re-homes the
	// sealed chunk to RetentionTransferTargetVaultID UNCHANGED — no
	// record decode, no re-route, no re-ingest (gastrolog-2l918).
	//
	// Default is "delete" because the routing path is fail-open: an
	// accidental match on a retention-source route can create a cascade
	// (re-ingested records produce new chunks that themselves expire on
	// the next sweep). Operators who want forwarding must opt in
	// explicitly. See gastrolog-18du3.
	RetentionDisposition string `json:"retentionDisposition,omitempty"`

	// RetentionTransferTargetVaultID is the destination vault for
	// RetentionDisposition == "transfer". Required when, and only valid
	// when, disposition is "transfer" — PutVault rejects self-transfer
	// and non-file source/target. See
	// docs/retention-transfer-disposition-design.md (gastrolog-2l918).
	RetentionTransferTargetVaultID *glid.GLID `json:"retentionTransferTargetVaultId,omitempty"`

	// gastrolog-9akebz: DiskFreeWarn/DiskFreeFloor removed from here — the
	// disk-guard free-space thresholds are a property of the storage a
	// vault is placed on (system.FileStorage.DiskFreeWarn/DiskFreeFloor),
	// not the vault: N vaults sharing one storage evaluating the same
	// statfs against potentially different per-vault thresholds was the
	// modeling error. A vault's refuse signal is now DERIVED — refused
	// whenever any of its placements' storages is below its floor. See
	// orchestrator.vaultAdmissionCauses (disk_guard.go).

	// gastrolog-33ul6h: MaxSize removed. The vault's disk-claim bound is no
	// longer a vault-level field — it lives on the retention policy
	// (RetentionPolicyConfig.MaxSize, which drains AND refuses at the same
	// bound) attached via RetentionRules, min-wins across attached policies,
	// and NO bound at all when no attached policy carries one — there is no
	// per-vault default (gastrolog-vl2p98); the volume-level storage
	// thresholds are the backstop. See orchestrator.resolveVaultSizeBound
	// (disk_guard.go).
}

// Defaults are expressions, like the fields they fill: what the operator would
// have typed. Stored verbatim at creation, so a defaulted vault reads exactly
// like a configured one (gastrolog-etcjdx).

// DefaultVaultCacheBudget is the warm-cache soft cap applied when a
// cloud-backed vault is created without one (the value the field long
// documented but never applied; see gastrolog-338j51). The cache is a soft LRU
// cap over cloud-backed chunk copies, so a too-small budget costs re-reads
// from the blob store, not refused records; but unbounded is still a defect.
const DefaultVaultCacheBudget = "1GiB"

// DefaultVaultMemoryBudget is the in-memory cap applied when a memory-typed
// vault is created without one. An unbounded memory vault grows until the
// process OOMs, so unset must be a bounded default. Matches the disk budgets
// for consistency; RAM is scarcer than disk, so operators on small nodes
// should lower it explicitly (gastrolog-1qd5wz).
const DefaultVaultMemoryBudget = "1GiB"

// Canonical values for VaultConfig.RetentionDisposition.
const (
	// RetentionDispositionDelete drops records when retention triggers.
	// Storage is freed immediately; the routing engine is not invoked.
	RetentionDispositionDelete = "delete"
	// RetentionDispositionRoute streams retention output through the
	// routing engine so operator-configured routes can forward records
	// to other destinations.
	RetentionDispositionRoute = "route"
	// RetentionDispositionTransfer re-homes the sealed chunk to
	// RetentionTransferTargetVaultID unchanged — no record decode, no
	// re-route, no re-ingest. See gastrolog-2l918.
	RetentionDispositionTransfer = "transfer"
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
	case RetentionDispositionTransfer:
		return RetentionDispositionTransfer
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
