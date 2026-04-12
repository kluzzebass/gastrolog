package system

import (
	"slices"
	"strings"

	"github.com/google/uuid"
)

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
