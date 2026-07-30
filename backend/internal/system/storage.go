package system

import (
	"fmt"
	"gastrolog/internal/glid"
	"slices"
	"strings"
	"time"
)

// NodeState is the lifecycle stage of a cluster node. See
// docs/node-lifecycle-design.md for the full state machine.
type NodeState uint8

const (
	// NodeStateUnknown is the zero value, present on legacy records
	// minted before the State field existed. Treat as NodeStateLive
	// via EffectiveState() for backward compatibility.
	NodeStateUnknown NodeState = iota
	NodeStateLive
	NodeStateUnreachable
	NodeStateMaintenance
	NodeStateDraining
	NodeStateDecommissioning
)

// String returns the human-readable label.
func (s NodeState) String() string {
	switch s {
	case NodeStateUnknown:
		return "unknown"
	case NodeStateLive:
		return "live"
	case NodeStateUnreachable:
		return "unreachable"
	case NodeStateMaintenance:
		return "maintenance"
	case NodeStateDraining:
		return "draining"
	case NodeStateDecommissioning:
		return "decommissioning"
	default:
		return fmt.Sprintf("nodestate(%d)", uint8(s))
	}
}

// ValidateNodeStateTransition returns nil if `to` is a legal successor
// of `from`, or an error describing the illegal transition. Legacy
// records with NodeStateUnknown are treated as NodeStateLive for the
// transition check.
//
// Legal transitions (see docs/node-lifecycle-design.md "Transitions in detail"):
//
//	Live           → Unreachable | Maintenance | Draining
//	Unreachable    → Live | Maintenance | Draining
//	Maintenance    → Live | Draining
//	Draining       → Decommissioning | Live (cancel)
//	Decommissioning → (Removed via DeleteNode; no in-FSM successor)
func ValidateNodeStateTransition(from, to NodeState) error {
	effFrom := from
	if effFrom == NodeStateUnknown {
		effFrom = NodeStateLive
	}
	if effFrom == to {
		// Idempotent — re-applying the same state is a no-op success.
		return nil
	}
	var legal []NodeState
	switch effFrom {
	case NodeStateUnknown:
		// Unreachable: effFrom was remapped to Live above; this branch
		// is unreachable but present to satisfy exhaustive-switch
		// linting.
		legal = nil
	case NodeStateLive:
		legal = []NodeState{NodeStateUnreachable, NodeStateMaintenance, NodeStateDraining}
	case NodeStateUnreachable:
		legal = []NodeState{NodeStateLive, NodeStateMaintenance, NodeStateDraining}
	case NodeStateMaintenance:
		legal = []NodeState{NodeStateLive, NodeStateDraining}
	case NodeStateDraining:
		legal = []NodeState{NodeStateDecommissioning, NodeStateLive}
	case NodeStateDecommissioning:
		// No in-FSM successor — Decommissioning → Removed happens via
		// DeleteNode, not a state transition.
		legal = nil
	}
	if slices.Contains(legal, to) {
		return nil
	}
	return fmt.Errorf("illegal node state transition: %s → %s", from, to)
}

// NodeConfig represents a cluster node configuration with its
// human-readable name and lifecycle state.
type NodeConfig struct {
	ID   glid.GLID `json:"id"`
	Name string    `json:"name"`
	// State is the node's lifecycle stage. Zero value (NodeStateUnknown)
	// is treated as NodeStateLive via EffectiveState() for backward
	// compatibility with records minted before this field existed.
	State NodeState `json:"state,omitempty"`
	// StateSince is the wall-clock instant the current State was
	// entered. Zero if the State has never been explicitly set (e.g.,
	// legacy record).
	StateSince time.Time `json:"stateSince,omitzero"`
}

// EffectiveState returns the node's State with the legacy/zero-value
// case (NodeStateUnknown) mapped to NodeStateLive. Use this for any
// behavioral decision that consults State; the raw `State` field
// preserves the FSM record's actual value for round-tripping.
func (n NodeConfig) EffectiveState() NodeState {
	if n.State == NodeStateUnknown {
		return NodeStateLive
	}
	return n.State
}

// FileStorage defines a local file storage on a node.
type FileStorage struct {
	ID                glid.GLID `json:"id"`
	StorageClass      uint32    `json:"storageClass"`
	Name              string    `json:"name"`
	Path              string    `json:"path,omitempty"`
	MemoryBudgetBytes uint64    `json:"memoryBudgetBytes,omitempty"`

	// DiskFreeWarn / DiskFreeFloor are disk-guard free-space thresholds on
	// this storage's volume: an absolute free-space size ("10GB") or a
	// percentage of the volume ("10%"), resolved per node against the
	// volume actually sampled (ParseSizeOrPercent). Empty inherits the
	// node defaults — the typeable expressions "10%" (warn) and "3%"
	// (floor). Warn raises the disk-space alarm naming this storage;
	// floor puts every vault placed here into admission refuse (cause
	// STORAGE_DISK_PROTECT) while vaults on healthy storages keep
	// ingesting. They live on the storage, not on VaultConfig: the
	// thresholds guard the volume, not the vaults sharing it — N vaults on
	// one storage evaluating the same statfs against potentially different
	// per-vault thresholds is a modeling error.
	DiskFreeWarn  string `json:"diskFreeWarn,omitempty"`
	DiskFreeFloor string `json:"diskFreeFloor,omitempty"`
}

// NodeStorageConfig defines the file storages for a specific cluster node.
type NodeStorageConfig struct {
	NodeID       string        `json:"nodeId"`
	FileStorages []FileStorage `json:"fileStorages"`
}

// CloudService defines a cluster-wide cloud storage endpoint.
// CloudStorageTransition defines a single step in an archival lifecycle chain.
type CloudStorageTransition struct {
	After string `json:"after"` // duration string (e.g. "30s", "7d", "2w", "360d")
	// CloudStorageClass is the archival TIER ("GLACIER", "cold"); empty means
	// delete at this age. Not the uint32 storage_class elsewhere in this file,
	// which selects a local disk.
	CloudStorageClass string `json:"cloudStorageClass"`
}

type CloudService struct {
	ID               glid.GLID `json:"id"`
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
	Transitions       []CloudStorageTransition `json:"transitions,omitempty"`       // ordered by After duration
	RestoreSpeed      string                   `json:"restoreSpeed,omitempty"`      // default restore speed
	RestoreDays       uint32                   `json:"restoreDays,omitempty"`       // S3 restore window
	SuspectGraceDays  uint32                   `json:"suspectGraceDays,omitempty"`  // default 7
	ReconcileSchedule string                   `json:"reconcileSchedule,omitempty"` // default "0 3 * * *"
}

// StoreParams returns this cloud service's blobstore factory params — the
// exact key/value shape blobstore.CreateStore and blobstore.ValidateConfig
// consume. Empty fields are omitted. This is the single mapping from the
// persisted CloudService config to store params: both vault init
// (orchestrator addCloudParams) and config-accept validation
// (PutCloudService) go through it, so a config that passes validation
// cannot fail store creation on shape.
//
// Keys mirror the blobstore.Param* constants; system cannot import
// blobstore without dragging provider SDKs into the config package, so
// the literals here are pinned against those constants by blobstore's
// factory tests.
func (cs CloudService) StoreParams() map[string]string {
	params := make(map[string]string)
	set := func(k, v string) {
		if v != "" {
			params[k] = v
		}
	}
	set("bucket", cs.Bucket)
	set("region", cs.Region)
	set("endpoint", cs.Endpoint)
	set("access_key", cs.AccessKey)
	set("secret_key", cs.SecretKey)
	set("container", cs.Container)
	set("connection_string", cs.ConnectionString)
	set("credentials_json", cs.CredentialsJSON)
	return params
}

// VaultType identifies the storage medium for a vault.
//
// There is no distinct "cloud" type: a cloud-backed vault is a file vault
// with CloudServiceID set, exposed via VaultConfig.IsCloud(). One file path
// serves both, flipping behavior on whether a cloud store is wired.
type VaultType string

const (
	VaultTypeMemory VaultType = "memory"
	VaultTypeFile   VaultType = "file"
	VaultTypeJSONL  VaultType = "jsonl"
)

// VaultPlacement assigns one replica of a vault to a specific file storage.
// The node is derived from the file storage's NodeStorageConfig.
type VaultPlacement struct {
	StorageID string `json:"storageId"`
	Leader    bool   `json:"leader"`
}

// LeaderStorageID returns the storage ID of the leader placement, or empty if unplaced.
func LeaderStorageID(placements []VaultPlacement) string {
	for _, p := range placements {
		if p.Leader {
			return p.StorageID
		}
	}
	return ""
}

// FollowerStorageIDs returns the storage IDs of all follower placements.
func FollowerStorageIDs(placements []VaultPlacement) []string {
	var ids []string
	for _, p := range placements {
		if !p.Leader {
			ids = append(ids, p.StorageID)
		}
	}
	return ids
}

// StorageIDs returns all placed storage IDs (leader first, then followers).
func StorageIDs(placements []VaultPlacement) []string {
	var ids []string
	for _, p := range placements {
		if p.Leader {
			ids = append([]string{p.StorageID}, ids...)
		} else {
			ids = append(ids, p.StorageID)
		}
	}
	return ids
}

// SyntheticStoragePrefix is the prefix for synthetic storage IDs used when a node has
// no file storages (e.g. memory vaults). Format: "node:<nodeID>".
const SyntheticStoragePrefix = "node:"

// SyntheticStorageID returns a synthetic storage ID for a node without file storages.
func SyntheticStorageID(nodeID string) string { return SyntheticStoragePrefix + nodeID }

// NodeIDForStorage resolves a storage ID to its node ID using the provided storage configs.
// Handles synthetic storage IDs of the form "node:<nodeID>" for nodes without file storages.
func NodeIDForStorage(storageID string, nscs []NodeStorageConfig) string {
	// Check synthetic storage IDs first (used for memory vaults on nodes without file storages).
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

// StorageIDForNode returns the best storage ID on a given node for a vault.
// For file vaults it requires an exact storage-class match and returns ""
// when the node has none — the caller must fail placement loudly. A silent
// FileStorages[0] fallback here placed leaders on the wrong disk class while
// follower placement (eligibleStorages/storageEligible) stayed strict.
// Memory/JSONL vaults have no class requirement and fall back to a synthetic
// storage ID.
func StorageIDForNode(nodeID string, v VaultConfig, nscs []NodeStorageConfig) string {
	idx := slices.IndexFunc(nscs, func(n NodeStorageConfig) bool { return n.NodeID == nodeID })

	switch v.Type {
	case VaultTypeMemory, VaultTypeJSONL:
		// No storage class — pick any storage, or synthetic if none.
		if idx >= 0 && len(nscs[idx].FileStorages) > 0 {
			return nscs[idx].FileStorages[0].ID.String()
		}
		return SyntheticStorageID(nodeID)
	case VaultTypeFile:
		if idx < 0 {
			return ""
		}
		// Single storage class for all file vaults (local-only and
		// cloud-backed alike). The active chunk and the warm cache
		// live at the same path under chunkDir, so distinguishing
		// "active" and "cache" classes serves no purpose.
		for _, fs := range nscs[idx].FileStorages {
			if fs.StorageClass == v.StorageClass {
				return fs.ID.String()
			}
		}
		return ""
	default:
		return ""
	}
}

// PlacementNodeIDs returns the unique node ID of every vault placement member.
func PlacementNodeIDs(placements []VaultPlacement, nscs []NodeStorageConfig) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, p := range placements {
		nid := NodeIDForStorage(p.StorageID, nscs)
		if nid == "" {
			continue
		}
		if _, ok := seen[nid]; ok {
			continue
		}
		seen[nid] = struct{}{}
		out = append(out, nid)
	}
	return out
}

// LeaderNodeID derives the leader node from placements + storage configs.
func LeaderNodeID(placements []VaultPlacement, nscs []NodeStorageConfig) string {
	storageID := LeaderStorageID(placements)
	if storageID == "" {
		return ""
	}
	return NodeIDForStorage(storageID, nscs)
}

// FollowerNodeIDs derives unique follower node IDs from placements + storage configs.
// Multiple same-node placements are deduplicated. Use FollowerTargets for
// storage-level granularity.
func FollowerNodeIDs(placements []VaultPlacement, nscs []NodeStorageConfig) []string {
	var nodeIDs []string
	seen := make(map[string]bool)
	for _, storageID := range FollowerStorageIDs(placements) {
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
func FollowerTargets(placements []VaultPlacement, nscs []NodeStorageConfig) []ReplicationTarget {
	var targets []ReplicationTarget
	for _, storageID := range FollowerStorageIDs(placements) {
		nid := NodeIDForStorage(storageID, nscs)
		if nid != "" {
			targets = append(targets, ReplicationTarget{NodeID: nid, StorageID: storageID})
		}
	}
	return targets
}
