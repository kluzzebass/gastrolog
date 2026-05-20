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
}

// NodeStorageConfig defines the file storages for a specific cluster node.
type NodeStorageConfig struct {
	NodeID       string        `json:"nodeId"`
	FileStorages []FileStorage `json:"fileStorages"`
}

// CloudService defines a cluster-wide cloud storage endpoint.
// CloudStorageTransition defines a single step in an archival lifecycle chain.
type CloudStorageTransition struct {
	After        string `json:"after"`        // duration string (e.g. "30s", "7d", "2w", "360d")
	StorageClass string `json:"storageClass"` // empty = delete (expiry)
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
	RestoreSpeed       string                   `json:"restoreSpeed,omitempty"`       // default restore speed
	RestoreDays       uint32                   `json:"restoreDays,omitempty"`       // S3 restore window
	SuspectGraceDays  uint32                   `json:"suspectGraceDays,omitempty"`  // default 7
	ReconcileSchedule string                   `json:"reconcileSchedule,omitempty"` // default "0 3 * * *"
}

// VaultType identifies the storage medium for a vault.
//
// "cloud" is no longer a distinct type: a cloud-backed vault is a file vault
// with CloudServiceID set, exposed via VaultConfig.IsCloud(). Step 8 of the
// chunk redesign collapsed the parallel cloud/file dispatch into a single
// file path that flips behavior based on whether a cloud store is wired.
// See gastrolog-4k5mg.
type VaultType string

const (
	VaultTypeMemory VaultType = "memory"
	VaultTypeFile   VaultType = "file"
	VaultTypeJSONL  VaultType = "jsonl"
)

// VaultPlacement assigns one replica of a vault to a specific file storage.
// The node is derived from the file storage's NodeStorageConfig. Under the
// fan-out data plane (gastrolog-hshgl) every placement member is symmetric:
// the legacy Leader bool that distinguished one canonical writer is gone,
// and routing-layer "pick a canonical member" decisions take the first
// placement deterministically.
type VaultPlacement struct {
	StorageID string `json:"storageId"`
}

// LeaderStorageID returns the storage ID of the first placement, or empty
// if unplaced. Under fan-out this is the "deterministic canonical" used
// by routing-layer code that needs to pick one placement member as a
// stable target — every other placement member is an equally valid
// alternative.
func LeaderStorageID(placements []VaultPlacement) string {
	if len(placements) == 0 {
		return ""
	}
	return placements[0].StorageID
}

// FollowerStorageIDs returns the storage IDs of every placement member
// except the first. Under fan-out the "leader" / "follower" distinction
// is gone — these are just "the other placement members" for symmetric
// peer enumeration.
func FollowerStorageIDs(placements []VaultPlacement) []string {
	if len(placements) <= 1 {
		return nil
	}
	ids := make([]string, 0, len(placements)-1)
	for _, p := range placements[1:] {
		ids = append(ids, p.StorageID)
	}
	return ids
}

// StorageIDs returns every placement storage ID in source order.
func StorageIDs(placements []VaultPlacement) []string {
	ids := make([]string, 0, len(placements))
	for _, p := range placements {
		ids = append(ids, p.StorageID)
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
// For file/cloud vaults, matches the required storage class.
// Returns a synthetic storage ID for memory vaults on nodes without matching file storages.
func StorageIDForNode(nodeID string, v VaultConfig, nscs []NodeStorageConfig) string {
	idx := slices.IndexFunc(nscs, func(n NodeStorageConfig) bool { return n.NodeID == nodeID })
	if idx < 0 {
		// Node has no storage config — use synthetic storage ID.
		return SyntheticStorageID(nodeID)
	}

	nsc := nscs[idx]
	var requiredClass uint32
	switch v.Type {
	case VaultTypeFile:
		// Single storage class for all file vaults (local-only and
		// cloud-backed alike). After step 7k, the active chunk and
		// the warm cache live at the same path under chunkDir, so
		// distinguishing "active" and "cache" classes serves no
		// purpose. See gastrolog-4k5mg.
		requiredClass = v.StorageClass
	case VaultTypeMemory, VaultTypeJSONL:
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

// PlacementNodeIDs returns every unique node ID across the vault's placements.
// Under fan-out, every placement member is a peer Receiver — no leader/follower
// distinction. Order matches the placement order with the leader (if any)
// listed first, then followers; duplicates removed.
func PlacementNodeIDs(placements []VaultPlacement, nscs []NodeStorageConfig) []string {
	var nodeIDs []string
	seen := make(map[string]bool)
	for _, storageID := range StorageIDs(placements) {
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
