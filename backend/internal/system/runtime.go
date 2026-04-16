package system

import "gastrolog/internal/glid"

// Runtime holds cluster-managed state — things the system controls
// autonomously, not the operator. Replicated via the system Raft group
// alongside Config, but logically separate: an operator never edits
// Runtime directly.
type Runtime struct {
	// Cluster membership: nodes and their storage.
	Nodes              []NodeConfig        `json:"nodes,omitempty"`
	NodeStorageConfigs []NodeStorageConfig `json:"nodeStorageConfigs,omitempty"`

	// Tier placements: which storages hold replicas, and who leads.
	// Keyed by tier ID. The operator sets replication factor and storage
	// class on TierConfig; the system decides placements.
	TierPlacements map[glid.GLID][]TierPlacement `json:"tierPlacements,omitempty"`

	// Cluster identity material (generated at cluster-init).
	ClusterTLS *ClusterTLS `json:"cluster_tls,omitempty"`

	// Ingester running state: ingesterID → (nodeID → alive).
	// Updated by each node as ingesters start/stop/fail.
	IngesterAlive map[glid.GLID]map[string]bool `json:"ingesterAlive,omitempty"`

	// Ingester checkpoints: ingesterID → opaque blob.
	// Written by the running node, read on failover by the new node.
	IngesterCheckpoints map[glid.GLID][]byte `json:"ingesterCheckpoints,omitempty"`

	// Active ingester assignments: ingesterID → assigned nodeID.
	// Set by the Raft leader's placement manager. Only the assigned node
	// starts the ingester. Empty means unassigned.
	IngesterAssignment map[glid.GLID]string `json:"ingesterAssignment,omitempty"`

	// UI state.
	SetupWizardDismissed bool `json:"setup_wizard_dismissed,omitempty"`
}
