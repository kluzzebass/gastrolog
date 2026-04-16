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

	// UI state.
	SetupWizardDismissed bool `json:"setup_wizard_dismissed,omitempty"`
}
