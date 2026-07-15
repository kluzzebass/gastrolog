package raftgroup

import (
	"strings"

	"gastrolog/internal/glid"
)

// VaultControlPlaneGroupID is the multiraft group ID for a vault's
// control-plane Raft replica set (replicated chunk metadata via OpVaultChunkFSM).
func VaultControlPlaneGroupID(vaultID glid.GLID) string {
	return "vault/" + vaultID.String() + "/ctl"
}

// ClusterControlPlaneGroupID is the Raft group ID for cluster-wide config state.
const ClusterControlPlaneGroupID = "cluster-ctl"

// IsVaultControlPlaneGroupID reports whether groupID is a per-vault control-plane
// Raft group (vault/<id>/ctl).
func IsVaultControlPlaneGroupID(groupID string) bool {
	return strings.HasPrefix(groupID, "vault/") && strings.HasSuffix(groupID, "/ctl")
}
