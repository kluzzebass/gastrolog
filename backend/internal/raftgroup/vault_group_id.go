package raftgroup

import "gastrolog/internal/glid"

// VaultControlPlaneGroupID is the multiraft group ID for a vault's
// control-plane Raft replica set (replicated tier chunk metadata via OpTierFSM).
func VaultControlPlaneGroupID(vaultID glid.GLID) string {
	return "vault/" + vaultID.String() + "/ctl"
}
