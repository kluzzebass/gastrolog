package raftgroup

import "gastrolog/internal/glid"

// TierMetadataGroupID returns the multiraft group ID (and on-disk group
// directory name under raft/groups/) for a tier's metadata Raft group.
// Vault-scoped so group routing and storage align with the control-plane
// model (gastrolog-5xxbd).
func TierMetadataGroupID(vaultID, tierID glid.GLID) string {
	return "vault/" + vaultID.String() + "/tier/" + tierID.String()
}
