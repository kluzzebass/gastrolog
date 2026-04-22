package raftgroup

import "gastrolog/internal/glid"

// TierMetadataGroupID returns the multiraft group ID (and on-disk group
// directory name under raft/groups/) for a tier's metadata Raft group.
// Transitional (gastrolog-5xxbd): terminal architecture has no per-tier Raft;
// this naming exists only until tierfsm applies move onto vault control-plane Raft.
func TierMetadataGroupID(vaultID, tierID glid.GLID) string {
	return "vault/" + vaultID.String() + "/tier/" + tierID.String()
}
