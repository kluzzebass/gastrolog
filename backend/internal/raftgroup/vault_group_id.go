package raftgroup

import "gastrolog/internal/glid"

// VaultControlPlaneGroupID is the multiraft group ID for a vault's
// control-plane Raft replica set. Uses suffix "/ctl" so snapshot paths stay
// disjoint from tier metadata groups under vault/<id>/tier/<tierGLID>.
func VaultControlPlaneGroupID(vaultID glid.GLID) string {
	return "vault/" + vaultID.String() + "/ctl"
}
