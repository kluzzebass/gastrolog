package orchestrator

import (
	"errors"
	"fmt"

	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"

	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// ErrVaultCtlRaftUnavailable is returned when ApplyVaultControlPlane is called
// but no cluster GroupManager was wired (single-node / tests without raft).
var ErrVaultCtlRaftUnavailable = errors.New("vault control-plane raft: group manager not configured")

// ApplyVaultControlPlane applies a marshaled vault control-plane FSM command
// for the given vault. Uses VaultApplyForwarder when PeerConns is configured
// so followers forward to the vault Raft leader; otherwise applies locally.
func (o *Orchestrator) ApplyVaultControlPlane(vaultID glid.GLID, data []byte) error {
	if o.groupMgr == nil {
		return ErrVaultCtlRaftUnavailable
	}
	gid := raftgroup.VaultControlPlaneGroupID(vaultID)
	g := o.groupMgr.GetGroup(gid)
	if g == nil {
		return fmt.Errorf("vault control-plane raft group %q not running on this node", gid)
	}
	if o.peerConns == nil {
		future := g.Raft.Apply(data, cluster.ReplicationTimeout)
		if err := future.Error(); err != nil {
			return err
		}
		// FSM-returned errors come through future.Response(); .Error()
		// only catches Raft-level failures. See gastrolog-3sr88.
		if resp := future.Response(); resp != nil {
			if err, ok := resp.(error); ok {
				return err
			}
		}
		return nil
	}
	fwd := cluster.NewVaultApplyForwarder(g.Raft, gid, o.peerConns, cluster.ReplicationTimeout)
	return fwd.Apply(data)
}

// vaultCtlApplier implements vaultctlfsm.Applier by wrapping vault commands
// as vault control-plane OpVaultChunkFSM entries (see vaultraft.MarshalVaultChunkCommand).
type vaultCtlApplier struct {
	o       *Orchestrator
	vaultID glid.GLID
}

func (a *vaultCtlApplier) Apply(data []byte) error {
	return a.o.ApplyVaultControlPlane(a.vaultID, vaultraft.MarshalVaultChunkCommand(a.vaultID, data))
}

var _ vaultctlfsm.Applier = (*vaultCtlApplier)(nil)
