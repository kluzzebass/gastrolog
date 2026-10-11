package orchestrator_test

// Multi-node coverage for a vault-ctl group whose creation deferred on one
// node: the rest of the cluster forms the group, the deferred node builds its
// instance without it, and the group appears there only when the missing
// peer resolves. From then on the node must act on the replicated manifest —
// a delete obligation naming it must be acked by it, or retention stalls
// cluster-wide.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestOrchRel_DeferredVaultCtlGroupBindsInstanceWhenPeerResolves(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node vault-ctl convergence test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4, withVaultCtlPeerHidden(3, 0))
	v := h.vaults[0]
	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	deferred := h.nodes[h.nodeIDs[3]]

	if deferred.groupMgr.GetGroup(gid) != nil {
		t.Fatal("precondition: the node that cannot resolve a peer deferred group creation")
	}
	if deferred.orch.FindLocalVaultInstance(v.id) == nil {
		t.Fatal("precondition: the deferred node built its local instance")
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := deferred.orch.WaitVaultReady(cancelled, v.id); err == nil {
		t.Fatal("the deferred node reports the vault ready while its vault-ctl group does not exist")
	}

	h.resolveVaultCtlPeers(deferred.id)
	if deferred.groupMgr.GetGroup(gid) == nil {
		t.Fatal("the group was not created once every peer resolved")
	}
	h.waitForAllReady()

	leader := h.waitForVaultCtlLeaderForVault(v)
	chunkID := chunk.NewChunkID()
	cmd := vaultraft.MarshalVaultChunkCommand(v.id,
		vaultctlfsm.MarshalRequestDelete(chunkID, time.Now(), "test", []string{deferred.id}))
	if err := leader.groupMgr.GetGroup(gid).Raft.Apply(cmd, 5*time.Second).Error(); err != nil {
		t.Fatalf("propose request-delete: %v", err)
	}

	h.waitProgress("the deferred node acks the delete obligation naming it", 50*time.Millisecond, func() (string, bool) {
		var views []string
		done := true
		for _, id := range h.nodeIDs {
			acks := h.nodes[id].orch.PendingDeleteAcks(v.id)
			if expected, pending := acks[chunkID]; pending {
				done = false
				views = append(views, fmt.Sprintf("%s:%v", h.nodes[id].label, expected))
			}
		}
		return fmt.Sprintf("pending=%v", views), done
	}, nil)
}
