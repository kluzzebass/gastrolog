package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/chunk"
	"gastrolog/internal/cluster"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// applyOnVaultCtlLeader commits one vault-scoped chunk command through
// whichever node currently leads the vault's vault-ctl group, re-resolving
// the leader when leadership moves between the lookup and the apply.
func (h *orchRelHarness) applyOnVaultCtlLeader(v vaultSpec, chunkCmd []byte) {
	h.t.Helper()
	data := vaultraft.MarshalVaultChunkCommand(v.id, chunkCmd)
	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	var lastErr error
	for range 10 {
		leader := h.waitForVaultCtlLeaderForVault(v)
		g := leader.groupMgr.GetGroup(gid)
		if g == nil {
			lastErr = fmt.Errorf("%s has no vault-ctl group", leader.label)
			continue
		}
		err := g.Raft.Apply(data, cluster.ReplicationTimeout).Error()
		if err == nil {
			return
		}
		if !errors.Is(err, hraft.ErrNotLeader) && !errors.Is(err, hraft.ErrLeadershipLost) &&
			!errors.Is(err, hraft.ErrLeadershipTransferInProgress) {
			h.t.Fatalf("vault-ctl apply on %s: %v", leader.label, err)
		}
		lastErr = err
	}
	h.t.Fatalf("vault-ctl apply never landed on a stable leader: %v", lastErr)
}

// The placement leader's pipeline re-registration fails (its segmentation
// root is damaged) on a 4-node cluster whose manifest holds sealed chunks far
// past any grace period with no bytes on the leader. Reconcile passes on
// every node must not retire a single one of them: the vault is a pipeline
// vault by configuration, whatever the leader's registration says.
func TestOrchPipeline_FailedLeaderReregistrationRetiresNoChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	h := newOrchRelHarness(t, 4,
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	ctx := context.Background()
	v := h.vaults[0]
	placementLeader := h.nodes[h.nodeIDs[v.nodeIdxs[0]]]

	old := time.Now().Add(-48 * time.Hour)
	ids := make([]chunk.ChunkID, 3)
	for i := range ids {
		ids[i] = chunk.NewChunkID()
		h.applyOnVaultCtlLeader(v, vaultctlfsm.MarshalCreateChunk(ids[i], old, old, old))
		h.applyOnVaultCtlLeader(v, vaultctlfsm.MarshalSealChunk(ids[i], old, 1, 1, old, old, old, false, old))
	}
	h.waitProgress("aged sealed entries replicated to every node", 30*time.Millisecond, func() (string, bool) {
		var views []string
		all := true
		for _, id := range h.nodeIDs {
			sub := h.vaultCtlSubFSM(v, id)
			have := 0
			for _, cid := range ids {
				if sub != nil && sub.Get(cid) != nil && sub.Get(cid).IsSealed() {
					have++
				}
			}
			views = append(views, fmt.Sprintf("%s=%d", h.nodes[id].label, have))
			all = all && have == len(ids)
		}
		return fmt.Sprintf("%v", views), all
	}, nil)

	working := paths.WorkingDir(h.pipelineVaultRoot(placementLeader.id, v))
	if err := os.RemoveAll(working); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(working, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	// A rotation policy edit changes every home's wanted registration, so the
	// next reload re-registers the vault — and on the placement leader the
	// supervisor refuses it.
	maxRecords := int64(pipelineChunkMaxRecords + 1)
	if err := h.cfgStore.PutRotationPolicy(ctx, system.RotationPolicyConfig{
		ID:         *h.rotationPolicyID,
		Name:       "orch-rel-pipeline-rotation",
		MaxRecords: &maxRecords,
	}); err != nil {
		t.Fatalf("PutRotationPolicy: %v", err)
	}
	if err := placementLeader.orch.ReloadFilters(ctx); err == nil {
		t.Fatalf("premise: re-registration on %s onto a damaged segmentation root must fail", placementLeader.label)
	}

	for _, id := range h.nodeIDs {
		inst := h.nodes[id].orch.FindLocalVaultInstance(v.id)
		if inst == nil || inst.Reconciler == nil {
			t.Fatalf("%s has no vault instance reconciler", h.nodes[id].label)
		}
		inst.Reconciler.ReconcileTick()
		inst.Reconciler.ReconcileMembershipCatchup()
	}

	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		sub := h.vaultCtlSubFSM(v, id)
		if sub == nil {
			t.Fatalf("%s has no vault-ctl manifest", n.label)
		}
		for _, cid := range ids {
			if sub.Get(cid) == nil {
				t.Errorf("%s: chunk %s was retired from the manifest", n.label, cid)
			}
			if pd := sub.PendingDelete(cid); pd != nil {
				t.Errorf("%s: chunk %s has a pending delete (reason %q)", n.label, cid, pd.Reason)
			}
		}
	}
}
