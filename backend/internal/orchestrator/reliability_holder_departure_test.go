package orchestrator_test

import (
	"context"
	"testing"

	"gastrolog/internal/system"
)

// TestOrchPipeline_RePlacedHomeLosesHolderClaim is the dead-disk re-placement
// scenario: a vault on homes {1,2,3} seals a chunk every home holds; node-3's
// disk dies and the vault is re-placed to {1,2,4}. Node-3 never runs a
// reconciler again, so nothing of its own revokes its holder claim — the
// placement leader's outside-placement prune must, and the holder set must
// converge on the new home set once node-4 catches the chunk up.
func TestOrchPipeline_RePlacedHomeLosesHolderClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	ctx := context.Background()

	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords, "pre-departure")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk, got %d", len(entries))
	}
	h.waitGLCBsOnHomes(v, []int{0, 1, 2}, entries)
	h.waitChunkHolders(v, entries[0].ID, []int{0, 1, 2})

	h.stopNode(h.nodeIDs[2])
	newHomes := []int{0, 1, 3}
	h.setVaultPlacements(v, newHomes)
	if err := h.nodes[h.nodeIDs[3]].orch.AddVaultInstance(ctx, v.id, h.nodes[h.nodeIDs[3]].factories); err != nil {
		t.Fatalf("AddVaultInstance on node-4: %v", err)
	}
	for _, idx := range newHomes {
		n := h.nodes[h.nodeIDs[idx]]
		if err := n.orch.ReloadFilters(ctx); err != nil {
			t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
		n.orch.ReconcileVaultPlacement(ctx, v.id)
	}

	h.waitChunkHolders(v, entries[0].ID, newHomes)
}

// TestOrchPipeline_ClusterDepartureDropsHolderClaim: a home removed from the
// cluster while still named in the vault's placement loses its holder claim
// when the vault-ctl leader removes it from the voter set. The placement is
// left unchanged so the outside-placement prune cannot be what drops it.
func TestOrchPipeline_ClusterDepartureDropsHolderClaim(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	ctx := context.Background()

	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords, "pre-removal")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk, got %d", len(entries))
	}
	h.waitChunkHolders(v, entries[0].ID, []int{0, 1, 2})

	departed := h.nodeIDs[2]
	h.stopNode(departed)
	nodes, err := h.cfgStore.ListNodes(ctx)
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	var remaining []system.NodeConfig
	for _, n := range nodes {
		if n.ID.String() != departed {
			remaining = append(remaining, n)
		}
	}
	for _, idx := range []int{0, 1, 3} {
		n := h.nodes[h.nodeIDs[idx]]
		n.orch.RefreshVaultCtlMembers(remaining, n.factories)
	}

	h.waitChunkHolders(v, entries[0].ID, []int{0, 1})
}
