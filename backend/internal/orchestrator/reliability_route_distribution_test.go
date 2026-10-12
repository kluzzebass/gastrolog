package orchestrator_test

import (
	"context"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// putSpreadRoute routes every record to dests with the given distribution
// and reloads routing on every node, as the config dispatcher does.
func (h *orchRelHarness) putSpreadRoute(dist system.DistributionMode, dests ...vaultSpec) {
	h.t.Helper()
	ids := make([]glid.GLID, len(dests))
	for i, d := range dests {
		ids[i] = d.id
	}
	if err := h.cfgStore.PutRoute(context.Background(), system.RouteConfig{
		ID: glid.New(), Name: "spread", Enabled: true,
		Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
		Destinations: ids,
		Distribution: dist,
	}); err != nil {
		h.t.Fatalf("PutRoute: %v", err)
	}
	h.reloadRouting()
}

func (h *orchRelHarness) reloadRouting() {
	h.t.Helper()
	for _, id := range h.nodeIDs {
		if err := h.nodes[id].orch.ReloadFilters(context.Background()); err != nil {
			h.t.Fatalf("%s: ReloadFilters: %v", h.nodes[id].label, err)
		}
	}
}

// Failover sends every record to the first destination; once it is
// disabled, every record goes to the next, and none is lost or duplicated.
func TestOrchPipeline_FailoverRouteMovesToTheNextDestination(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	t.Parallel()
	homes := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homes), withExtraVault(homes),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	first, second := h.vaults[1], h.vaults[2]
	h.putSpreadRoute(system.DistributionFailover, first, second)

	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords, "failover-first")
	h.waitSealedRecords(first, h.nodeIDs[0], pipelineChunkMaxRecords)

	disableVault(t, h, first)
	h.reloadRouting()
	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords, "failover-second")
	h.waitSealedRecords(second, h.nodeIDs[0], pipelineChunkMaxRecords)
	h.waitSealedRecords(first, h.nodeIDs[0], pipelineChunkMaxRecords)
}

// Round-robin splits records evenly across the destinations, each record
// stored once.
func TestOrchPipeline_RoundRobinRouteSplitsRecords(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}
	t.Parallel()
	homes := []int{0, 1, 2}
	h := newOrchRelHarness(t, 4,
		withExtraVault(homes), withExtraVault(homes),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	b, c := h.vaults[1], h.vaults[2]
	h.putSpreadRoute(system.DistributionRoundRobin, b, c)

	h.submitIngestRecords(h.nodeIDs[3], 2*pipelineChunkMaxRecords, "round-robin")
	h.waitSealedRecords(b, h.nodeIDs[0], pipelineChunkMaxRecords)
	h.waitSealedRecords(c, h.nodeIDs[0], pipelineChunkMaxRecords)
}
