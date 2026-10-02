package orchestrator_test

// A fresh joiner boots before its config replicates: ApplyConfig runs with
// nil config and the dispatcher replays the store's vaults and routes
// afterwards. The replay restores config state but never re-runs
// ApplyConfig's wiring, so everything config-independent must land on the
// nil-config call. If it does not, the joiner's vault-ctl handle never
// materializes: its segment publishes are refused fail-closed forever,
// chunking waits for second copies that cannot be announced, and records
// ingested on that node stay invisible to the cluster.

import (
	"context"
	"testing"

	"gastrolog/internal/system"
)

func TestOrchPipeline_NilConfigBootStillPublishesAndChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withMatchAllRoute(0),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
		withNilConfigBoot(3),
	)
	ctx := context.Background()
	vA := h.vaults[0]
	joiner := h.nodeIDs[3]

	// The dispatcher-replay equivalent: the joiner receives the stored vault
	// and route config as individual puts after its nil-config boot.
	sys, err := h.cfgStore.Load(ctx)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var vaultCfg system.VaultConfig
	for i := range sys.Config.Vaults {
		if sys.Config.Vaults[i].ID == vA.id {
			vaultCfg = sys.Config.Vaults[i]
		}
	}
	n := h.nodes[joiner]
	if err := n.orch.AddVault(ctx, vaultCfg, n.factories); err != nil {
		t.Fatalf("AddVault on joiner: %v", err)
	}
	if err := n.orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters on joiner: %v", err)
	}

	// Records ingested ON the joiner must publish to the registry, chunk on
	// the homes, and come back queryable — the full pipeline, driven from
	// the one node whose boot had no config.
	const total = 2 * pipelineChunkMaxRecords
	h.submitIngestRecords(joiner, total, "joiner-boot")
	h.waitSealedRecords(vA, h.nodeIDs[0], total)
	h.waitSearchable(vA, h.nodeIDs[1], total)
}
