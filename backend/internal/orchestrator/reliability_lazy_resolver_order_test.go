package orchestrator_test

// A pipeline vault's sealed GLCBs become searchable on a home through a
// resolver and lister installed on the vault instance's chunk manager. The
// background pipeline-config-reconcile pass re-registers a vault whenever its
// registration key changes — and a placement change makes the key change on
// a joining home BEFORE that home's instance exists (the shared config is
// written first; the per-node instance build follows). A reload landing in
// that window must not consume the change without installing anything:
// otherwise the instance attached afterwards never gets the resolver, every
// later reload sees an unchanged key, and the joined home answers a
// match-all search with nothing — forever — while holding the bytes.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

func TestOrchPipeline_ReloadBeforeInstanceAttachStillServesTheJoinedHome(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	joiner := h.nodeIDs[3]
	ctx := context.Background()

	h.submitIngestRecords(joiner, pipelineChunkMaxRecords, "pre-churn")
	first := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	h.waitGLCBsOnHomes(v, []int{0, 1, 2}, first)

	// Churn: node-3 leaves the home set, node-4 joins. The shared config
	// changes first.
	newHomes := []int{0, 1, 3}
	h.setVaultPlacements(v, newHomes)

	// The window: node-4's background reload runs after the config change
	// and before node-4's instance is built — exactly what a
	// pipeline-config-reconcile tick does when it lands there.
	jn := h.nodes[joiner]
	if err := jn.orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("early reload on joiner: %v", err)
	}

	// Then the dispatcher-equivalent fan-out, in production order.
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if id == joiner {
			if err := n.orch.AddVaultInstance(ctx, v.id, n.factories); err != nil {
				t.Fatalf("AddVaultInstance on %s: %v", n.label, err)
			}
		}
		if id == h.nodeIDs[2] {
			n.orch.RemoveVaultInstance(v.id)
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
	}

	h.submitIngestRecords(joiner, pipelineChunkMaxRecords, "post-churn")
	h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)

	h.waitProgress("post-churn records searchable on joined home", 50*time.Millisecond, func() (string, bool) {
		postChurn := 0
		for _, raw := range h.searchRecords(v, joiner) {
			if bytes.HasPrefix(raw, []byte("post-churn-")) {
				postChurn++
			}
		}
		return fmt.Sprintf("post_churn_records=%d/%d", postChurn, pipelineChunkMaxRecords),
			postChurn == pipelineChunkMaxRecords
	}, func() { h.dumpPipelineState(v) })
}
