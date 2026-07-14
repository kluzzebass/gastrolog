package orchestrator_test

import (
	"context"
	"gastrolog/internal/query"
	"testing"
	"time"
)

// TestOrchPipeline_LazyResolutionServesAfterRestart pins the restart-survival
// contract of lazy external-GLCB resolution (registration as a cache): a home
// node restarted after chunks sealed serves them the moment its FSM and the
// on-disk GLCBs are back — no registration sweep, no warm-up window, no
// per-chunk boot work. The incident this guards against: a restarted node
// spent 16 minutes answering 'chunk not found' for hundreds of chunks it
// held complete on disk, because eager registration raced FSM replay and
// then waited on sweep timing.
func TestOrchPipeline_LazyResolutionServesAfterRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]

	h.submitIngestRecords(h.nodeIDs[3], pipelineChunkMaxRecords, "lazy-restart")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	h.waitGLCBsOnHomes(v, []int{0, 1, 2}, entries)

	// Restart a follower home. Its home dir (FSM state, GLCBs) persists;
	// its in-memory chunk registrations die with the process.
	victim := h.nodeIDs[1]
	h.stopNode(victim)
	h.startNode(victim)
	h.waitForAllReady()

	// The restarted home serves every record through lazy resolution at
	// first lookup. Poll tolerantly: instance construction (ApplyConfig)
	// races this loop right after start, and "vault not ready" is a
	// legitimate transient — the assertion is servability once ready,
	// with NO registration pass in between.
	deadline := time.Now().Add(orchHarnessConvWait)
	var got int
	var lastErr error
	for time.Now().Before(deadline) {
		got, lastErr = countSearchable(h, v, victim)
		if lastErr == nil && got == pipelineChunkMaxRecords {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("restarted home served %d/%d records (last error: %v) — sealed on-disk chunks did not lazily resolve",
		got, pipelineChunkMaxRecords, lastErr)
}

// countSearchable is a fatal-free variant of searchRecords for polling a
// node through its post-restart convergence window.
func countSearchable(h *orchRelHarness, v vaultSpec, nodeID string) (int, error) {
	n := h.nodes[nodeID]
	seq, _, err := n.orch.Search(context.Background(), v.id, query.Query{}, nil)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, err := range seq {
		if err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}
