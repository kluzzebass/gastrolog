package orchestrator_test

import (
	"fmt"
	"testing"
	"time"
)

// Every home of a pipeline vault builds its own copy of each sealed chunk, and
// any of them can become the vault leader that serves searches. Each home
// therefore indexes the copy it built — the leader and every follower alike —
// so a leadership move never leaves the new leader scanning unindexed chunks.
func TestOrchRel_PipelineVault_EveryHomeIndexesItsSealedChunks(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2, 3}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	enableVault(t, h, v)

	h.submitIngestRecords(h.nodeIDs[0], 2*pipelineChunkMaxRecords, "every-home-indexes")
	sealed := h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	if len(sealed) == 0 {
		t.Fatal("premise: no sealed pipeline chunk to check")
	}

	followers := 0
	for _, id := range h.nodeIDs {
		inst := h.nodes[id].orch.FindLocalVaultInstance(v.id)
		if inst == nil {
			t.Fatalf("%s: no local instance of vault %s", h.nodes[id].label, v.label)
		}
		if inst.IsFollower {
			followers++
		}
	}
	if followers == 0 {
		t.Fatal("premise: every home is the vault leader, so the follower path is not exercised")
	}

	h.waitProgress("every home indexing every sealed chunk it built", 50*time.Millisecond,
		func() (string, bool) {
			var views []string
			all := true
			for _, id := range h.nodeIDs {
				inst := h.nodes[id].orch.FindLocalVaultInstance(v.id)
				done := 0
				for _, e := range sealed {
					if complete, err := inst.Indexes.IndexesComplete(e.ID); err == nil && complete {
						done++
					}
				}
				if done != len(sealed) {
					all = false
				}
				role := "leader"
				if inst.IsFollower {
					role = "follower"
				}
				views = append(views, fmt.Sprintf("%s(%s)=%d/%d", h.nodes[id].label, role, done, len(sealed)))
			}
			return fmt.Sprintf("%v", views), all
		}, nil)
}
