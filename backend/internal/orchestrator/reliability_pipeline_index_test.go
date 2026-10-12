package orchestrator_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/vaultraft/vaultctlfsm"
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

// indexFilesPerChunk is the number of secondary index files a complete chunk
// has on disk: token, three attr, three kv, and json.
const indexFilesPerChunk = 8

// waitIndexesOnHomes blocks until every given home has all secondary index
// files for every given chunk. Progress metric: the count of (chunk, home)
// pairs that are complete.
func (h *orchRelHarness) waitIndexesOnHomes(v vaultSpec, homeIdxs []int, entries []vaultctlfsm.ManifestEntry) {
	h.t.Helper()
	total := len(entries) * len(homeIdxs)
	what := fmt.Sprintf("vault %s: secondary indexes for %d chunks on %d homes", v.label, len(entries), len(homeIdxs))
	h.waitProgress(what, 50*time.Millisecond, func() (string, bool) {
		done := 0
		var missing []string
		for _, idx := range homeIdxs {
			n := h.nodes[h.nodeIDs[idx]]
			vi := n.orch.FindLocalVaultInstance(v.id)
			for _, e := range entries {
				files := 0
				if vi != nil && vi.Indexes != nil {
					files = len(vi.Indexes.IndexSizes(e.ID))
				}
				if files == indexFilesPerChunk {
					done++
					continue
				}
				missing = append(missing, fmt.Sprintf("%s@%s=%d/%d", e.ID, n.label, files, indexFilesPerChunk))
			}
		}
		return fmt.Sprintf("indexed=%d/%d missing=%v", done, total, missing), done == total
	}, func() { h.dumpPipelineState(v) })
}

// reloadPlacementOnLiveNodes rewrites a vault's placement and replays the
// config fan-out on every running node, as the dispatcher does.
func (h *orchRelHarness) reloadPlacementOnLiveNodes(v vaultSpec, homeIdxs []int) {
	h.t.Helper()
	h.setVaultPlacements(v, homeIdxs)
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if n.orch == nil {
			continue
		}
		if err := n.orch.ReloadFilters(context.Background()); err != nil {
			h.t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
	}
}

// A home that is down through a chunk's build and its source segments'
// release has no segments to build from when it returns: the replica catch-up
// pull is its only way back to holding the chunk, and the pulled copy is
// indexed like one it built.
func TestOrchRel_PipelineVault_HomeDownThroughReleaseIndexesItsPulledCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	ingestNode := h.nodeIDs[3]
	victimIdx := 2
	victim := h.nodeIDs[victimIdx]

	// The victim goes down before the chunk exists and leaves the placement,
	// so the surviving homes' receipts are enough to release the segments.
	h.stopNode(victim)
	h.reloadPlacementOnLiveNodes(v, []int{0, 1})
	h.submitIngestRecords(ingestNode, pipelineChunkMaxRecords, "built-while-down")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	if len(entries) != 1 {
		t.Fatalf("expected 1 sealed chunk, got %d", len(entries))
	}
	h.waitGLCBsOnHomes(v, []int{0, 1}, entries)
	h.waitChunkHolders(v, entries[0].ID, []int{0, 1})
	h.waitRegistryDrained(v, []int{0, 1, 3})
	if sealed := h.vaultCtlSubFSM(v, h.nodeIDs[0]).SealedChunkManifest(entries[0].ID); sealed != nil {
		t.Fatal("premise: releasing the segments drops the sealed manifest, so the victim cannot build the chunk")
	}

	// Back in the placement and back up: the pull lands the copy.
	h.reloadPlacementOnLiveNodes(v, []int{0, 1, victimIdx})
	h.startNode(victim)
	h.waitForAllReady()
	h.waitGLCBsOnHomes(v, []int{0, 1, victimIdx}, entries)
	h.waitIndexesOnHomes(v, []int{0, 1, victimIdx}, entries)
}

// A home restarting with copies whose secondary indexes were never written —
// the process died between the build and the index build — gets them back at
// boot. Its seals are in the vault-ctl log rather than a snapshot, so the
// restart replays them.
func TestOrchRel_PipelineVault_RestartIndexesCopiesMissingIndexes(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	homeIdxs := []int{0, 1, 2}
	victimIdx := 1
	victim := h.nodeIDs[victimIdx]

	h.submitIngestRecords(h.nodeIDs[3], 2*pipelineChunkMaxRecords, "restart-indexes")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], 2*pipelineChunkMaxRecords)
	if len(entries) == 0 {
		t.Fatal("premise: no sealed pipeline chunk to check")
	}
	h.waitIndexesOnHomes(v, homeIdxs, entries)

	inst := h.nodes[victim].orch.FindLocalVaultInstance(v.id)
	h.stopNode(victim)
	for _, e := range entries {
		if err := inst.Indexes.DeleteIndexes(e.ID); err != nil {
			t.Fatalf("delete victim indexes for %s: %v", e.ID, err)
		}
		if n := len(inst.Indexes.IndexSizes(e.ID)); n != 0 {
			t.Fatalf("premise: victim index files for %s removed, %d left", e.ID, n)
		}
	}

	h.startNode(victim)
	h.waitForAllReady()
	h.waitIndexesOnHomes(v, []int{victimIdx}, entries)
}
