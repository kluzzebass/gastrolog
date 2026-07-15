package orchestrator_test

// gastrolog-5do8sh: pipeline flow-tail and failure-injection acceptance.
//
// The Rubicon E3 tests (reliability_pipeline_test.go) prove ingest → publish →
// collection → sealed GLCB → query. These tests pin the two remaining legs of
// the segment flow:
//
//   - the RELEASE tail: once every home holds the sealed GLCB and has
//     committed a holder receipt, the vault-ctl leader releases the source
//     segments from the completed-segment registry and every home purges its
//     head/ copies — while the records stay fully queryable from the GLCBs.
//   - failure injection: a follower home that is DOWN while records flow
//     (missing both the collection window and the GLCB build window) must
//     converge after restart with byte-identical GLCBs and full servability,
//     with no operator action.

import (
	"fmt"
	"testing"
	"time"
)

// stageCounterTotals is the cluster-wide sum of the per-vault discrete
// pipeline stage counters, gathered by reading each node's orchestrator (each
// node counts only its own events) — the same sum NodeStats/GetClusterStatus
// produces for the inspector (gastrolog-4r784a).
type stageCounterTotals struct {
	segmentsCompleted uint64
	segmentsPublished uint64
	segmentsReleased  uint64
	chunksPlanned     uint64
	chunksBuilt       uint64
	chunksSealed      uint64
	headPurges        uint64
}

func (h *orchRelHarness) aggregateStageCounters(v vaultSpec) stageCounterTotals {
	h.t.Helper()
	var agg stageCounterTotals
	for _, id := range h.nodeIDs {
		orch := h.nodes[id].orch
		if orch == nil {
			continue
		}
		for _, s := range orch.VaultAppendStats() {
			if s.VaultID == v.id {
				agg.segmentsCompleted += s.SegmentsCompleted
			}
		}
		for _, s := range orch.VaultPublishStats() {
			if s.VaultID == v.id {
				agg.segmentsPublished += s.Published
			}
		}
		for _, s := range orch.VaultChunkStageStats() {
			if s.VaultID == v.id {
				agg.segmentsReleased += s.SegmentsReleased
				agg.chunksPlanned += s.ChunksPlanned
				agg.chunksBuilt += s.ChunksBuilt
				agg.chunksSealed += s.ChunksSealed
				agg.headPurges += s.HeadPurges
			}
		}
	}
	return agg
}

// waitRegistryDrained waits until the vault's completed-segment registry is
// empty on every given node's FSM — i.e. every published segment has been
// released via CmdReleaseSegments after its records reached RF in sealed
// chunks. Progress metric: per-node registry sizes (every release resets the
// stall clock); only a genuine wedge trips the stall.
func (h *orchRelHarness) waitRegistryDrained(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	what := fmt.Sprintf("vault %s: completed-segment registry drain", v.label)
	h.waitProgress(what, 100*time.Millisecond, func() (string, bool) {
		counts := map[string]int{}
		drained := true
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			sub := h.vaultCtlSubFSM(v, nodeID)
			if sub == nil {
				drained = false
				counts[h.nodes[nodeID].label] = -1
				continue
			}
			n := len(sub.ListCompletedSegments())
			counts[h.nodes[nodeID].label] = n
			if n != 0 {
				drained = false
			}
		}
		return fmt.Sprintf("registry_segments=%v", counts), drained
	}, func() { h.dumpPipelineState(v) })
}

// waitHeadDrained waits until head/ holds zero segment files on every given
// home — the purge that follows holder receipts and segment release.
// Progress metric: per-home head/ file counts.
func (h *orchRelHarness) waitHeadDrained(v vaultSpec, homeIdxs []int) {
	h.t.Helper()
	what := fmt.Sprintf("vault %s: head/ purge on homes", v.label)
	h.waitProgress(what, 100*time.Millisecond, func() (string, bool) {
		counts := map[string]int{}
		drained := true
		for _, idx := range homeIdxs {
			nodeID := h.nodeIDs[idx]
			count, err := countHeadSegmentFiles(h.pipelineVaultRoot(nodeID, v))
			if err != nil {
				h.t.Fatalf("vault %s home %s: list head/: %v", v.label, h.nodes[nodeID].label, err)
			}
			counts[h.nodes[nodeID].label] = count
			if count != 0 {
				drained = false
			}
		}
		return fmt.Sprintf("head_files=%v", counts), drained
	}, func() { h.dumpPipelineState(v) })
}

// assertSearchBodiesExactly runs a match-all search on nodeID and asserts the
// result is exactly the given body multiset (each body once, nothing extra,
// nothing missing).
func (h *orchRelHarness) assertSearchBodiesExactly(v vaultSpec, nodeID string, bodies map[string]bool) {
	h.t.Helper()
	got := h.searchRecords(v, nodeID)
	if len(got) != len(bodies) {
		h.t.Fatalf("search on %s returned %d records, want %d", h.nodes[nodeID].label, len(got), len(bodies))
	}
	remaining := make(map[string]bool, len(bodies))
	for b := range bodies {
		remaining[b] = true
	}
	for _, raw := range got {
		body := string(raw)
		if !remaining[body] {
			h.t.Errorf("search on %s returned unexpected or duplicate record %q", h.nodes[nodeID].label, body)
			continue
		}
		delete(remaining, body)
	}
	for body := range remaining {
		h.t.Errorf("search on %s missing ingested record %q", h.nodes[nodeID].label, body)
	}
}

// TestOrchPipeline_ReleaseDrainsRegistryAndHead pins the release tail of the
// segment flow end to end: ingest on a non-home origin → segments completed
// and published to vault-ctl → pulled by every home over the real PullSegment
// transport → sealed GLCBs built and byte-identical on every home → holder
// receipts committed by every home → the leader releases every source segment
// from the completed-segment registry → every home purges its head/ copies —
// and afterwards every ingested record is still served, exactly once, from
// the sealed GLCBs alone.
func TestOrchPipeline_ReleaseDrainsRegistryAndHead(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	homeIdxs := []int{0, 1, 2}
	ingestNode := h.nodeIDs[3] // non-home origin

	const total = 2 * pipelineChunkMaxRecords
	bodies := h.submitIngestRecords(ingestNode, total, "release-tail")

	// Sealed chunks cover every record; GLCBs land byte-identical on homes.
	entries := h.waitSealedRecords(v, h.nodeIDs[0], total)
	if len(entries) != 2 {
		t.Fatalf("expected 2 sealed chunks, got %d", len(entries))
	}
	h.waitGLCBsOnHomes(v, homeIdxs, entries)

	// Every home commits a holder receipt for every sealed chunk — the gate
	// segment release depends on (records are RF-replicated in chunks).
	for _, e := range entries {
		h.waitChunkHolders(v, e.ID, homeIdxs)
	}

	// Release: the completed-segment registry drains to zero on every node
	// (replicated FSM state, so origin included), and head/ purges on every
	// home. Raw segments are gone; the GLCBs are the only copy left.
	h.waitRegistryDrained(v, []int{0, 1, 2, 3})
	h.waitHeadDrained(v, homeIdxs)

	// Record integrity after release: a follower home serves every ingested
	// record exactly once from the sealed GLCBs.
	h.assertSearchBodiesExactly(v, h.nodeIDs[1], bodies)

	// Stage counters end to end (gastrolog-4r784a): the discrete pipeline
	// milestones populated on their owning nodes. Each node counts only its
	// own events, so the cluster picture is the sum across nodes — exactly the
	// aggregation NodeStats/GetClusterStatus performs for the UI. Assert the
	// cluster totals reflect the flow: segments completed+published on the
	// origin, chunks planned/built/sealed and segments released across the
	// homes, and head/ purged.
	agg := h.aggregateStageCounters(v)
	if agg.segmentsCompleted == 0 || agg.segmentsPublished == 0 {
		t.Fatalf("segment stage counters: completed=%d published=%d, want > 0",
			agg.segmentsCompleted, agg.segmentsPublished)
	}
	// Two chunks, three homes each build a GLCB → 6 builds cluster-wide;
	// the leader plans and seals each chunk once.
	if agg.chunksBuilt < 2 || agg.chunksPlanned < 2 || agg.chunksSealed < 2 {
		t.Fatalf("chunk stage counters: planned=%d built=%d sealed=%d, want planned/sealed>=2, built>=2 (3 homes)",
			agg.chunksPlanned, agg.chunksBuilt, agg.chunksSealed)
	}
	if agg.segmentsReleased == 0 {
		t.Fatalf("segments released = 0, want > 0 after registry drain")
	}
	if agg.headPurges == 0 {
		t.Fatalf("head purges = 0, want > 0 after head drain")
	}
}

// TestOrchPipeline_HomeDownDuringIngestCatchesUpOnRestart injects a home
// failure in the middle of the flow: a follower home is stopped, records flow
// while it is down (it misses both the segment collection window and the GLCB
// build window — by the time it returns the source segments may already be
// released), and after restart it must converge: byte-identical GLCBs for
// every sealed chunk, a re-earned holder receipt, and full servability.
func TestOrchPipeline_HomeDownDuringIngestCatchesUpOnRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline acceptance test")
	}

	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	homeIdxs := []int{0, 1, 2}
	ingestNode := h.nodeIDs[3] // non-home origin
	victim := h.nodeIDs[1]     // follower home (placement leader is node-1 at idx 0)

	// Phase 1: one sealed chunk lands on all three homes while healthy.
	preBodies := h.submitIngestRecords(ingestNode, pipelineChunkMaxRecords, "pre-outage")
	first := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	h.waitGLCBsOnHomes(v, homeIdxs, first)

	// Phase 2: the victim home goes down. The vault-ctl group keeps quorum
	// (2 of 3); ingest continues on the origin and the surviving homes carry
	// the chunk to seal without the victim.
	h.stopNode(victim)
	postBodies := h.submitIngestRecords(ingestNode, pipelineChunkMaxRecords, "during-outage")

	const total = 2 * pipelineChunkMaxRecords
	entries := h.waitSealedRecords(v, h.nodeIDs[0], total)
	if len(entries) != 2 {
		t.Fatalf("expected 2 sealed chunks, got %d", len(entries))
	}
	h.waitGLCBsOnHomes(v, []int{0, 2}, entries)

	// Phase 3: the victim restarts with its home dir intact. Vault-ctl FSM
	// replay tells it about the chunk it missed; the catch-up sweep pulls the
	// GLCB from a peer home. Every sealed chunk must end byte-identical
	// across ALL homes, including the one that was down.
	h.startNode(victim)
	h.waitForAllReady()
	h.waitGLCBsOnHomes(v, homeIdxs, entries)

	// Holder receipts converge to the full home set for both chunks —
	// including the victim's re-earned claim on the chunk it never built.
	for _, e := range entries {
		h.waitChunkHolders(v, e.ID, homeIdxs)
	}

	// The recovered home serves every record from both phases exactly once.
	allBodies := make(map[string]bool, total)
	for b := range preBodies {
		allBodies[b] = true
	}
	for b := range postBodies {
		allBodies[b] = true
	}
	h.waitSearchable(v, victim, total)
	h.assertSearchBodiesExactly(v, victim, allBodies)
}
