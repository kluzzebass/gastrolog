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
	"testing"
	"time"
)

// waitRegistryDrained polls until the vault's completed-segment registry is
// empty on every given node's FSM — i.e. every published segment has been
// released via CmdReleaseSegments after its records reached RF in sealed
// chunks. Uses the shared coarse backstop; only a genuine wedge trips it.
func (h *orchRelHarness) waitRegistryDrained(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	var lastCounts map[string]int
	for time.Now().Before(deadline) {
		lastCounts = map[string]int{}
		drained := true
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			sub := h.vaultCtlSubFSM(v, nodeID)
			if sub == nil {
				drained = false
				lastCounts[h.nodes[nodeID].label] = -1
				continue
			}
			n := len(sub.ListCompletedSegments())
			lastCounts[h.nodes[nodeID].label] = n
			if n != 0 {
				drained = false
			}
		}
		if drained {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.dumpPipelineState(v)
	h.t.Fatalf("vault %s: completed-segment registry never drained within %s (remaining per node: %v)",
		v.label, orchHarnessConvWait, lastCounts)
}

// waitHeadDrained polls until head/ holds zero segment files on every given
// home — the purge that follows holder receipts and segment release.
func (h *orchRelHarness) waitHeadDrained(v vaultSpec, homeIdxs []int) {
	h.t.Helper()
	deadline := time.Now().Add(orchHarnessConvWait)
	var lastCounts map[string]int
	for time.Now().Before(deadline) {
		lastCounts = map[string]int{}
		drained := true
		for _, idx := range homeIdxs {
			nodeID := h.nodeIDs[idx]
			count, err := countHeadSegmentFiles(h.pipelineVaultRoot(nodeID, v))
			if err != nil {
				h.t.Fatalf("vault %s home %s: list head/: %v", v.label, h.nodes[nodeID].label, err)
			}
			lastCounts[h.nodes[nodeID].label] = count
			if count != 0 {
				drained = false
			}
		}
		if drained {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.dumpPipelineState(v)
	h.t.Fatalf("vault %s: head/ never drained on homes within %s (remaining per home: %v)",
		v.label, orchHarnessConvWait, lastCounts)
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
	deadline := time.Now().Add(orchHarnessConvWait)
	for time.Now().Before(deadline) {
		if n, err := countSearchable(h, v, victim); err == nil && n == total {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.assertSearchBodiesExactly(v, victim, allBodies)
}
