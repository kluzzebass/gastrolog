package orchestrator_test

import (
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/pipeline/paths"
)

func countCompletedSegmentFiles(root string) (int, error) {
	ids, err := paths.ListSegmentIDs(paths.CompletedDir(root))
	if err != nil {
		return 0, err
	}
	return len(ids), nil
}

// waitCompletedDrained waits until completed/ holds zero segment files on
// every given node — the purge each origin performs once vault-ctl releases
// the segments it originated. Progress metric: per-node completed/ counts.
func (h *orchRelHarness) waitCompletedDrained(v vaultSpec, nodeIdxs []int) {
	h.t.Helper()
	what := fmt.Sprintf("vault %s: completed/ purge on origins", v.label)
	h.waitProgress(what, 100*time.Millisecond, func() (string, bool) {
		counts := map[string]int{}
		drained := true
		for _, idx := range nodeIdxs {
			nodeID := h.nodeIDs[idx]
			count, err := countCompletedSegmentFiles(h.pipelineVaultRoot(nodeID, v))
			if err != nil {
				h.t.Fatalf("vault %s node %s: list completed/: %v", v.label, h.nodes[nodeID].label, err)
			}
			counts[h.nodes[nodeID].label] = count
			if count != 0 {
				drained = false
			}
		}
		return fmt.Sprintf("completed_files=%v", counts), drained
	}, func() { h.dumpPipelineState(v) })
}

// waitOriginSegmentsHeld waits until the completed-segment registry (read on
// observerIdx) holds segments originated by originID covering wantRecords
// records, each with a holder receipt from every home — every home has pulled
// the origin's bytes, so the origin is no longer needed for them.
func (h *orchRelHarness) waitOriginSegmentsHeld(v vaultSpec, observerIdx int, originID string, homeIdxs []int, wantRecords int) {
	h.t.Helper()
	homes := make([]string, 0, len(homeIdxs))
	for _, idx := range homeIdxs {
		homes = append(homes, h.nodeIDs[idx])
	}
	what := fmt.Sprintf("vault %s: origin %s segments held by every home", v.label, h.nodes[originID].label)
	h.waitProgress(what, 100*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, h.nodeIDs[observerIdx])
		if sub == nil {
			return "no vault-ctl FSM", false
		}
		records, held := 0, 0
		for _, e := range sub.ListCompletedSegments() {
			if e.OriginNodeID != originID {
				continue
			}
			if !containsAll(e.Holders, homes) {
				continue
			}
			held++
			records += int(e.RecordCount)
		}
		return fmt.Sprintf("held_segments=%d held_records=%d", held, records), records == wantRecords
	}, func() { h.dumpPipelineState(v) })
}

func containsAll(have, want []string) bool {
	for _, w := range want {
		if !slices.Contains(have, w) {
			return false
		}
	}
	return true
}

// TestOrchPipeline_NonHomeOriginRestartPurgesReleasedSegments: a non-home
// origin is down while vault-ctl releases segments it originated (every home
// pulled them first). Its completed/ copies survive the outage on disk —
// nothing on the node ran — and after restart the node reclaims them from the
// replicated release state alone. Every record stays servable from the homes.
func TestOrchPipeline_NonHomeOriginRestartPurgesReleasedSegments(t *testing.T) {
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
	origin := h.nodeIDs[3] // non-home origin

	// Half a chunk from the origin: published and pulled by every home, but
	// the chunk stays open, so nothing is released yet.
	const half = pipelineChunkMaxRecords / 2
	originBodies := h.submitIngestRecords(origin, half, "origin")
	h.waitOriginSegmentsHeld(v, 0, origin, homeIdxs, half)

	h.stopNode(origin)
	originRoot := h.pipelineVaultRoot(origin, v)
	before, err := countCompletedSegmentFiles(originRoot)
	if err != nil {
		t.Fatalf("list origin completed/: %v", err)
	}
	if before == 0 {
		t.Fatal("origin completed/ is empty before release — the scenario never reached the outage with unreleased segments")
	}

	// The rest of the chunk arrives on a home; the chunk seals and vault-ctl
	// releases every source segment — the stopped origin's included.
	homeBodies := h.submitIngestRecords(h.nodeIDs[0], pipelineChunkMaxRecords-half, "home")
	entries := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)
	h.waitGLCBsOnHomes(v, homeIdxs, entries)
	h.waitRegistryDrained(v, homeIdxs)

	stillThere, err := countCompletedSegmentFiles(originRoot)
	if err != nil {
		t.Fatalf("list origin completed/: %v", err)
	}
	if stillThere != before {
		t.Fatalf("origin completed/ changed while the origin was down: %d -> %d", before, stillThere)
	}

	h.startNode(origin)
	h.waitForAllReady()
	h.waitCompletedDrained(v, []int{3})

	all := maps.Clone(originBodies)
	maps.Copy(all, homeBodies)
	h.assertSearchBodiesExactly(v, h.nodeIDs[1], all)
}
