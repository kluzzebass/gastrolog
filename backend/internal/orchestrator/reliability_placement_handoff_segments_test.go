package orchestrator_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// The only home of a vault has collected segments no sealed chunk covers yet
// when the vault moves to another node, and the node that originated them is
// down. The segments are unreleased and the former home's head/ copies are the
// only reachable ones. The former home, held out of reach across three of the
// new leader's sweeps, stays their holder, and neither it nor the origin may be
// removed; once reachable it hands them to the new home, which seals the chunk
// they belong to, every acked record is searchable there, and both may go.
func TestOrchPipeline_FormerHomeHandsOffUnchunkedSegmentsWhileTheOriginIsDown(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline durability test with a node down")
	}
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	oldHome, newHome, ingestAfter, origin := h.nodeIDs[0], h.nodeIDs[1], h.nodeIDs[2], h.nodeIDs[3]
	const half = pipelineChunkMaxRecords / 2
	ctx := context.Background()

	before := h.submitIngestRecords(origin, half, "unchunked")
	segments := h.waitCollectedUnchunked(v, oldHome, half)
	root := h.pipelineVaultRoot(oldHome, v)
	for _, seg := range segments {
		if _, err := os.Stat(paths.HeadSegment(root, seg.SegmentID)); err != nil {
			t.Fatalf("premise: the home holds no head/ copy of segment %s: %v", seg.SegmentID, err)
		}
	}

	h.stopNode(origin)
	h.refusePulls(origin)
	h.refusePulls(oldHome)
	h.movePlacement(&v, []int{1})
	after := h.submitIngestRecords(ingestAfter, pipelineChunkMaxRecords-half, "after-move")
	bodies := mergeBodies(before, after)

	h.waitVaultCtlLeaderOn(v, newHome)
	h.holdAcrossSweeps("the former home stays the holder of every segment the new home lacks",
		newHome, "vault-catchup-sweep", 3, func() (string, bool) {
			sub := h.vaultCtlSubFSM(v, newHome)
			if sub == nil {
				return "no vault-ctl FSM", false
			}
			registered := sub.ListCompletedSegments()
			for _, want := range segments {
				i := slices.IndexFunc(registered, func(s vaultctlfsm.CompletedSegmentEntry) bool { return s.SegmentID == want.SegmentID })
				if i < 0 {
					return fmt.Sprintf("segment %s left the vault segment manifest", want.SegmentID), false
				}
				if !slices.Contains(registered[i].Holders, oldHome) {
					return fmt.Sprintf("segment %s holders=%v", want.SegmentID, registered[i].Holders), false
				}
			}
			for _, node := range []string{oldHome, origin} {
				losses := h.nodes[newHome].orch.HolderLossWithout(ctx, node)
				if len(losses) != 1 || losses[0].VaultID != v.id || losses[0].Segments != len(segments) {
					return fmt.Sprintf("removing %s must lose all %d segments: got %+v", h.nodes[node].label, len(segments), losses), false
				}
			}
			return "", true
		})
	h.servePulls(oldHome)

	entries := h.waitSealedRecords(v, newHome, int64(len(bodies)))
	h.waitGLCBsOnHomes(v, []int{1}, entries)
	h.waitSearchable(v, newHome, len(bodies))
	h.assertSearchBodiesExactly(v, newHome, bodies)
	// The chunk supersedes the segments, so they are released and neither the
	// former home nor the origin is needed any more.
	h.waitRegistryDrained(v, []int{0, 1, 2})
	for _, node := range []string{oldHome, origin} {
		if losses := h.nodes[newHome].orch.HolderLossWithout(ctx, node); len(losses) != 0 {
			t.Fatalf("after the hand-off nothing depends on %s: %+v", h.nodes[node].label, losses)
		}
	}
}

// waitCollectedUnchunked waits until home holds every completed segment of the
// vault, the segments carry exactly records records, and they sit in the open
// chunk with no chunk sealed. It returns the segments.
func (h *orchRelHarness) waitCollectedUnchunked(v vaultSpec, home string, records int) []vaultctlfsm.CompletedSegmentEntry {
	h.t.Helper()
	var segments []vaultctlfsm.CompletedSegmentEntry
	h.waitProgress("segments collected by the home, unchunked", 50*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, home)
		if sub == nil {
			return "no vault-ctl FSM", false
		}
		segments = sub.ListCompletedSegments()
		held, total := 0, 0
		for _, seg := range segments {
			total += int(seg.RecordCount)
			if slices.Contains(seg.Holders, home) {
				held++
			}
		}
		open := uint64(0)
		if oc := sub.OpenChunk(); oc != nil {
			open = oc.TotalRecords
		}
		sealed := len(h.sealedPipelineChunks(v, home))
		if sealed != 0 {
			h.t.Fatalf("premise: %d chunks sealed from %d records, want none", sealed, records)
		}
		return fmt.Sprintf("segments=%d held=%d records=%d open=%d", len(segments), held, total, open),
			total == records && held == len(segments) && open == uint64(records)
	}, func() { h.dumpPipelineState(v) })
	return segments
}

func mergeBodies(sets ...map[string]bool) map[string]bool {
	out := map[string]bool{}
	for _, s := range sets {
		for b := range s {
			out[b] = true
		}
	}
	return out
}

// servePulls lets the node serve pulls again after refusePulls.
func (h *orchRelHarness) servePulls(nodeID string) {
	h.nodes[nodeID].pullRefusal.Store(nil)
}
