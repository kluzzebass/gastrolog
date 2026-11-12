package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
)

// A placement change that replaces every home holding a sealed chunk must hand
// the chunk to the new homes: its segments are long released, so the former
// homes' GLCBs are the only copies of those records. The former homes stay the
// chunk's holders, and keep serving it, until the new homes hold it.
func TestOrchPipeline_PlacementChangeHandsSealedChunksToNewHomes(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline durability test")
	}

	cases := []struct {
		name     string
		oldHomes []int
		newHomes []int
		ingest   int
		// sweepsBeforePull keeps the former homes from serving their copies
		// across this many of the new leader's reconcile sweeps — each one a
		// chance to prune them — before the new homes may pull.
		sweepsBeforePull int
	}{
		{name: "RF=1 home replaced", oldHomes: []int{0}, newHomes: []int{1}, ingest: 3},
		{name: "RF=2 every home replaced", oldHomes: []int{0, 1}, newHomes: []int{2, 3}, ingest: 0},
		{name: "RF=1 home replaced, leader sweeps before the pull", oldHomes: []int{0}, newHomes: []int{1}, ingest: 3, sweepsBeforePull: 3},
		{name: "RF=2 every home replaced, leader sweeps before the pull", oldHomes: []int{0, 1}, newHomes: []int{2, 3}, ingest: 0, sweepsBeforePull: 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newOrchRelHarness(t, 4,
				withExtraVault(tc.oldHomes),
				withMatchAllRoute(1),
				withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
			)
			v := h.vaults[1]
			ctx := context.Background()

			bodies := h.submitIngestRecords(h.nodeIDs[tc.ingest], 2*pipelineChunkMaxRecords, "handoff")
			entries := h.waitSealedRecords(v, h.nodeIDs[tc.oldHomes[0]], int64(len(bodies)))
			h.waitGLCBsOnHomes(v, tc.oldHomes, entries)
			// No segment is left to rebuild a chunk from: the former homes'
			// GLCBs are the only copies of these records.
			h.waitSegmentsReleased(v, h.nodeIDs[tc.oldHomes[0]])

			if tc.sweepsBeforePull > 0 {
				for _, idx := range tc.oldHomes {
					h.nodes[h.nodeIDs[idx]].orch.RefuseGLCBServingForTest(errors.New("former home held out of reach"))
				}
			}
			h.setVaultPlacements(v, tc.newHomes)
			for i, id := range h.nodeIDs {
				n := h.nodes[id]
				switch {
				case slices.Contains(tc.newHomes, i) && !slices.Contains(tc.oldHomes, i):
					if err := n.orch.AddVaultInstance(ctx, v.id, n.factories); err != nil {
						t.Fatalf("AddVaultInstance on %s: %v", n.label, err)
					}
				case slices.Contains(tc.oldHomes, i) && !slices.Contains(tc.newHomes, i):
					n.orch.RemoveVaultInstance(v.id)
				}
				if err := n.orch.ReloadFilters(ctx); err != nil {
					t.Fatalf("ReloadFilters on %s: %v", n.label, err)
				}
			}

			leader := h.nodeIDs[tc.newHomes[0]]
			if tc.sweepsBeforePull > 0 {
				h.waitVaultCtlLeaderOn(v, leader)
				h.holdAcrossSweeps("former homes stay holders of every chunk the new homes lack",
					leader, "vault-catchup-sweep", tc.sweepsBeforePull, func() (string, bool) {
						for _, e := range h.sealedPipelineChunks(v, leader) {
							for _, idx := range tc.oldHomes {
								if !slices.Contains(e.Holders, h.nodeIDs[idx]) {
									return fmt.Sprintf("chunk %s holders=%v", e.ID, e.Holders), false
								}
							}
						}
						return "", true
					})
				for _, idx := range tc.oldHomes {
					former := h.nodeIDs[idx]
					losses := h.nodes[leader].orch.ChunkHolderLossWithout(ctx, former)
					if len(losses) != 1 || losses[0].VaultID != v.id || losses[0].Chunks != len(entries) {
						t.Fatalf("while the new homes lack the chunks, removing former home %s must lose all %d: got %+v",
							h.nodes[former].label, len(entries), losses)
					}
					h.nodes[former].orch.RefuseGLCBServingForTest(nil)
				}
			}

			h.waitGLCBsOnHomes(v, tc.newHomes, entries)
			h.waitProgress("handed-off records searchable on the new leader home", 50*time.Millisecond, func() (string, bool) {
				found := 0
				for _, raw := range h.searchRecords(v, leader) {
					if bodies[string(raw)] {
						found++
					}
				}
				return fmt.Sprintf("records=%d/%d", found, len(bodies)), found == len(bodies)
			}, func() { h.dumpPipelineState(v) })

			// Once the new homes hold every chunk, the former homes are no
			// longer needed as holders and are pruned.
			newHomeIDs := make([]string, 0, len(tc.newHomes))
			for _, idx := range tc.newHomes {
				newHomeIDs = append(newHomeIDs, h.nodeIDs[idx])
			}
			h.waitProgress("holders settle on the new homes", 50*time.Millisecond, func() (string, bool) {
				settled := 0
				var state []string
				for _, e := range h.sealedPipelineChunks(v, leader) {
					holders := slices.Clone(e.Holders)
					slices.Sort(holders)
					want := slices.Clone(newHomeIDs)
					slices.Sort(want)
					if slices.Equal(holders, want) {
						settled++
					}
					state = append(state, fmt.Sprintf("%s=%v", e.ID, e.Holders))
				}
				return fmt.Sprintf("settled=%d/%d %v", settled, len(entries), state), settled == len(entries)
			}, func() { h.dumpPipelineState(v) })
			for _, idx := range tc.oldHomes {
				if losses := h.nodes[leader].orch.ChunkHolderLossWithout(ctx, h.nodeIDs[idx]); len(losses) != 0 {
					t.Fatalf("after the handoff no chunk depends on former home %s: %+v", h.nodes[h.nodeIDs[idx]].label, losses)
				}
			}
		})
	}
}

// waitSegmentsReleased waits until the vault-ctl FSM on nodeID lists no
// completed segment for the vault: every segment was chunked and released.
func (h *orchRelHarness) waitSegmentsReleased(v vaultSpec, nodeID string) {
	h.t.Helper()
	h.waitProgress(fmt.Sprintf("vault %s: every segment released", v.label), 50*time.Millisecond, func() (string, bool) {
		fsm := h.vaultCtlSubFSM(v, nodeID)
		if fsm == nil {
			return "no fsm", false
		}
		n := len(fsm.ListCompletedSegments())
		return fmt.Sprintf("segments=%d", n), n == 0
	}, func() { h.dumpPipelineState(v) })
}

// waitVaultCtlLeaderOn waits until nodeID leads the vault's vault-ctl group.
func (h *orchRelHarness) waitVaultCtlLeaderOn(v vaultSpec, nodeID string) {
	h.t.Helper()
	gid := raftgroup.VaultControlPlaneGroupID(v.id)
	n := h.nodes[nodeID]
	h.waitProgress(fmt.Sprintf("vault %s vault-ctl leadership on %s", v.label, n.label), 30*time.Millisecond, func() (string, bool) {
		g := n.groupMgr.GetGroup(gid)
		if g == nil {
			return "no-group", false
		}
		st := g.Raft.State()
		return fmt.Sprintf("%s@t%s", st, g.Raft.Stats()["term"]), st == hraft.Leader
	}, nil)
}
