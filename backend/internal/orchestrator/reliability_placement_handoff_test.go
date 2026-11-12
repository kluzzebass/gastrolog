package orchestrator_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/vaultraft/vaultctlfsm"

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

// A cloud-backed vault moves from its only home to another node while the
// former home's uploads are parked, so no sealed chunk is in the cloud yet and
// the former home's GLCBs are the only copies of their records. The new home
// pulls each chunk from the former home and uploads it; until a chunk is in the
// cloud the former home keeps its copy and stays the chunk's holder.
func TestOrchPipeline_CloudVaultMoveHandsOffChunksNotYetUploaded(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline durability test")
	}
	h, ledger := newCloudUploadHarness(t, 4)
	v := h.addCloudCacheVault("C", []int{0}, "")
	oldHome, newHome, ingest := h.nodeIDs[0], h.nodeIDs[1], h.nodeIDs[3]

	ledger.hold("upload", oldHome)
	bodies := h.submitIngestRecords(ingest, 2*pipelineChunkMaxRecords, "cloud-move")
	entries := h.waitSealedRecords(v, oldHome, int64(len(bodies)))
	h.waitHeldByHomes(v, oldHome, entries, []int{0})
	h.waitRegistryDrained(v, []int{0, 1, 2, 3})
	for _, e := range h.sealedPipelineChunks(v, oldHome) {
		if e.CloudBacked {
			t.Fatalf("premise: chunk %s is cloud-backed while the former home's uploads are parked", e.ID)
		}
	}
	copies := h.formerHomeCopies(v, []int{0}, entries)

	h.movePlacement(&v, []int{1})
	ledger.release("upload", oldHome, errors.New("upload abandoned by the former placement leader"), true)

	h.waitProgress("every chunk cloud-backed through the new home", 100*time.Millisecond, func() (string, bool) {
		st := h.checkHandoff(v, newHome, entries, copies)
		return st.String(), st.cloudBacked == len(entries)
	}, func() {
		h.dumpPipelineState(v)
		t.Logf("cloud PUT ledger:\n%s", ledger)
	})
	h.waitCloudUploadsSettled(v)
	for _, e := range entries {
		if _, stored := ledger.putsFor(e.ID); !slices.Equal(stored, []string{h.nodes[newHome].label}) {
			t.Fatalf("chunk %s stored by %v, want once by the new home %s\nledger:\n%s",
				e.ID, stored, h.nodes[newHome].label, ledger)
		}
	}
	h.waitSearchable(v, newHome, len(bodies))
	h.assertSearchBodiesExactly(v, newHome, bodies)
	h.waitFormerHomesPruned(v, newHome, entries, []string{oldHome})
}

// The only home of a vault, holding the only copy of every sealed chunk, is
// down when the vault moves to another node. While it is down the new home
// raises glcb-unrecoverable, the former home stays the holder of every chunk
// across the new leader's sweeps, and nothing is deleted. Once it returns it
// serves the chunks to the new home and the alarm clears.
func TestOrchPipeline_DepartedHomeDownDuringTheMoveIsWaitedFor(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node pipeline durability test with a node restart")
	}
	clock := &offsetClock{}
	h := newOrchRelHarness(t, 4,
		withExtraVault([]int{0}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
		withAlarmClock(clock.now),
	)
	v := h.vaults[1]
	oldHome, newHome, ingest := h.nodeIDs[0], h.nodeIDs[1], h.nodeIDs[3]
	ctx := context.Background()

	bodies := h.submitIngestRecords(ingest, 2*pipelineChunkMaxRecords, "departed")
	entries := h.waitSealedRecords(v, oldHome, int64(len(bodies)))
	h.waitGLCBsOnHomes(v, []int{0}, entries)
	h.waitRegistryDrained(v, []int{0, 1, 2, 3})
	copies := h.formerHomeCopies(v, []int{0}, entries)

	h.stopNode(oldHome)
	h.refusePulls(oldHome)
	h.movePlacement(&v, []int{1})
	h.waitVaultCtlLeaderOn(v, newHome)

	unrecoverable, ok := alert.TypeByID(glcbUnrecoverableAlarm)
	if !ok {
		t.Fatalf("alarm type %s is not in the catalog", glcbUnrecoverableAlarm)
	}
	h.waitProgress("glcb-unrecoverable on the new home", 100*time.Millisecond, func() (string, bool) {
		clock.advance(unrecoverable.DelayOn + time.Second)
		a := h.standingAlarm(newHome, glcbUnrecoverableAlarm, v)
		return a, a != ""
	}, func() { h.dumpPipelineState(v) })

	h.holdAcrossSweeps("the down former home stays every chunk's holder, the alarm stands, nothing is deleted",
		newHome, "vault-catchup-sweep", 3, func() (string, bool) {
			if h.standingAlarm(newHome, glcbUnrecoverableAlarm, v) == "" {
				return "glcb-unrecoverable cleared with no source back", false
			}
			st := h.checkHandoff(v, newHome, entries, copies)
			if st.missing != "" {
				return st.missing, false
			}
			for _, e := range h.sealedPipelineChunks(v, newHome) {
				if !slices.Equal(e.Holders, []string{oldHome}) {
					return fmt.Sprintf("chunk %s holders=%v", e.ID, e.Holders), false
				}
			}
			losses := h.nodes[newHome].orch.ChunkHolderLossWithout(ctx, oldHome)
			if len(losses) != 1 || losses[0].VaultID != v.id || losses[0].Chunks != len(entries) {
				return fmt.Sprintf("removing the former home must lose all %d chunks: got %+v", len(entries), losses), false
			}
			return "", true
		})

	h.startNode(oldHome)
	h.waitForAllReady()
	h.waitProgress("the returned former home hands every chunk to the new home", 100*time.Millisecond, func() (string, bool) {
		st := h.checkHandoff(v, newHome, entries, copies)
		return st.String(), st.held == len(entries)
	}, func() { h.dumpPipelineState(v) })
	h.waitGLCBsOnHomes(v, []int{1}, entries)
	h.waitProgress("glcb-unrecoverable clears on the new home", 100*time.Millisecond, func() (string, bool) {
		a := h.standingAlarm(newHome, glcbUnrecoverableAlarm, v)
		return a, a == ""
	}, nil)
	h.waitSearchable(v, newHome, len(bodies))
	h.assertSearchBodiesExactly(v, newHome, bodies)
	h.waitFormerHomesPruned(v, newHome, entries, []string{oldHome})
}

const glcbUnrecoverableAlarm = "glcb-unrecoverable"

// offsetClock is the wall clock plus an offset the test advances.
type offsetClock struct{ offset atomic.Int64 }

func (c *offsetClock) now() time.Time { return time.Now().Add(time.Duration(c.offset.Load())) }

func (c *offsetClock) advance(d time.Duration) { c.offset.Add(int64(d)) }

// standingAlarm is the detail of the vault's standing alarm of typeID on the
// node, or "".
func (h *orchRelHarness) standingAlarm(nodeID, typeID string, v vaultSpec) string {
	for _, a := range h.nodes[nodeID].alerts.Standing() {
		if a.TypeID == typeID && a.InstanceKey == v.id.String() {
			return a.Detail
		}
	}
	return ""
}

// movePlacement replaces the vault's placement with newIdxs (first is the
// placement leader) and runs on every running node what the config dispatcher
// runs for it: the vault instance added on a node joining the placement and
// removed from a node leaving it, routes reloaded, the placement role
// reconciled.
func (h *orchRelHarness) movePlacement(v *vaultSpec, newIdxs []int) {
	h.t.Helper()
	ctx := context.Background()
	oldIdxs := v.nodeIdxs
	h.setVaultPlacements(*v, newIdxs)
	v.nodeIdxs = newIdxs
	for i := range h.vaults {
		if h.vaults[i].id == v.id {
			h.vaults[i].nodeIdxs = newIdxs
		}
	}
	for i, id := range h.nodeIDs {
		n := h.nodes[id]
		if n.orch == nil {
			continue
		}
		switch {
		case slices.Contains(newIdxs, i) && !slices.Contains(oldIdxs, i):
			if err := n.orch.AddVaultInstance(ctx, v.id, n.factories); err != nil {
				h.t.Fatalf("AddVaultInstance %s on %s: %v", v.label, n.label, err)
			}
		case slices.Contains(oldIdxs, i) && !slices.Contains(newIdxs, i):
			n.orch.RemoveVaultInstance(v.id)
		}
		if err := n.orch.ReloadFilters(ctx); err != nil {
			h.t.Fatalf("ReloadFilters on %s: %v", n.label, err)
		}
		n.orch.ReconcileVaultPlacement(ctx, v.id)
	}
}

// refusePulls makes the node refuse every segment and GLCB pull, as an
// unreachable node does. A stopped node's cluster server outlives its
// orchestrator in this harness, so a stopped node refuses pulls only through
// this; startNode serves them again.
func (h *orchRelHarness) refusePulls(nodeID string) {
	errDown := fmt.Errorf("%s is unreachable", h.nodes[nodeID].label)
	h.nodes[nodeID].pullRefusal.Store(&errDown)
}

// formerHomeCopies records where each of the given homes keeps its GLCB of
// each chunk, resolved while they are still homes, keyed by node ID.
func (h *orchRelHarness) formerHomeCopies(v vaultSpec, homeIdxs []int, entries []vaultctlfsm.ManifestEntry) map[string]map[chunk.ChunkID]string {
	h.t.Helper()
	out := make(map[string]map[chunk.ChunkID]string, len(homeIdxs))
	for _, idx := range homeIdxs {
		nodeID := h.nodeIDs[idx]
		out[nodeID] = make(map[chunk.ChunkID]string, len(entries))
		for _, e := range entries {
			path := h.pipelineGLCBPath(nodeID, v, e.ID)
			if _, err := os.Stat(path); err != nil {
				h.t.Fatalf("premise: %s holds no GLCB of chunk %s: %v", h.nodes[nodeID].label, e.ID, err)
			}
			out[nodeID][e.ID] = path
		}
	}
	return out
}

// handoffState is one observation of a placement change handing chunks from
// the former homes to the new ones.
type handoffState struct {
	held        int // chunks every new home holds: GLCB on disk and a holder receipt
	cloudBacked int
	missing     string // the first chunk let go too early, or ""
	pending     []string
}

func (s handoffState) String() string {
	return fmt.Sprintf("held=%d cloud_backed=%d pending=%v", s.held, s.cloudBacked, s.pending)
}

// checkHandoff observes the hand-off of entries on observer's vault-ctl FSM to
// the vault's current homes. It fails the test the moment a chunk leaves the
// vault chunk manifest, or a former home in copies lets go of its copy or its
// holder record of a chunk that is neither cloud-backed nor held by
// HolderQuorum of the new homes.
func (h *orchRelHarness) checkHandoff(v vaultSpec, observer string, entries []vaultctlfsm.ManifestEntry, copies map[string]map[chunk.ChunkID]string) handoffState {
	h.t.Helper()
	var st handoffState
	sub := h.vaultCtlSubFSM(v, observer)
	if sub == nil {
		st.pending = append(st.pending, "no vault-ctl FSM on "+h.nodes[observer].label)
		return st
	}
	homes := make([]string, 0, len(v.nodeIdxs))
	for _, idx := range v.nodeIdxs {
		homes = append(homes, h.nodeIDs[idx])
	}
	quorum := vaultctlfsm.HolderQuorum(homes)
	for _, want := range entries {
		e := sub.Get(want.ID)
		if e == nil {
			h.dumpPipelineState(v)
			h.t.Fatalf("chunk %s left the vault chunk manifest during the hand-off", want.ID)
		}
		placed := 0
		for _, home := range homes {
			if _, err := os.Stat(h.pipelineGLCBPath(home, v, e.ID)); err == nil && slices.Contains(e.Holders, home) {
				placed++
			}
		}
		if placed == len(homes) {
			st.held++
		} else {
			st.pending = append(st.pending, fmt.Sprintf("%s:held_by_%d/%d_homes", e.ID, placed, len(homes)))
		}
		if e.CloudBacked {
			st.cloudBacked++
			continue
		}
		if placed >= quorum {
			continue
		}
		for former, held := range copies {
			if !slices.Contains(e.Holders, former) {
				st.missing = fmt.Sprintf("former home %s is no longer a holder of chunk %s held by %d of the %d homes it needs (holders %v)",
					h.nodes[former].label, e.ID, placed, quorum, e.Holders)
			} else if _, err := os.Stat(held[e.ID]); err != nil {
				st.missing = fmt.Sprintf("former home %s lost its copy of chunk %s held by %d of the %d homes it needs: %v",
					h.nodes[former].label, e.ID, placed, quorum, err)
			}
			if st.missing != "" {
				h.dumpPipelineState(v)
				h.t.Fatalf("%s", st.missing)
			}
		}
	}
	return st
}

// waitFormerHomesPruned waits until no chunk names a former home as a holder
// and no chunk depends on a former home's copy any more.
func (h *orchRelHarness) waitFormerHomesPruned(v vaultSpec, observer string, entries []vaultctlfsm.ManifestEntry, former []string) {
	h.t.Helper()
	ctx := context.Background()
	h.waitProgress("former homes pruned from the holders", 50*time.Millisecond, func() (string, bool) {
		sub := h.vaultCtlSubFSM(v, observer)
		if sub == nil {
			return "no vault-ctl FSM", false
		}
		var claimed []string
		for _, want := range entries {
			e := sub.Get(want.ID)
			if e == nil {
				h.t.Fatalf("chunk %s left the vault chunk manifest", want.ID)
			}
			for _, f := range former {
				if slices.Contains(e.Holders, f) {
					claimed = append(claimed, fmt.Sprintf("%s@%s", e.ID, h.nodes[f].label))
				}
			}
		}
		return fmt.Sprintf("claimed=%v", claimed), len(claimed) == 0
	}, func() { h.dumpPipelineState(v) })
	for _, f := range former {
		if losses := h.nodes[observer].orch.ChunkHolderLossWithout(ctx, f); len(losses) != 0 {
			h.t.Fatalf("after the hand-off no chunk depends on former home %s: %+v", h.nodes[f].label, losses)
		}
	}
}
