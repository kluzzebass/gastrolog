package orchestrator_test

// Multi-node coverage for gastrolog-3ba5ei: a seal the manifest never learned
// about must be driven back into agreement on EVERY voter, not just the node
// that noticed.
//
// The single-node tests pin which entries the category selects and what values
// it announces. This pins the part that only exists in a cluster: the recovery
// is itself a Raft command, so it has to commit and replicate — a leader that
// quietly fixed its own copy would leave the followers diverged, which is the
// same bug one layer down.

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
)

// TestOrchRel_SealAnnounceDivergence_ConvergesEveryNode produces a REAL
// divergence — the local seal happens and the manifest never learns — then
// checks the reconcile category drives every voter back into agreement.
//
// The announcer is detached across the seal. sealLocked only queues an announce
// when one is wired, so with it removed the chunk seals on disk and no
// CmdBeginSeal or CmdSealChunk is ever proposed. That is precisely what a
// swallowed apply failure produces, without needing the apply to fail.
//
// An earlier version of this test sealed normally and asserted the nodes agreed.
// That passes whether or not the fix exists, because there was no divergence to
// recover — a vacuous test, deleted rather than kept.
func TestOrchRel_SealAnnounceDivergence_ConvergesEveryNode(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	now := time.Now()
	for i := range 15 {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("announce-divergence-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	leader := h.waitForVaultCtlLeader()
	inst := leader.orch.FindLocalVaultInstance(h.vaultID)
	if inst == nil || inst.Chunks == nil {
		t.Fatal("vault-ctl leader has no local vault instance")
	}
	active := inst.Chunks.Active()
	if active == nil {
		t.Fatal("no active chunk to seal")
	}
	chunkID := active.ID

	// The create must have replicated before the divergence is staged, or the
	// followers simply have no entry yet and "not Active" would mean "not yet
	// told" rather than "diverged".
	h.waitProgress("chunk create replicating to every node", 50*time.Millisecond,
		func() (string, bool) {
			var views []string
			all := true
			for _, id := range h.nodeIDs {
				st, ok := h.chunkStatesOnNode(id)[chunkID]
				if !ok {
					all = false
				}
				views = append(views, fmt.Sprintf("%s=%s", h.nodes[id].label, st))
			}
			// Leadership is part of the progress metric, not just the chunk
			// states: the sweep is leadership-gated, so a window spent
			// electing is real activity that would otherwise read as a
			// 20-second stall under package-parallel load.
			leaderNow := "none"
			if l := h.currentVaultCtlLeader(); l != nil {
				leaderNow = l.label
			}
			return fmt.Sprintf("%v leader=%s", views, leaderNow), all
		}, func() {
			t.Logf("chunk %s never appeared on every node; nothing to diverge", chunkID)
		})

	getter, okGet := inst.Chunks.(chunk.AnnouncerGetter)
	setter, okSet := inst.Chunks.(chunk.AnnouncerSetter)
	if !okGet || !okSet {
		t.Skip("chunk manager cannot have its announcer swapped; divergence cannot be staged")
	}
	saved := getter.GetAnnouncer()
	if saved == nil {
		t.Fatal("leader has no announcer wired; the harness is not exercising vault-ctl")
	}

	// Seal with no announcer: local files close and the manifest never hears.
	setter.SetAnnouncer(nil)
	if err := inst.Chunks.Seal(); err != nil {
		setter.SetAnnouncer(saved)
		t.Fatalf("Seal: %v", err)
	}
	setter.SetAnnouncer(saved)

	// Verify the premise rather than assume it: without a divergence the rest
	// of this test proves nothing.
	for _, id := range h.nodeIDs {
		if got := h.chunkStatesOnNode(id)[chunkID]; got != chunk.ChunkStateActive {
			t.Fatalf("precondition: %s reports %s, want active — the divergence was not staged",
				h.nodes[id].label, got)
		}
	}
	if meta, err := inst.Chunks.Meta(chunkID); err != nil || !meta.Sealed {
		t.Fatalf("precondition: chunk is not sealed on the leader's disk (err=%v)", err)
	}

	if inst.Reconciler == nil {
		t.Fatal("leader has no lifecycle reconciler")
	}
	// The recovery is itself a Raft command, so every voter must converge — a
	// leader that fixed only its own copy would leave the same bug one layer
	// down.
	//
	// The sweep is re-driven on every poll rather than called once. It is
	// gated on vault-ctl leadership, so a single invocation can land in a
	// moment when this node is mid-election and do nothing, with no retry — a
	// one-shot call failed exactly that way under full-suite load. Production
	// drives this from the periodic tick, so repeating it here is the faithful
	// shape, not a workaround.
	h.waitProgress("every node reaching Sealed after the divergence sweep", 50*time.Millisecond,
		func() (string, bool) {
			// Re-resolve the leader each poll rather than reusing the one
			// captured before the seal: under package-parallel load the
			// vault-ctl group re-elects, and the sweep is leadership-gated,
			// so pinning one node can leave nobody driving the recovery.
			for _, id := range h.nodeIDs {
				n := h.nodes[id]
				if n == nil || n.orch == nil {
					continue
				}
				if vi := n.orch.FindLocalVaultInstance(h.vaultID); vi != nil && vi.Reconciler != nil {
					vi.Reconciler.SweepSealAnnounceDivergence()
				}
			}
			var views []string
			all := true
			for _, id := range h.nodeIDs {
				st := h.chunkStatesOnNode(id)[chunkID]
				if st != chunk.ChunkStateSealed {
					all = false
				}
				views = append(views, fmt.Sprintf("%s=%s", h.nodes[id].label, st))
			}
			return fmt.Sprintf("%v", views), all
		}, func() {
			for _, id := range h.nodeIDs {
				t.Logf("%s: %v", h.nodes[id].label, h.chunkStatesOnNode(id))
			}
		})
}

// A healthy cluster must be untouched by the new category. It runs on every
// tick for every vault, so a category that re-announced seals it should not
// touch would replay CmdSealChunk across the cluster forever.
func TestOrchRel_SealAnnounceDivergence_LeavesHealthyClusterAlone(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	now := time.Now()
	for i := range 10 {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("healthy-announce-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	h.sealOnLeader()
	h.eventuallyAllSeeSealedChunk(t)

	leader := h.waitForVaultCtlLeader()
	inst := leader.orch.FindLocalVaultInstance(h.vaultID)
	if inst == nil || inst.Reconciler == nil {
		t.Fatal("vault-ctl leader has no reconciler")
	}

	before := h.chunkStatesOnNode(leader.id)
	for range 3 {
		inst.Reconciler.SweepSealAnnounceDivergence()
	}
	after := h.chunkStatesOnNode(leader.id)

	if len(before) != len(after) {
		t.Fatalf("the divergence sweep changed the manifest: %d entries before, %d after", len(before), len(after))
	}
	// Only a REGRESSION is a violation. A chunk still mid-post-seal when the
	// snapshot was taken legitimately reaches Sealed during these divergence sweeps, and an
	// earlier version of this assertion called that a failure — it demanded the
	// manifest be frozen, which a live cluster's is not.
	for id, st := range before {
		if st == chunk.ChunkStateSealed && after[id] != chunk.ChunkStateSealed {
			t.Errorf("chunk %s regressed from sealed to %s across divergence sweeps", id, after[id])
		}
		if after[id] == chunk.ChunkStateActive && st != chunk.ChunkStateActive {
			t.Errorf("chunk %s was pushed back to active from %s", id, st)
		}
	}
}

// currentVaultCtlLeader returns the node currently holding vault-ctl leadership
// for the harness's default vault, or nil if the group is mid-election.
//
// Unlike waitForVaultCtlLeader this does not wait: callers use it inside a
// progress predicate, where "nobody is leader right now" is information to
// report rather than a condition to block on.
func (h *orchRelHarness) currentVaultCtlLeader() *orchRelNode {
	gid := raftgroup.VaultControlPlaneGroupID(h.vaultID)
	for _, id := range h.nodeIDs {
		n := h.nodes[id]
		if n == nil || n.groupMgr == nil {
			continue
		}
		g := n.groupMgr.GetGroup(gid)
		if g == nil || g.Raft == nil {
			continue
		}
		if g.Raft.State() == hraft.Leader {
			return n
		}
	}
	return nil
}
