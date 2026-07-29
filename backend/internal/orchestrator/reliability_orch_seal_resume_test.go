package orchestrator_test

// Multi-node coverage for gastrolog-v6nf71: a chunk stranded mid-seal must
// reach Sealed on EVERY node without restarting anything.
//
// The single-node tests (seal_resume_steady_state_test.go) pin which entries
// the category picks up and which it leaves alone. This pins the part that only
// exists in a cluster: the resumed post-seal has to produce a CmdSealChunk that
// replicates, so all three voters converge on Sealed rather than the leader
// quietly fixing its own copy.

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
)

// catchupSweepJob is the scheduler job that carries ReconcileTick — and with it
// the seal-resume category. Mirrors catchupSweepJob, which this
// external test package cannot see, the same way retentionSweepJob does.
const catchupSweepJob = "vault-catchup-sweep"

// TestOrchRel_StrandedSeal_ResumesWithoutRestart strands a seal the way a
// failed post-seal job does — the FSM entry advances to Sealing and nothing
// ever runs the second half — and then lets the steady-state reconcile pass
// recover it.
//
// Chunks.Seal() is the honest way to produce that state: it announces
// Active → Sealing and returns, which is exactly what SealActive does before it
// hands the rest to the scheduler. Skipping that hand-off reproduces a post-seal
// that never ran, with no fault injection and no timing games.
//
// Before this change the only caller of the resume path was the FSM's
// after-restore hook, so this test's chunk would sit in Sealing forever.
func TestOrchRel_StrandedSeal_ResumesWithoutRestart(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 3)

	const records = 20
	now := time.Now()
	for i := range records {
		if err := h.appendOnLeader(chunk.Record{
			SourceTS: now,
			IngestTS: now,
			Raw:      []byte("stranded-seal-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	leader := h.waitForVaultCtlLeader()
	inst := leader.orch.FindLocalVaultInstance(h.vaultID)
	if inst == nil || inst.Chunks == nil {
		t.Fatal("vault-ctl leader has no local vault instance to seal")
	}
	active := inst.Chunks.Active()
	if active == nil {
		t.Fatal("no active chunk to strand after appending")
	}
	strandedID := active.ID

	// Seal WITHOUT the post-seal hand-off: Active → Sealing, then nothing.
	if err := inst.Chunks.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// The entry must actually be stranded, or the rest of this test proves
	// nothing. Sealing has to have replicated before we assert recovery.
	h.waitProgress("stranded entry reaching Sealing on every node", 50*time.Millisecond, func() (string, bool) {
		var views []string
		allSealing := true
		for _, id := range h.nodeIDs {
			st, ok := h.chunkStatesOnNode(id)[strandedID]
			if !ok || st != chunk.ChunkStateSealing {
				allSealing = false
			}
			views = append(views, fmt.Sprintf("%s=%s", h.nodes[id].label, st))
		}
		return fmt.Sprintf("%v", views), allSealing
	}, func() {
		t.Logf("chunk %s never reached Sealing; the strand setup itself failed", strandedID)
	})

	if inst.Reconciler == nil {
		t.Fatal("vault instance has no lifecycle reconciler")
	}
	// Re-driven on every poll rather than called once. The categories inside
	// ReconcileTick are leadership-gated, so a single invocation can land while
	// the group is mid-election and do nothing, with no retry — which is how a
	// one-shot call failed under full-suite load. Production drives this from
	// the periodic tick, so repeating it is the faithful shape, not a
	// workaround.
	h.waitProgress("stranded entry reaching Sealed on every node", 50*time.Millisecond, func() (string, bool) {
		for _, id := range h.nodeIDs {
			if n := h.nodes[id]; n != nil && n.orch != nil {
				if vi := n.orch.FindLocalVaultInstance(h.vaultID); vi != nil && vi.Reconciler != nil {
					vi.Reconciler.ReconcileTick()
				}
			}
		}
		var views []string
		allSealed := true
		for _, id := range h.nodeIDs {
			st, ok := h.chunkStatesOnNode(id)[strandedID]
			if !ok || st != chunk.ChunkStateSealed {
				allSealed = false
			}
			views = append(views, fmt.Sprintf("%s=%s", h.nodes[id].label, st))
		}
		return fmt.Sprintf("%v", views), allSealed
	}, func() {
		for _, id := range h.nodeIDs {
			for cid, st := range h.chunkStatesOnNode(id) {
				t.Logf("%s: chunk %s state=%s", h.nodes[id].label, cid, st)
			}
			h.logPostSealJobs(id)
		}
	})
}

// A reconcile pass over a HEALTHY cluster must not disturb anything. The tick
// now runs on every cadence for every vault, so a category that re-drove seals
// it should not touch would rebuild GLCBs and re-announce across the cluster
// forever — the duplicate-work shape gastrolog-3hwngy postmortemed.
func TestOrchRel_ReconcileTickLeavesCompletedSealsAlone(t *testing.T) {
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
			Raw:      []byte("healthy-seal-" + strconv.Itoa(i)),
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
		inst.Reconciler.ReconcileTick()
	}
	after := h.chunkStatesOnNode(leader.id)

	if len(before) != len(after) {
		t.Fatalf("reconcile ticks changed the manifest: %d entries before, %d after", len(before), len(after))
	}
	// Only a REGRESSION is a violation. A chunk still mid-post-seal when the
	// snapshot was taken legitimately reaches Sealed during these reconcile ticks, and an
	// earlier version of this assertion called that a failure — it demanded the
	// manifest be frozen, which a live cluster's is not.
	for id, st := range before {
		if st == chunk.ChunkStateSealed && after[id] != chunk.ChunkStateSealed {
			t.Errorf("chunk %s regressed from sealed to %s across reconcile ticks", id, after[id])
		}
		if after[id] == chunk.ChunkStateActive && st != chunk.ChunkStateActive {
			t.Errorf("chunk %s was pushed back to active from %s", id, st)
		}
	}
}
