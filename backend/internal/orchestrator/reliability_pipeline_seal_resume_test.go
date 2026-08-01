package orchestrator_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// Multi-node restart-survival for seal resumption on PIPELINE vaults.
//
// ReconcileFromSnapshot is the path under test: it runs once a node's vault-ctl
// FSM has been restored, and it is where a pipeline vault's Sealing entries have
// to be told apart. Three real nodes, a real pipeline vault with traffic through
// it, and a real chunk-manager chunk stranded mid-seal beside the pipeline's own
// chunks — the coexistence that makes the discrimination necessary.
//
// The steady-state category is a different test (see
// TestOrchRel_StrandedSeal_ResumesWithoutRestart) and is switched off here, or
// it would recover the strand on its own and this test would assert nothing.
func TestOrchRel_PipelineVault_RestartResumesStrandWithoutTouchingManifests(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-node reliability test")
	}
	t.Parallel()
	h := newOrchRelHarness(t, 3,
		withExtraVault([]int{0, 1, 2}),
		withMatchAllRoute(1),
		withPipelineCluster(pipelineTestCompletePolicy, pipelineChunkMaxRecords),
	)
	v := h.vaults[1]
	enableVault(t, h, v)

	// Pipeline traffic, so the vault really is running a pipeline while the
	// strand below is recovered. Without it this would pass on a vault that is
	// only nominally a pipeline vault.
	h.submitIngestRecords(h.nodeIDs[0], pipelineChunkMaxRecords, "restart-resume")
	// Captured while the strand below does not exist yet, so this set is the
	// pipeline population by construction rather than by filtering.
	pipelineChunks := h.waitSealedRecords(v, h.nodeIDs[0], pipelineChunkMaxRecords)

	// Isolate the restart path. The steady-state seal-resume category rides the
	// vault-catchup sweep and recovers the same strand within a sweep or two, so
	// with it running this test passes whether or not the restart path works.
	h.stopVaultCatchupSweep()

	// A chunk-manager chunk on the SAME vault. AppendToVault writes through the
	// chunk manager, which is how a legacy active comes to sit beside pipeline
	// manifests.
	leader := h.waitForVaultCtlLeaderForVault(v)
	now := time.Now()
	for i := range 20 {
		if err := h.appendOnLeaderForVault(v, chunk.Record{
			SourceTS: now, IngestTS: now,
			Raw: []byte("strand-" + strconv.Itoa(i)),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	inst := leader.orch.FindLocalVaultInstance(v.id)
	if inst == nil || inst.Chunks == nil {
		t.Fatal("vault-ctl leader has no local vault instance")
	}
	active := inst.Chunks.Active()
	if active == nil {
		t.Fatal("no chunk-manager active chunk to strand; the append path did not take")
	}
	strandedID := active.ID

	// Strand it: Active → Sealing with no post-seal hand-off, which is what a
	// crash between AnnounceBeginSeal and PostSealProcess leaves behind.
	if err := inst.Chunks.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	if meta, err := inst.Chunks.Meta(strandedID); err != nil || !meta.Sealed {
		t.Fatalf("premise: chunk not sealed on the leader's disk (err=%v)", err)
	}

	// PREMISE, and the check the deleted draft lacked: the FSM must actually
	// hold this chunk in Sealing, and the pipeline's chunks must be there too.
	// Without both, a passing assertion below would mean nothing.
	fsm := h.vaultCtlSubFSM(v, leader.id)
	if fsm == nil {
		t.Fatal("premise: no vault-ctl sub-FSM on the leader")
	}
	h.waitProgress("premise: strand visible in the FSM as Sealing", 50*time.Millisecond,
		func() (string, bool) {
			st, ok := entryState(fsm, strandedID)
			return fmt.Sprintf("strand=%v/%s", ok, st), ok && st == chunk.ChunkStateSealing
		}, nil)
	// Exactly the discrimination under test, asserted against real FSM state
	// rather than assumed: a chunk-manager strand carries no sealed manifest.
	if fsm.AwaitingBuild(strandedID) {
		t.Fatal("premise: the strand looks like a pipeline chunk awaiting build; " +
			"the two populations are not being distinguished as expected")
	}

	// The restart: ReconcileFromSnapshot is what a node runs once its vault-ctl
	// FSM has been restored. Repeated because the post-seal it schedules can
	// lose a race under full-suite load, which is how production behaves too —
	// the tick re-drives it.
	h.waitProgress("stranded chunk reaching Sealed on every node after restore",
		50*time.Millisecond, func() (string, bool) {
			for _, id := range h.nodeIDs {
				if n := h.nodes[id]; n != nil && n.orch != nil {
					if vi := n.orch.FindLocalVaultInstance(v.id); vi != nil && vi.Reconciler != nil {
						if sub := h.vaultCtlSubFSM(v, id); sub != nil {
							vi.Reconciler.ReconcileFromSnapshot(sub)
						}
					}
				}
			}
			var views []string
			allSealed := true
			for _, id := range h.nodeIDs {
				st, ok := h.chunkStatesOnNodeForVault(v, id)[strandedID]
				if !ok || st != chunk.ChunkStateSealed {
					allSealed = false
				}
				views = append(views, fmt.Sprintf("%s=%s", h.nodes[id].label, st))
			}
			return fmt.Sprintf("%v", views), allSealed
		}, func() {
			for _, id := range h.nodeIDs {
				for cid, st := range h.chunkStatesOnNodeForVault(v, id) {
					t.Logf("%s: chunk %s state=%s", h.nodes[id].label, cid, st)
				}
			}
			for _, id := range h.nodeIDs {
				h.logPostSealJobs(id)
			}
			h.dumpPipelineState(v)
		})

	// The resume is what recovered the strand, not some other path: the leader
	// must hold a post-seal job for it. This is also the control for the
	// pipeline-side assertion below, which is a negative on the same observable
	// and would pass against a scheduler that never records anything.
	if !h.postSealScheduledFor(leader.id, strandedID) {
		t.Errorf("no post-seal job for the strand on the vault-ctl leader; the strand reached Sealed by some other path")
	}

	// The other half. The pipeline's chunks must survive the restore untouched:
	// still Sealed, and never handed to a post-seal. A resume that grabbed one
	// would drive it through sealToGLCB over a GLCB the pipeline had already
	// built, and a post-seal job under its ID is what that looks like.
	//
	// The sealed-but-unbuilt window itself is not held open here — it closes as
	// soon as a home builds the GLCB, and holding it open would mean racing the
	// build. The FSM-level tests cover that population directly.
	for _, id := range h.nodeIDs {
		states := h.chunkStatesOnNodeForVault(v, id)
		for _, e := range pipelineChunks {
			if st := states[e.ID]; st != chunk.ChunkStateSealed {
				t.Errorf("%s: pipeline chunk %s is %s after the restore pass, want Sealed",
					h.nodes[id].label, e.ID, st)
			}
			if h.postSealScheduledFor(id, e.ID) {
				t.Errorf("%s: pipeline chunk %s was handed to a post-seal; the resume took a chunk awaiting its build",
					h.nodes[id].label, e.ID)
			}
		}
	}
}

// entryState reads one chunk's state out of an FSM listing.
func entryState(fsm *vaultctlfsm.FSM, id chunk.ChunkID) (chunk.ChunkState, bool) {
	for _, e := range fsm.List() {
		if e.ID == id {
			return e.State, true
		}
	}
	return 0, false
}
