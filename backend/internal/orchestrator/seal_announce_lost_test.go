package orchestrator

// A seal whose vault-ctl announce fails leaves the chunk sealed on disk and
// Active in the replicated manifest — and every layer reports success.
//
// This is the state captured under gastrolog-231ik: entry Active on all three
// nodes while the sealing leader's post-seal job read status=completed. The
// flake is one way to trigger it; the defect is that a failed announce is both
// invisible and unrecoverable. That does not need a flake to demonstrate.
//
// Announcer.apply discards applier.Apply's error into a warn line, so
// AnnounceBeginSeal returns normally on failure. Seal() therefore returns nil,
// the local sealed flag is already set, and PostSealProcess runs and completes.
// Nothing anywhere learns the manifest never moved.

import (
	"log/slog"
	"testing"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// failingCtlApplier refuses every command, standing in for a vault-ctl apply
// that cannot commit — no leader, a forward that times out, an apply pump
// blocked behind its own commit.
type failingCtlApplier struct {
	fsm     *vaultctlfsm.FSM
	failing bool
	applied int
}

func (d *failingCtlApplier) Apply(data []byte) error {
	if d.failing {
		return errApplyRefused
	}
	d.applied++
	d.fsm.Apply(&hraft.Log{Data: data})
	return nil
}

var errApplyRefused = errAsString("vault-ctl apply refused")

type errAsString string

func (e errAsString) Error() string { return string(e) }

// newLostAnnounceFixture is newSealActiveFixture with an applier whose failure
// is under the test's control.
func newLostAnnounceFixture(t *testing.T) (*Orchestrator, glid.GLID, *vaultctlfsm.FSM, *failingCtlApplier) {
	t.Helper()
	vaultID := glid.New()
	fsm := vaultctlfsm.New()
	inst, _ := newFileInstance(t, vaultID)
	setter, ok := inst.Chunks.(chunk.AnnouncerSetter)
	if !ok {
		t.Fatalf("file chunk manager does not implement chunk.AnnouncerSetter")
	}
	applier := &failingCtlApplier{fsm: fsm}
	setter.SetAnnouncer(vaultctlfsm.NewAnnouncer(applier, nil, slog.Default()))

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	orch.RegisterVault(NewVault(vaultID, inst))
	orch.mu.Lock()
	orch.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: true, hasHandle: true})
	orch.mu.Unlock()

	// Wire the lifecycle reconciler so the "does anything recover this?"
	// question can be asked of the real reconcile categories rather than
	// skipped.
	rec := NewVaultLifecycleReconciler(orch, vaultID, inst, "node-A", slog.Default())
	rec.fsm = fsm
	inst.Reconciler = rec
	return orch, vaultID, fsm, applier
}

// TestLostBeginSealAnnounceLeavesChunkSealedLocallyButActiveInFSM reproduces the
// captured divergence deterministically, and pins that nothing reports it.
func TestLostBeginSealAnnounceLeavesChunkSealedLocallyButActiveInFSM(t *testing.T) {
	t.Parallel()
	orch, vaultID, fsm, applier := newLostAnnounceFixture(t)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, makeRecord("one")); err != nil {
		t.Fatalf("AppendToVault: %v", err)
	}
	active := orch.vaults[vaultID].Instance.Chunks.Active()
	if active == nil {
		t.Fatal("no active chunk after append")
	}
	chunkID := active.ID
	if got := fsmState(t, fsm, chunkID); got != chunk.ChunkStateActive {
		t.Fatalf("pre-seal FSM state = %s, want active", got)
	}

	// Every announce from here on is refused.
	applier.failing = true

	sealed, err := orch.SealActive(vaultID)
	if err != nil {
		t.Fatalf("SealActive reported an error; the point is that it does NOT: %v", err)
	}
	if sealed != 1 {
		t.Fatalf("SealActive sealed %d, want 1", sealed)
	}
	requireIdle(t, orch.scheduler, postSealDrainBudget)

	// The three observations from the gastrolog-231ik capture.
	local, err := orch.vaults[vaultID].Instance.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("local Meta: %v", err)
	}
	if !local.Sealed {
		t.Fatal("local chunk is not sealed; the fixture did not reach the state under test")
	}
	if got := fsmState(t, fsm, chunkID); got != chunk.ChunkStateActive {
		t.Errorf("FSM state = %s, want active — this test exists for the case where the announce is LOST", got)
	}

	// And nothing surfaced it: no error from Seal, and the post-seal job
	// completed rather than failing. A chunk that is sealed on disk and Active
	// in replicated truth is a divergence no layer reported.
	for _, j := range orch.scheduler.ListJobs() {
		if j.Name != postSealJobName(vaultID, chunkID) {
			continue
		}
		if snap := j.Snapshot(); snap.Progress != nil && snap.Progress.Status == JobStatusFailed {
			t.Log("post-seal reported failure; the announce loss would at least be visible there")
			return
		}
	}
	t.Log("confirmed: local sealed=true, FSM=active, Seal returned nil, post-seal did not fail")
}

// Once the applier recovers, the manifest must be driven back into agreement
// with the disk. Before this fix nothing re-announced, so the divergence was
// permanent on a running node — that permanence is what made it a durability
// problem rather than a transient.
func TestLostSealAnnounceIsReAnnouncedOnceApplyRecovers(t *testing.T) {
	t.Parallel()
	orch, vaultID, fsm, applier := newLostAnnounceFixture(t)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, makeRecord("one")); err != nil {
		t.Fatalf("AppendToVault: %v", err)
	}
	chunkID := orch.vaults[vaultID].Instance.Chunks.Active().ID

	applier.failing = true
	if _, err := orch.SealActive(vaultID); err != nil {
		t.Fatalf("SealActive: %v", err)
	}
	requireIdle(t, orch.scheduler, postSealDrainBudget)

	// Apply works again — the transient is over.
	applier.failing = false
	appliedBefore := applier.applied

	inst := orch.vaults[vaultID].Instance
	if inst.Reconciler == nil {
		t.Skip("fixture has no reconciler wired; the reconcile path is covered in seal_resume_steady_state_test.go")
	}
	for range 5 {
		inst.Reconciler.ReconcileTick()
	}

	if got := fsmState(t, fsm, chunkID); got != chunk.ChunkStateSealed {
		t.Errorf("FSM state after the transient cleared = %s, want sealed — "+
			"the manifest must be driven back into agreement with the disk", got)
	}
	if applier.applied == appliedBefore {
		t.Error("nothing was re-announced; the divergence would be permanent")
	}
}

// Re-announcing must carry the LOCAL metadata, not placeholders: the manifest
// entry is what retention, replication and grounded reads read, so a recovered
// entry with a zero RecordCount would be a different kind of wrong.
func TestReAnnouncedSealCarriesTheLocalMetadata(t *testing.T) {
	t.Parallel()
	orch, vaultID, fsm, applier := newLostAnnounceFixture(t)

	const records = 5
	for i := range records {
		if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, makeRecord("rec")); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	chunkID := orch.vaults[vaultID].Instance.Chunks.Active().ID

	applier.failing = true
	if _, err := orch.SealActive(vaultID); err != nil {
		t.Fatalf("SealActive: %v", err)
	}
	requireIdle(t, orch.scheduler, postSealDrainBudget)
	applier.failing = false

	local, err := orch.vaults[vaultID].Instance.Chunks.Meta(chunkID)
	if err != nil {
		t.Fatalf("local Meta: %v", err)
	}
	orch.vaults[vaultID].Instance.Reconciler.ReconcileTick()

	e := fsm.Get(chunkID)
	if e == nil {
		t.Fatal("chunk absent from the manifest after re-announce")
	}
	if e.State != chunk.ChunkStateSealed {
		t.Fatalf("state = %s, want sealed", e.State)
	}
	if e.RecordCount != local.RecordCount {
		t.Errorf("manifest RecordCount = %d, want the local %d", e.RecordCount, local.RecordCount)
	}
	if e.Bytes != local.Bytes {
		t.Errorf("manifest Bytes = %d, want the local %d", e.Bytes, local.Bytes)
	}
	if !e.WriteEnd.Equal(local.WriteEnd) {
		t.Errorf("manifest WriteEnd = %s, want the local %s", e.WriteEnd, local.WriteEnd)
	}
}

// The chunk currently open for writes is Active in the manifest because it IS
// active. Re-announcing it would seal a chunk still being appended to.
func TestSealAnnounceDivergenceLeavesTheOpenActiveChunkAlone(t *testing.T) {
	t.Parallel()
	orch, vaultID, fsm, _ := newLostAnnounceFixture(t)

	if err := orch.AppendToVault(vaultID, chunk.ChunkID{}, makeRecord("one")); err != nil {
		t.Fatalf("AppendToVault: %v", err)
	}
	active := orch.vaults[vaultID].Instance.Chunks.Active()
	if active == nil {
		t.Fatal("no active chunk")
	}

	for range 3 {
		orch.vaults[vaultID].Instance.Reconciler.ReconcileTick()
	}

	if got := fsmState(t, fsm, active.ID); got != chunk.ChunkStateActive {
		t.Errorf("the open active chunk was moved to %s; it must stay active", got)
	}
}
