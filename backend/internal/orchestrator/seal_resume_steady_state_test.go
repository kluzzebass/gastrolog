package orchestrator

// Seal resumption on a RUNNING node (gastrolog-v6nf71).
//
// The Sealing → Sealed half of a seal rides a scheduled one-time job. Until
// this category existed, resumeSealingFromFSM had exactly one caller —
// ReconcileFromSnapshot, fired only by the vault-ctl FSM's after-restore hook —
// so a post-seal that failed or never got scheduled left its manifest entry in
// Sealing until the node restarted. The chunk is then neither writable nor
// durable-complete, and nothing else picks it up: the stale-fsm sweep skips
// exactly the chunks the leader holds locally.
//
// TestReconcileFromSnapshotResumesSealingChunks covers the restore path; these
// cover the steady state. Idempotency of a resume that lands while a healthy
// post-seal is still running is pinned by TestPostSealDoesNotRunTwiceForOneChunk
// (the RunOnceIfAbsent claim), which is what makes running this every tick safe.

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// sealResumeFixture builds a reconciler holding one Sealing entry whose chunk
// is present and sealed on disk — the exact state with no recovery path before
// this change — plus an Active and a Sealed entry that must be left alone.
func sealResumeFixture(t *testing.T, logger *slog.Logger) (*VaultLifecycleReconciler, chunk.ChunkID) {
	t.Helper()
	fsm := vaultctlfsm.New()
	now := time.Now()

	idActive := chunk.NewChunkID()
	idSealing := chunk.NewChunkID()
	idSealed := chunk.NewChunkID()
	for _, id := range []chunk.ChunkID{idActive, idSealing, idSealed} {
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
			t.Fatalf("create %s: %v", id, err)
		}
	}
	for _, id := range []chunk.ChunkID{idSealing, idSealed} {
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalBeginSeal(id)}); err != nil {
			t.Fatalf("begin-seal %s: %v", id, err)
		}
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(idSealed, now, 1, 1, now, now, now, false, now)}); err != nil {
		t.Fatalf("seal-chunk: %v", err)
	}

	cm := &reconcilerFakeChunkManager{}
	cm.chunks = []chunk.ChunkMeta{
		{ID: idActive, Sealed: false},
		{ID: idSealing, Sealed: true},
		{ID: idSealed, Sealed: true},
	}
	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(nil, vaultInst.VaultID, vaultInst, "node-A", logger)
	rec.fsm = fsm
	return rec, idSealing
}

func recordScheduled(rec *VaultLifecycleReconciler) *[]chunk.ChunkID {
	var scheduled []chunk.ChunkID
	rec.postSealHook = func(_ glid.GLID, _ chunk.ChunkManager, id chunk.ChunkID) {
		scheduled = append(scheduled, id)
	}
	return &scheduled
}

// The periodic backstop must re-drive a stranded seal. This is the case with no
// upstream edge at all: a post-seal job that failed under a stable leader, with
// no restart, no snapshot install and no leadership change to wake anything.
func TestReconcileTickResumesStrandedSealingChunk(t *testing.T) {
	t.Parallel()
	rec, idSealing := sealResumeFixture(t, slog.Default())
	scheduled := recordScheduled(rec)

	rec.ReconcileTick()

	if len(*scheduled) != 1 {
		t.Fatalf("post-seal scheduled %d times (%v), want 1 — only the Sealing chunk", len(*scheduled), *scheduled)
	}
	if (*scheduled)[0] != idSealing {
		t.Errorf("resumed %s, want the Sealing chunk %s", (*scheduled)[0], idSealing)
	}
}

// A node that just gained vault-ctl leadership may be holding a chunk stranded
// mid-seal. It must not wait out a tick to notice.
func TestMembershipCatchupResumesStrandedSealingChunk(t *testing.T) {
	t.Parallel()
	rec, idSealing := sealResumeFixture(t, slog.Default())
	scheduled := recordScheduled(rec)

	rec.ReconcileMembershipCatchup()

	if len(*scheduled) != 1 || (*scheduled)[0] != idSealing {
		t.Fatalf("lead-gained resumed %v, want exactly [%s]", *scheduled, idSealing)
	}
}

// The isolated Sweep* entry point exists for targeted recovery and gathers its
// own view, like every other category's.
func TestSweepSealingResumeIsolatedEntryPoint(t *testing.T) {
	t.Parallel()
	rec, idSealing := sealResumeFixture(t, slog.Default())
	scheduled := recordScheduled(rec)

	rec.SweepSealingResume()

	if len(*scheduled) != 1 || (*scheduled)[0] != idSealing {
		t.Fatalf("sweep resumed %v, want exactly [%s]", *scheduled, idSealing)
	}
}

// Only the node that can propose the follow-on CmdSealChunk should schedule the
// work — same role gate as every other leader-side category here. A follower
// firing this would schedule a GLCB build whose announcement it cannot commit.
func TestSealResumeSkippedWithoutRaftLeadership(t *testing.T) {
	t.Parallel()
	rec, _ := sealResumeFixture(t, slog.Default())
	rec.vaultInst.HasRaftLeader = func() bool { return false }
	scheduled := recordScheduled(rec)

	rec.ReconcileTick()

	if len(*scheduled) != 0 {
		t.Errorf("a node without vault-ctl leadership scheduled %v; want nothing", *scheduled)
	}
}

// Steady state must stay quiet about shapes that are ordinary in flight.
//
// An entry is Sealing for the entire normal duration of PostSealProcess, and
// the local sealed flag is only set once sealActiveLocked closes the files — so
// a tick landing inside a healthy seal sees "Sealing, not sealed on disk". The
// restore path warns about that (there it means the crash tore the seal); a
// tick that warned would report healthy work as an anomaly on every pass,
// forever. That noise is what buries real signal.
func TestSealResumeDoesNotWarnAboutInFlightSeals(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	rec, _ := sealResumeFixture(t, slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Unsealed on disk: the window between CmdBeginSeal applying and the local
	// files being closed.
	cm, ok := rec.vaultInst.Chunks.(*reconcilerFakeChunkManager)
	if !ok {
		t.Fatalf("fixture chunk manager is %T, want *reconcilerFakeChunkManager", rec.vaultInst.Chunks)
	}
	for i := range cm.chunks {
		cm.chunks[i].Sealed = false
	}
	scheduled := recordScheduled(rec)

	rec.ReconcileTick()

	if len(*scheduled) != 0 {
		t.Errorf("resumed %v for a chunk not yet sealed on disk; want nothing", *scheduled)
	}
	if got := out.String(); strings.Contains(got, "not sealed on disk") {
		t.Errorf("steady-state pass warned about a seal in flight; log was:\n%s", got)
	}
}

// The no-local-chunk case belongs to the stale-fsm sweep and its grace period
// (gastrolog-1huz5), not here: this path cannot rebuild a GLCB it has no bytes
// for. Steady state skips it silently rather than warning every tick about a
// condition another category is already timing out.
func TestSealResumeSkipsAndStaysQuietWhenChunkIsAbsentLocally(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	rec, _ := sealResumeFixture(t, slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))

	cm, ok := rec.vaultInst.Chunks.(*reconcilerFakeChunkManager)
	if !ok {
		t.Fatalf("fixture chunk manager is %T, want *reconcilerFakeChunkManager", rec.vaultInst.Chunks)
	}
	cm.chunks = nil // leader holds no local chunks at all
	scheduled := recordScheduled(rec)

	rec.ReconcileTick()

	if len(*scheduled) != 0 {
		t.Errorf("resumed %v with no local chunk to assemble from; want nothing", *scheduled)
	}
	if got := out.String(); strings.Contains(got, "cannot resume") {
		t.Errorf("steady-state pass warned about a case the stale-fsm sweep owns; log was:\n%s", got)
	}
}

// The restore path keeps its loud reporting: there, both shapes mean the crash
// left something this path cannot repair, it happened once, and an operator
// needs to see it. Pinning this separately stops a later "quiet it down" from
// silencing both callers at once.
func TestSnapshotResumeStillWarnsOnUnrecoverableSealing(t *testing.T) {
	t.Parallel()
	var out syncBuffer
	rec, _ := sealResumeFixture(t, slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))

	cm, ok := rec.vaultInst.Chunks.(*reconcilerFakeChunkManager)
	if !ok {
		t.Fatalf("fixture chunk manager is %T, want *reconcilerFakeChunkManager", rec.vaultInst.Chunks)
	}
	cm.chunks = nil
	recordScheduled(rec)

	rec.ReconcileFromSnapshot(rec.fsm)

	if got := out.String(); !strings.Contains(got, "cannot resume") {
		t.Errorf("restore path went quiet about an unrecoverable Sealing entry; log was:\n%s", got)
	}
}
