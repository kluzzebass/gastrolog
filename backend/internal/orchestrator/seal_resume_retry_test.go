package orchestrator

// What happens when the resumed post-seal never succeeds.
//
// reconcileSealingResume runs on the retention tick, whose cadence an operator
// can set as low as one second (CronEvery returns "* * * * * *"). A chunk whose
// post-seal fails permanently stays in Sealing, so the category re-selects it on
// every pass. The RunOnceIfAbsent claim does NOT bound that: it prevents an
// overlapping duplicate, then frees the name when the job completes — including
// when it completes by failing.
//
// Measured before it was bounded: 10 passes produced 10 rebuild attempts and 10
// log lines. At a one-second cadence that is a GLCB rebuild and re-announce
// every second, forever, for a chunk that will never succeed.

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func TestSealResumeStopsRetryingAfterTheBudget(t *testing.T) {
	t.Parallel()
	rec, idSealing := sealResumeFixture(t, slog.Default())
	scheduled := recordScheduled(rec)

	for range 10 {
		rec.ReconcileTick()
	}

	if len(*scheduled) != maxSealResumeAttempts {
		t.Fatalf("post-seal re-driven %d times over 10 passes, want %d — "+
			"an unbounded retry rebuilds the GLCB every tick",
			len(*scheduled), maxSealResumeAttempts)
	}
	for i, id := range *scheduled {
		if id != idSealing {
			t.Fatalf("attempt %d resumed %s, want the stranded chunk %s", i, id, idSealing)
		}
	}
}

// Giving up silently would trade a busy loop for an invisible one: the chunk is
// still neither writable nor durable-complete, and nothing else reports it.
func TestSealResumeRaisesAnAlarmWhenItGivesUp(t *testing.T) {
	t.Parallel()
	rec, idSealing := sealResumeFixture(t, slog.Default())
	sink := &recordingSink{}
	rec.orch = &Orchestrator{alerts: sink}
	recordScheduled(rec)

	for range maxSealResumeAttempts + 3 {
		rec.ReconcileTick()
	}

	raised := countPrefixed(sink.raises, "seal-stranded|"+idSealing.String()+"|")
	if raised == 0 {
		t.Fatal("gave up on a stranded chunk without raising an alarm")
	}
	// Raised once, not once per pass: the alarm is standing, and re-raising it
	// every tick is the log-spam problem wearing a different hat.
	if raised > 1 {
		t.Errorf("alarm raised %d times, want 1 — a standing condition re-raised every pass", raised)
	}
}

// The budget must reset when the chunk settles, or a chunk id that strands,
// recovers, and strands again later would never be retried the second time.
func TestSealResumeBudgetResetsWhenTheChunkLeavesSealing(t *testing.T) {
	t.Parallel()
	rec, idSealing := sealResumeFixture(t, slog.Default())
	sink := &recordingSink{}
	rec.orch = &Orchestrator{alerts: sink}
	scheduled := recordScheduled(rec)

	for range maxSealResumeAttempts + 2 {
		rec.ReconcileTick()
	}
	spent := len(*scheduled)

	// The chunk settles: promote it out of Sealing in the FSM.
	promoteToSealed(t, rec.fsm, idSealing)
	rec.ReconcileTick()

	if got := countPrefixed(sink.clears, "seal-stranded|"+idSealing.String()); got == 0 {
		t.Error("chunk left Sealing but its alarm was never cleared")
	}
	if rec.sealResumeAttempts[idSealing] != 0 {
		t.Errorf("retry budget for a settled chunk = %d, want 0 (reset)", rec.sealResumeAttempts[idSealing])
	}
	if len(*scheduled) != spent {
		t.Errorf("a settled chunk was re-driven: %d attempts, want the %d already spent", len(*scheduled), spent)
	}
}

// The restore path stays unbounded on purpose: a snapshot install is a fresh
// start after a crash, not a repetition of work already proven not to help.
func TestSnapshotResumeIsNotSubjectToTheRetryBudget(t *testing.T) {
	t.Parallel()
	rec, idSealing := sealResumeFixture(t, slog.Default())
	scheduled := recordScheduled(rec)

	const restores = maxSealResumeAttempts + 4
	for range restores {
		rec.ReconcileFromSnapshot(rec.fsm)
	}

	if len(*scheduled) != restores {
		t.Fatalf("restore pass resumed %d times over %d restores, want %d — "+
			"the steady-state budget must not leak into it", len(*scheduled), restores, restores)
	}
	if (*scheduled)[0] != idSealing {
		t.Fatalf("restore resumed %s, want %s", (*scheduled)[0], idSealing)
	}
}

// countPrefixed counts recordingSink entries for one alarm instance.
func countPrefixed(entries []string, prefix string) int {
	n := 0
	for _, e := range entries {
		if strings.HasPrefix(e, prefix) {
			n++
		}
	}
	return n
}

// promoteToSealed settles a chunk in the FSM, the way a successful post-seal
// does, so the retry budget's reset can be observed.
func promoteToSealed(t *testing.T, fsm *vaultctlfsm.FSM, id chunk.ChunkID) {
	t.Helper()
	now := time.Now()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 1, 1, now, now, now, false, now)}); err != nil {
		t.Fatalf("promote %s to sealed: %v", id, err)
	}
}
