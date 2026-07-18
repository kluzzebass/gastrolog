package orchestrator

// gastrolog-5ct2av: a route-disposition vault whose fan-out is deferred
// sweep after sweep must raise ONE alarm that names the deadlock — the
// incident's operator signal was three unrelated warnings from three
// subsystems. The streak is a pure count of consecutive deferred sweeps;
// nothing persists across restart.

import (
	"strings"
	"sync"
	"testing"

	"gastrolog/internal/glid"
)

// recordingSink is a minimal alert.Sink capturing raises and clears.
type recordingSink struct {
	mu     sync.Mutex
	raises []string // typeID + "|" + instanceKey + "|" + detail
	clears []string // typeID + "|" + instanceKey
}

func (s *recordingSink) Raise(typeID, instanceKey, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.raises = append(s.raises, typeID+"|"+instanceKey+"|"+detail)
}

func (s *recordingSink) Clear(typeID, instanceKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clears = append(s.clears, typeID+"|"+instanceKey)
}

func TestDeferralStreakRaisesAtThresholdAndClearsOnProgress(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	vaultID := glid.New()
	r := &retentionRunner{
		vaultID:   vaultID,
		vaultName: "first-vault",
		orch:      &Orchestrator{alerts: sink},
	}

	// Two deferred sweeps: below the threshold, no raise.
	for range retentionDeferralAlarmAfter - 1 {
		r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	sink.mu.Lock()
	raised := len(sink.raises)
	sink.mu.Unlock()
	if raised != 0 {
		t.Fatalf("below the threshold no alarm may raise; got %d", raised)
	}

	// Third consecutive deferral: raise, naming vault and cause.
	r.noteFanOutDeferral("destination vault second-vault is at its size budget")
	r.finishSweepDeferralState()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.raises) != 1 {
		t.Fatalf("want exactly 1 raise at the threshold, got %d", len(sink.raises))
	}
	got := sink.raises[0]
	if !strings.HasPrefix(got, alarmRetentionRouteDeferred+"|"+vaultID.String()+"|") {
		t.Errorf("raise must be typed and instance-keyed by vault: %s", got)
	}
	for _, want := range []string{"first-vault", "size budget", "3 consecutive"} {
		if !strings.Contains(got, want) {
			t.Errorf("alarm detail must contain %q; got: %s", want, got)
		}
	}
}

func TestDeferralStreakResetsOnRoutedChunk(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	r := &retentionRunner{
		vaultID:   glid.New(),
		vaultName: "first-vault",
		orch:      &Orchestrator{alerts: sink},
	}
	for range retentionDeferralAlarmAfter {
		r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	// A sweep that fully routes a chunk clears the alarm and resets.
	r.noteFanOutProgress()
	r.finishSweepDeferralState()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.clears) == 0 {
		t.Fatal("progress must clear the alarm")
	}
	// A single fresh deferral after recovery must not re-raise.
	r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
	r.finishSweepDeferralState()
	if len(sink.raises) != 1 {
		t.Fatalf("streak must reset on progress; raises=%d", len(sink.raises))
	}
}
