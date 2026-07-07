package orchestrator

import (
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
)

type alertSpy struct {
	mu  sync.Mutex
	set map[string]string
}

func (s *alertSpy) Set(id string, _ alert.Severity, _, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.set == nil {
		s.set = map[string]string{}
	}
	s.set[id] = msg
}

func (s *alertSpy) Clear(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.set, id)
}

func (s *alertSpy) active() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.set)
}

// TestLeaderlessAlarmDelayOnAndClear pins the EEMUA-style behavior: a vault
// resolving to no leader must NOT alarm inside the delay-on window (mid-flap
// transients are self-healing), must alarm once the window elapses, and must
// clear the moment a leader resolves again.
func TestLeaderlessAlarmDelayOnAndClear(t *testing.T) {
	t.Parallel()
	spy := &alertSpy{}
	o := &Orchestrator{alerts: spy}
	vaultID := glid.New()
	t0 := time.Now()
	leaderless := map[glid.GLID]string{vaultID: "first-vault"}

	// First sighting starts the clock; no alarm.
	o.updateLeaderlessAlarms(t0, leaderless)
	if spy.active() != 0 {
		t.Fatal("alarm raised on first leaderless tick — delay-on window ignored")
	}

	// Still inside the window on the next sweep tick.
	o.updateLeaderlessAlarms(t0.Add(15*time.Second), leaderless)
	if spy.active() != 0 {
		t.Fatal("alarm raised inside the delay-on window")
	}

	// Window elapsed: alarm.
	o.updateLeaderlessAlarms(t0.Add(vaultLeaderlessAlarmAfter+time.Second), leaderless)
	if spy.active() != 1 {
		t.Fatal("sustained leaderless vault must raise the alarm")
	}

	// Leader resolves: immediate clear, and the clock resets — a fresh
	// leaderless episode starts a fresh window.
	o.updateLeaderlessAlarms(t0.Add(vaultLeaderlessAlarmAfter+16*time.Second), map[glid.GLID]string{})
	if spy.active() != 0 {
		t.Fatal("alarm must clear when the leader resolves")
	}
	o.updateLeaderlessAlarms(t0.Add(vaultLeaderlessAlarmAfter+31*time.Second), leaderless)
	if spy.active() != 0 {
		t.Fatal("new leaderless episode must restart the delay-on window, not alarm instantly")
	}
}

// TestLeaderlessAlarmTracksMultipleVaults verifies independent per-vault
// clocks and that clearing one vault leaves the other's alarm standing.
func TestLeaderlessAlarmTracksMultipleVaults(t *testing.T) {
	t.Parallel()
	spy := &alertSpy{}
	o := &Orchestrator{alerts: spy}
	a, b := glid.New(), glid.New()
	t0 := time.Now()

	o.updateLeaderlessAlarms(t0, map[glid.GLID]string{a: "A"})
	o.updateLeaderlessAlarms(t0.Add(30*time.Second), map[glid.GLID]string{a: "A", b: "B"})
	o.updateLeaderlessAlarms(t0.Add(vaultLeaderlessAlarmAfter+time.Second), map[glid.GLID]string{a: "A", b: "B"})
	if spy.active() != 1 {
		t.Fatalf("only vault A's window has elapsed; active=%d, want 1", spy.active())
	}

	// B's window elapses too; then A resolves.
	o.updateLeaderlessAlarms(t0.Add(30*time.Second+vaultLeaderlessAlarmAfter+time.Second), map[glid.GLID]string{a: "A", b: "B"})
	if spy.active() != 2 {
		t.Fatalf("both vaults sustained leaderless; active=%d, want 2", spy.active())
	}
	o.updateLeaderlessAlarms(t0.Add(31*time.Second+vaultLeaderlessAlarmAfter+time.Second), map[glid.GLID]string{b: "B"})
	if spy.active() != 1 {
		t.Fatalf("A resolved, B still leaderless; active=%d, want 1", spy.active())
	}
}
