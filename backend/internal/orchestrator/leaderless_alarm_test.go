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

func (s *alertSpy) Raise(typeID, instanceKey, detail string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.set == nil {
		s.set = map[string]string{}
	}
	s.set[spyAlarmID(typeID, instanceKey)] = detail
}

func (s *alertSpy) Clear(typeID, instanceKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.set, spyAlarmID(typeID, instanceKey))
}

// spyAlarmID mirrors the collector's type:instance ID composition.
func spyAlarmID(typeID, instanceKey string) string {
	if instanceKey == "" {
		return typeID
	}
	return typeID + ":" + instanceKey
}

func (s *alertSpy) active() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.set)
}

// leaderlessDelayOn is the catalog's delay-on window for vault-leaderless —
// the tests read it from the registry so they pin "the sweep rides the
// catalog", not a copied constant.
func leaderlessDelayOn(t *testing.T) time.Duration {
	t.Helper()
	typ, ok := alert.TypeByID("vault-leaderless")
	if !ok || typ.DelayOn <= 0 {
		t.Fatal("vault-leaderless must carry a catalog DelayOn")
	}
	return typ.DelayOn
}

// leaderlessFixture is an orchestrator wired to a real alert.Collector on a
// deterministic clock: the delay-on window now lives in the collector, so
// the tests advance the collector's clock, never wall time.
func leaderlessFixture(t *testing.T) (*Orchestrator, *alert.Collector, func(time.Duration)) {
	t.Helper()
	var mu sync.Mutex
	now := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	}
	advance := func(d time.Duration) {
		mu.Lock()
		now = now.Add(d)
		mu.Unlock()
	}
	c := alert.NewWithClock(clock)
	return &Orchestrator{alerts: c}, c, advance
}

// TestLeaderlessAlarmDelayOnAndClear pins the EEMUA-style behavior: a vault
// resolving to no leader must NOT alarm inside the delay-on window (mid-flap
// transients are self-healing), must alarm once the window elapses, and must
// clear the moment a leader resolves again. The window is the catalog's
// DelayOn, enforced by the collector — the sweep just reports the raw
// condition each tick (gastrolog-4wvxqh).
func TestLeaderlessAlarmDelayOnAndClear(t *testing.T) {
	t.Parallel()
	o, c, advance := leaderlessFixture(t)
	delayOn := leaderlessDelayOn(t)
	vaultID := glid.New()
	leaderless := map[glid.GLID]string{vaultID: "first-vault"}

	// First sighting starts the window; no alarm.
	o.updateLeaderlessAlarms(leaderless)
	if c.Count() != 0 {
		t.Fatal("alarm raised on first leaderless tick — delay-on window ignored")
	}

	// Still inside the window on the next sweep tick.
	advance(15 * time.Second)
	o.updateLeaderlessAlarms(leaderless)
	if c.Count() != 0 {
		t.Fatal("alarm raised inside the delay-on window")
	}

	// Window elapsed: alarm.
	advance(delayOn - 14*time.Second)
	o.updateLeaderlessAlarms(leaderless)
	if c.Count() != 1 {
		t.Fatal("sustained leaderless vault must raise the alarm")
	}

	// Leader resolves: immediate clear (no DelayOff in the catalog), and a
	// fresh leaderless episode starts a fresh window.
	advance(15 * time.Second)
	o.updateLeaderlessAlarms(map[glid.GLID]string{})
	if c.Count() != 0 {
		t.Fatal("alarm must clear when the leader resolves")
	}
	advance(15 * time.Second)
	o.updateLeaderlessAlarms(leaderless)
	if c.Count() != 0 {
		t.Fatal("new leaderless episode must restart the delay-on window, not alarm instantly")
	}
}

// TestLeaderlessAlarmSustainedWithoutReRaise pins the lazy-evaluation
// choice: even if the sweep stopped re-raising (it does re-raise every
// tick, but activation must not DEPEND on that), a persisting condition
// activates once the window elapses — surfaced by the next read.
func TestLeaderlessAlarmSustainedWithoutReRaise(t *testing.T) {
	t.Parallel()
	o, c, advance := leaderlessFixture(t)
	delayOn := leaderlessDelayOn(t)
	vaultID := glid.New()

	o.updateLeaderlessAlarms(map[glid.GLID]string{vaultID: "quiet-vault"})
	advance(delayOn + time.Second)
	alarms := c.Standing()
	if len(alarms) != 1 {
		t.Fatalf("persisting condition past delay-on must activate on read; active=%d", len(alarms))
	}
	if alarms[0].ID != "vault-leaderless:"+vaultID.String() {
		t.Fatalf("alarm ID = %q", alarms[0].ID)
	}
}

// TestLeaderlessAlarmTracksMultipleVaults verifies independent per-vault
// windows and that clearing one vault leaves the other's alarm standing.
func TestLeaderlessAlarmTracksMultipleVaults(t *testing.T) {
	t.Parallel()
	o, c, advance := leaderlessFixture(t)
	delayOn := leaderlessDelayOn(t)
	a, b := glid.New(), glid.New()

	o.updateLeaderlessAlarms(map[glid.GLID]string{a: "A"})
	advance(30 * time.Second)
	o.updateLeaderlessAlarms(map[glid.GLID]string{a: "A", b: "B"})
	advance(delayOn - 29*time.Second)
	o.updateLeaderlessAlarms(map[glid.GLID]string{a: "A", b: "B"})
	if c.Count() != 1 {
		t.Fatalf("only vault A's window has elapsed; active=%d, want 1", c.Count())
	}

	// B's window elapses too; then A resolves.
	advance(30 * time.Second)
	o.updateLeaderlessAlarms(map[glid.GLID]string{a: "A", b: "B"})
	if c.Count() != 2 {
		t.Fatalf("both vaults sustained leaderless; active=%d, want 2", c.Count())
	}
	o.updateLeaderlessAlarms(map[glid.GLID]string{b: "B"})
	if c.Count() != 1 {
		t.Fatalf("A resolved, B still leaderless; active=%d, want 1", c.Count())
	}
}

// TestLeaderlessAlarmClearsWhenVaultLeavesRegistry pins the departure diff:
// a vault whose instance is gone (removed, transferred away) stops being
// reported at all — its standing alarm must clear rather than stand
// forever, exactly as the old per-vault bookkeeping did.
func TestLeaderlessAlarmClearsWhenVaultLeavesRegistry(t *testing.T) {
	t.Parallel()
	o, c, advance := leaderlessFixture(t)
	delayOn := leaderlessDelayOn(t)
	vaultID := glid.New()

	o.updateLeaderlessAlarms(map[glid.GLID]string{vaultID: "doomed"})
	advance(delayOn + time.Second)
	o.updateLeaderlessAlarms(map[glid.GLID]string{vaultID: "doomed"})
	if c.Count() != 1 {
		t.Fatal("sustained leaderless vault must alarm")
	}
	// The vault disappears from the sweep's outcome entirely.
	o.updateLeaderlessAlarms(map[glid.GLID]string{})
	if c.Count() != 0 {
		t.Fatal("alarm for a departed vault must clear")
	}
}
