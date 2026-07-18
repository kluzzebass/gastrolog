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
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
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

// TestDeferralStreakRaiseNamesMatchedChunkCount pins Fix 3b: the alarm
// detail must say how much is waiting — chunks past policy, not bytes
// (bytes are never captured on this path). sweepMatchedChunks is set by
// sweep() itself; this test drives it directly like
// TestDeferralStreakRaisesAtThresholdAndClearsOnProgress does for the
// other scratch flags.
func TestDeferralStreakRaiseNamesMatchedChunkCount(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	vaultID := glid.New()
	r := &retentionRunner{
		vaultID:   vaultID,
		vaultName: "first-vault",
		orch:      &Orchestrator{alerts: sink},
	}

	for range retentionDeferralAlarmAfter - 1 {
		r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	r.mu.Lock()
	r.sweepMatchedChunks = 5
	r.mu.Unlock()
	r.noteFanOutDeferral("destination vault second-vault is at its size budget")
	r.finishSweepDeferralState()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.raises) != 1 {
		t.Fatalf("want exactly 1 raise, got %d", len(sink.raises))
	}
	got := sink.raises[0]
	if !strings.Contains(got, "5 chunks past policy are waiting") {
		t.Errorf("alarm detail must name the matched chunk count; got: %s", got)
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

// TestDeferralStreakResetsOnDeleteDispositionProgress documents that the
// progress signal is disposition-agnostic: tryRetainChunk now calls
// noteFanOutProgress unconditionally after ANY successful
// applyRetentionDispositionToChunk, not just a route-disposition fan-out.
// The alarm's response text tells the operator to flip disposition to
// delete; once flipped, a delete-disposition sweep that destroys chunks and
// frees space is progress by the same definition a route fan-out is, and
// must clear the alarm the same way. The runner method itself carries no
// disposition parameter — this test drives the identical call a
// delete-disposition sweep now makes, to pin that semantics rather than
// duplicate a mechanism already covered above.
func TestDeferralStreakResetsOnDeleteDispositionProgress(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	r := &retentionRunner{
		vaultID:   glid.New(),
		vaultName: "delete-vault",
		orch:      &Orchestrator{alerts: sink},
	}
	for range retentionDeferralAlarmAfter {
		r.noteFanOutDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	// Simulates a delete-disposition sweep successfully destroying a chunk:
	// applyRetentionDispositionToChunk returns true (disposition == delete
	// is a no-op that always succeeds) and tryRetainChunk now notes progress
	// unconditionally rather than gating on disposition == route.
	r.noteFanOutProgress()
	r.finishSweepDeferralState()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.clears) == 0 {
		t.Fatal("delete-disposition progress must clear the alarm same as route-disposition progress")
	}
}

// TestRetentionSweepAllClearsAlarmOnRunnerGC pins the other alarm-clearing
// path: when a runner is garbage-collected because its vault fell out of
// config (or its placement moved off this node), nothing else ever calls
// finishSweepDeferralState for it again — so if a deferral alarm was
// standing when the runner was GC'd, only the GC path itself can clear it.
// Mirrors disk_guard.go retainVaultGuards, which clears its own alarms on
// prune for the same reason.
func TestRetentionSweepAllClearsAlarmOnRunnerGC(t *testing.T) {
	t.Parallel()

	sink := &recordingSink{}
	o := newTestOrch(t, Config{LocalNodeID: "node-A"})
	o.alerts = sink
	// The store needs at least one entity or Load returns (nil, nil) and
	// retentionSweepAll bails out before reaching the GC loop at all — so
	// seed an unrelated vault. It never registers on this orchestrator
	// (o.vaults stays empty), so it never marks any key active; the
	// pre-seeded runner below is still the only thing the GC loop sees.
	store := sysmem.NewStore()
	_ = store.PutVault(t.Context(), system.VaultConfig{ID: glid.New(), Name: "other", Type: system.VaultTypeMemory})
	o.sysLoader = &transitionSystemLoader{store: store}

	vaultID := glid.New()
	o.mu.Lock()
	if o.retention == nil {
		o.retention = make(map[string]*retentionRunner)
	}
	o.retention[vaultID.String()] = &retentionRunner{vaultID: vaultID, vaultName: "gc-vault"}
	o.mu.Unlock()

	o.retentionSweepAll()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	found := false
	for _, c := range sink.clears {
		if c == alarmRetentionRouteDeferred+"|"+vaultID.String() {
			found = true
		}
	}
	if !found {
		t.Fatalf("runner GC must clear the deferral alarm; clears=%v", sink.clears)
	}
	o.mu.RLock()
	_, stillPresent := o.retention[vaultID.String()]
	o.mu.RUnlock()
	if stillPresent {
		t.Fatal("runner must have been pruned")
	}
}
