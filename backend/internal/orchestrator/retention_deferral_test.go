package orchestrator

// A route-disposition vault whose fan-out is deferred sweep after sweep
// must raise ONE alarm that names the deadlock — the incident's operator
// signal was three unrelated warnings from three subsystems. The streak
// is a pure count of consecutive deferred sweeps;
// nothing persists across restart.

import (
	"os"
	"path/filepath"
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
		r.noteRetentionDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	sink.mu.Lock()
	raised := len(sink.raises)
	sink.mu.Unlock()
	if raised != 0 {
		t.Fatalf("below the threshold no alarm may raise; got %d", raised)
	}

	// Third consecutive deferral: raise, naming vault and cause.
	r.noteRetentionDeferral("destination vault second-vault is at its max-size bound")
	r.finishSweepDeferralState()
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.raises) != 1 {
		t.Fatalf("want exactly 1 raise at the threshold, got %d", len(sink.raises))
	}
	got := sink.raises[0]
	if !strings.HasPrefix(got, alarmRetentionDeferred+"|"+vaultID.String()+"|") {
		t.Errorf("raise must be typed and instance-keyed by vault: %s", got)
	}
	for _, want := range []string{"first-vault", "max-size bound", "3 consecutive"} {
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
		r.noteRetentionDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	r.mu.Lock()
	r.sweepMatchedChunks = 5
	r.mu.Unlock()
	r.noteRetentionDeferral("destination vault second-vault is at its max-size bound")
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
		r.noteRetentionDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	// A sweep that fully routes a chunk clears the alarm and resets.
	r.noteRetentionProgress()
	r.finishSweepDeferralState()

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if len(sink.clears) == 0 {
		t.Fatal("progress must clear the alarm")
	}
	// A single fresh deferral after recovery must not re-raise.
	r.noteRetentionDeferral("drain gate engaged (node below its disk floor)")
	r.finishSweepDeferralState()
	if len(sink.raises) != 1 {
		t.Fatalf("streak must reset on progress; raises=%d", len(sink.raises))
	}
}

// TestDeferralStreakResetsOnDeleteDispositionProgress documents that the
// progress signal is disposition-agnostic: tryRetainChunk now calls
// noteRetentionProgress unconditionally after ANY successful
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
		r.noteRetentionDeferral("drain gate engaged (node below its disk floor)")
		r.finishSweepDeferralState()
	}
	// Simulates a delete-disposition sweep successfully destroying a chunk:
	// applyRetentionDispositionToChunk returns true (disposition == delete
	// is a no-op that always succeeds) and tryRetainChunk now notes progress
	// unconditionally rather than gating on disposition == route.
	r.noteRetentionProgress()
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
	o.setSystemLoader(&transitionSystemLoader{store: store})

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
		if c == alarmRetentionDeferred+"|"+vaultID.String() {
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

// TestRetentionRouteDeferredStringFullyRenamed proves the alarm rename
// (retention-route-deferred -> retention-deferred, done because transfer
// disposition now shares the same deferral streak and alarm as route)
// left no trace in the Go source tree. Walks every .go
// file under backend/ except api/gen (generated protobuf, out of scope for
// a hand-rename) and this test's own file (which must legitimately name
// the old identifier to build the needle and describe what it's checking).
// The needle is built by concatenation so this file doesn't trip its own
// scan by containing the literal string.
func TestRetentionRouteDeferredStringFullyRenamed(t *testing.T) {
	t.Parallel()

	// backend/internal/orchestrator -> backend
	backendRoot, err := filepath.Abs(filepath.Join(".", "..", ".."))
	if err != nil {
		t.Fatalf("resolve backend root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(backendRoot, "go.mod")); err != nil {
		t.Fatalf("resolved path %q does not look like the backend module root: %v", backendRoot, err)
	}
	selfPath, err := filepath.Abs("retention_deferral_test.go")
	if err != nil {
		t.Fatalf("resolve own path: %v", err)
	}

	needle := "retention" + "-" + "route" + "-" + "deferred"
	oldSymbol := "alarmRetentionRoute" + "Deferred"
	oldNoteDeferral := "noteFanOut" + "Deferral"
	oldNoteProgress := "noteFanOut" + "Progress"

	var offenders []string
	err = filepath.WalkDir(backendRoot, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "gen" && filepath.Base(filepath.Dir(path)) == "api" {
				return filepath.SkipDir // generated protobuf
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		abs, err := filepath.Abs(path)
		if err != nil {
			return err
		}
		if abs == selfPath {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		content := string(data)
		for _, s := range []string{needle, oldSymbol, oldNoteDeferral, oldNoteProgress} {
			if strings.Contains(content, s) {
				offenders = append(offenders, path+" contains "+s)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk backend tree: %v", err)
	}
	if len(offenders) > 0 {
		t.Fatalf("rename incomplete — %d reference(s) to the old alarm/method names survive:\n%s",
			len(offenders), strings.Join(offenders, "\n"))
	}
}
