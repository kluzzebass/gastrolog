package orchestrator

// gastrolog-5yfaqj: refusal generalizes from max-size to every retention
// policy bound. This file covers the disk-guard-level plumbing for the two
// new causes (age, chunk-count) — the setter/getter pair the retention
// runner's post-sweep verdict drives, the shared vault-bound-capped alarm
// with its per-cause entity key, vaultAdmissionCauses/vaultAdmissionGate
// wiring (local and remote-peer), the NodeStats broadcast listers, and
// retainVaultGuards' alarm cleanup on prune. The retention-runner side of
// the VIOLATION PREDICATE (swept-and-failed-to-clear) is covered in
// retention_bound_refusal_test.go — these tests drive the guard's setters
// directly, standing in for whatever verdict a sweep would have reported.

import (
	"errors"
	"testing"

	"gastrolog/internal/glid"
)

// TestVaultAgeBoundCapAndAlarm pins the basic engage/release cycle for the
// age-bound cause: setVaultAgeBoundCapped(true) refuses and raises the
// shared vault-bound-capped alarm at the age-specific entity key;
// setVaultAgeBoundCapped(false) releases and clears it. Mirrors
// TestVaultMaxSizeCap's shape for the instantaneous size cause, but driven
// by direct setter calls since age/count are sweep-verdict-driven, not
// guard-tick-measured.
func TestVaultAgeBoundCapAndAlarm(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()
	g.SetVaultGuard(vaultA, "aged", []string{"volA"}, "", "", 10*gib)

	if g.vaultAgeBoundCapped(vaultA) {
		t.Fatal("a fresh guard entry must start uncapped for the age bound")
	}

	g.setVaultAgeBoundCapped(spy, vaultA, true)
	if !g.vaultAgeBoundCapped(vaultA) {
		t.Fatal("setVaultAgeBoundCapped(true) must engage the cap")
	}
	if !spy.has(alarmVaultBoundCapped + ":" + vaultA.String() + "/age") {
		t.Fatalf("age cap must raise the shared vault-bound-capped alarm at the /age key; raised=%v", spy.set)
	}

	g.setVaultAgeBoundCapped(spy, vaultA, false)
	if g.vaultAgeBoundCapped(vaultA) {
		t.Fatal("setVaultAgeBoundCapped(false) must release the cap")
	}
	if spy.has(alarmVaultBoundCapped + ":" + vaultA.String() + "/age") {
		t.Fatal("release must clear the alarm")
	}
}

// TestVaultChunkCountBoundCapAndAlarm is TestVaultAgeBoundCapAndAlarm's
// max-chunks sibling.
func TestVaultChunkCountBoundCapAndAlarm(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()
	g.SetVaultGuard(vaultA, "many-chunks", []string{"volA"}, "", "", 10*gib)

	g.setVaultChunkCountBoundCapped(spy, vaultA, true)
	if !g.vaultChunkCountBoundCapped(vaultA) {
		t.Fatal("setVaultChunkCountBoundCapped(true) must engage the cap")
	}
	if !spy.has(alarmVaultBoundCapped + ":" + vaultA.String() + "/count") {
		t.Fatalf("count cap must raise the shared alarm at the /count key; raised=%v", spy.set)
	}

	g.setVaultChunkCountBoundCapped(spy, vaultA, false)
	if g.vaultChunkCountBoundCapped(vaultA) {
		t.Fatal("release must clear the cap")
	}
	if spy.has(alarmVaultBoundCapped + ":" + vaultA.String() + "/count") {
		t.Fatal("release must clear the alarm")
	}
}

// TestVaultBoundCapsCoexistOnOneVault pins the entity-key disambiguation
// that lets age and chunk-count stand on the SAME vault at once without
// colliding on one alarm slot — the reason this alarm type carries a
// cause suffix instead of reusing the vault ID alone as the entity key.
func TestVaultBoundCapsCoexistOnOneVault(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultA := glid.New()
	g.SetVaultGuard(vaultA, "double-bound", []string{"volA"}, "", "", 10*gib)

	g.setVaultAgeBoundCapped(spy, vaultA, true)
	g.setVaultChunkCountBoundCapped(spy, vaultA, true)
	if !g.vaultAgeBoundCapped(vaultA) || !g.vaultChunkCountBoundCapped(vaultA) {
		t.Fatal("both bounds must be independently capped")
	}
	if !spy.has(alarmVaultBoundCapped+":"+vaultA.String()+"/age") || !spy.has(alarmVaultBoundCapped+":"+vaultA.String()+"/count") {
		t.Fatalf("both alarms must stand at once; raised=%v", spy.set)
	}

	// Releasing ONE must not clear the other.
	g.setVaultAgeBoundCapped(spy, vaultA, false)
	if g.vaultChunkCountBoundCapped(vaultA) == false {
		t.Fatal("fixture check: count cap must still be engaged")
	}
	if !spy.has(alarmVaultBoundCapped + ":" + vaultA.String() + "/count") {
		t.Fatal("clearing the age cause must not clear the count alarm")
	}
	if spy.has(alarmVaultBoundCapped + ":" + vaultA.String() + "/age") {
		t.Fatal("age alarm must have cleared")
	}
}

// TestSetVaultBoundCappedNoOpsWithoutGuardEntry pins the documented no-op:
// a vault the disk guard has never registered (a memory vault, or a file
// vault not yet reconciled by refreshVaultDiskGuards) has nothing to fold
// a sweep verdict into — the setter must not panic or fabricate an entry.
func TestSetVaultBoundCappedNoOpsWithoutGuardEntry(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	unknown := glid.New()

	g.setVaultAgeBoundCapped(spy, unknown, true)
	g.setVaultChunkCountBoundCapped(spy, unknown, true)
	if g.vaultAgeBoundCapped(unknown) || g.vaultChunkCountBoundCapped(unknown) {
		t.Fatal("a vault with no guard entry must never read as capped")
	}
	if spy.active() != 0 {
		t.Fatalf("no guard entry means no alarm may raise; active=%d", spy.active())
	}
}

// TestVaultAdmissionGateAgeAndChunkCountBound pins gate ordering and the
// remote-peer half for the two new causes, mirroring
// TestVaultAdmissionGateMaxSize.
func TestVaultAdmissionGateAgeAndChunkCountBound(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultAge, vaultCount, vaultRemote := glid.New(), glid.New(), glid.New()
	g.SetVaultGuard(vaultAge, "age", []string{"volA"}, "", "", 10*gib)
	g.SetVaultGuard(vaultCount, "count", []string{"volA"}, "", "", 10*gib)
	g.setVaultAgeBoundCapped(spy, vaultAge, true)
	g.setVaultChunkCountBoundCapped(spy, vaultCount, true)

	o := &Orchestrator{diskGuard: g}
	if err := o.vaultAdmissionGate(vaultAge); !errors.Is(err, ErrVaultAgeBound) {
		t.Fatalf("age-capped vault must be refused with ErrVaultAgeBound, got %v", err)
	}
	if err := o.vaultAdmissionGate(vaultCount); !errors.Is(err, ErrVaultChunkCountBound) {
		t.Fatalf("count-capped vault must be refused with ErrVaultChunkCountBound, got %v", err)
	}

	// Remote cause (another node's broadcast) refuses a vault this node
	// never locally registered with its guard at all.
	if err := o.vaultAdmissionGate(vaultRemote); err != nil {
		t.Fatalf("unregistered vault must admit before any remote hook is installed: %v", err)
	}
	o.SetRemoteVaultAgeBoundCapped(func(id glid.GLID) bool { return id == vaultRemote })
	if err := o.vaultAdmissionGate(vaultRemote); !errors.Is(err, ErrVaultAgeBound) {
		t.Fatalf("remotely age-bound-capped vault must be refused, got %v", err)
	}
	o.SetRemoteVaultAgeBoundCapped(func(glid.GLID) bool { return false })
	o.SetRemoteVaultChunkCountBoundCapped(func(id glid.GLID) bool { return id == vaultRemote })
	if err := o.vaultAdmissionGate(vaultRemote); !errors.Is(err, ErrVaultChunkCountBound) {
		t.Fatalf("remotely count-bound-capped vault must be refused, got %v", err)
	}

	// Broadcast side: only locally capped vaults are published.
	ageCapped := o.AgeBoundCappedVaults()
	if len(ageCapped) != 1 || ageCapped[0] != vaultAge {
		t.Fatalf("AgeBoundCappedVaults = %v, want [%s]", ageCapped, vaultAge)
	}
	countCapped := o.ChunkCountBoundCappedVaults()
	if len(countCapped) != 1 || countCapped[0] != vaultCount {
		t.Fatalf("ChunkCountBoundCappedVaults = %v, want [%s]", countCapped, vaultCount)
	}
}

// TestVaultAdmissionCausesReportsAllBoundCausesTogether pins that
// VaultAdmissionCauses (the VaultInfo.AdmissionRefused collector) reports
// EVERY applicable cause, not just the one vaultAdmissionGate acts on
// first — mirrors the existing size+disk-protect combination coverage in
// retention_maxsize_admission_test.go / vault_info_admission_test.go, now
// spanning all five causes.
func TestVaultAdmissionCausesReportsAllBoundCausesTogether(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultID := glid.New()
	g.SetVaultGuard(vaultID, "everything", []string{"volA"}, "", "", 10*gib)
	g.setVaultAgeBoundCapped(spy, vaultID, true)
	g.setVaultChunkCountBoundCapped(spy, vaultID, true)

	o := &Orchestrator{diskGuard: g}
	causes := o.VaultAdmissionCauses(vaultID)
	want := []VaultAdmissionCause{VaultAdmissionCauseAgeBound, VaultAdmissionCauseChunkCountBound}
	if len(causes) != len(want) {
		t.Fatalf("VaultAdmissionCauses = %v, want %v", causes, want)
	}
	for i, c := range want {
		if causes[i] != c {
			t.Errorf("cause[%d] = %v, want %v (gate-check order)", i, causes[i], c)
		}
	}
}

// TestRetainVaultGuardsClearsBoundAlarms pins the prune-time alarm cleanup
// disk_guard.go's other cap alarms already get: once a vault falls out of
// this node's guarded set (removed from config, placement moved off-node),
// nothing else ever clears a standing age/count-bound alarm for it.
func TestRetainVaultGuardsClearsBoundAlarms(t *testing.T) {
	t.Parallel()
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	spy := &alertSpy{}
	vaultID := glid.New()
	g.SetVaultGuard(vaultID, "pruned", []string{"volA"}, "", "", 10*gib)
	g.setVaultAgeBoundCapped(spy, vaultID, true)
	g.setVaultChunkCountBoundCapped(spy, vaultID, true)
	if spy.active() != 2 {
		t.Fatalf("fixture setup: both alarms must be standing; active=%d", spy.active())
	}

	g.retainVaultGuards(map[glid.GLID]bool{}, spy)

	if spy.active() != 0 {
		t.Fatalf("pruning the vault must clear both bound alarms; active=%d set=%v", spy.active(), spy.set)
	}
	if g.vaultAgeBoundCapped(vaultID) || g.vaultChunkCountBoundCapped(vaultID) {
		t.Fatal("pruned entry must read as uncapped")
	}
}
