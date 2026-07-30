package orchestrator

// Refusal generalizes from max-size to every retention policy bound (age,
// chunk-count), gated by a per-policy refuse flag (default on). The
// VIOLATION PREDICATE is the subtle part: normal
// operation transiently violates both dimensions between a chunk's seal
// and the next sweep — refusing on that transient is pure flapping. A
// violation only counts once retentionRunner.checkBoundViolations
// (retention.go) observes it STILL standing after a full sweep attempted
// to clear it. These tests drive retentionRunner.sweep() directly against
// a real (delete-tracking) fake chunk manager so the post-sweep re-list
// reflects what the sweep actually left behind — no mocked verdicts.

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// boundFakeChunkManager embeds retentionFakeChunkManager (retention_test.go)
// for its long stub method set, but overrides List/Delete with real
// mutation: checkBoundViolations' post-sweep re-list must see what
// expireChunk actually removed. retentionFakeChunkManager's own
// Delete only appends to a side list and leaves List() static — correct
// for ITS callers (which assert against .deleted directly), wrong for
// this file's "did the post-sweep list actually change" assertions.
type boundFakeChunkManager struct {
	retentionFakeChunkManager
	listMu sync.Mutex
	byID   map[chunk.ChunkID]chunk.ChunkMeta
}

func newBoundFakeChunkManager(metas ...chunk.ChunkMeta) *boundFakeChunkManager {
	m := &boundFakeChunkManager{byID: make(map[chunk.ChunkID]chunk.ChunkMeta)}
	for _, cm := range metas {
		m.byID[cm.ID] = cm
	}
	return m
}

func (f *boundFakeChunkManager) List() ([]chunk.ChunkMeta, error) {
	f.listMu.Lock()
	defer f.listMu.Unlock()
	out := make([]chunk.ChunkMeta, 0, len(f.byID))
	for _, m := range f.byID {
		out = append(out, m)
	}
	return out, nil
}

func (f *boundFakeChunkManager) Delete(id chunk.ChunkID) error {
	f.listMu.Lock()
	defer f.listMu.Unlock()
	delete(f.byID, id)
	return nil
}

// sealedChunkMeta builds a minimal sealed chunk meta with the given
// seal time — the only field TTLRetentionPolicy/CountRetentionPolicy read
// beyond identity and Sealed.
func sealedChunkMeta(sealedAt time.Time) chunk.ChunkMeta {
	return chunk.ChunkMeta{
		ID:       chunk.NewChunkID(),
		Sealed:   true,
		SealedAt: sealedAt,
	}
}

// newBoundRunnerFixture builds a retentionRunner wired to a real diskGuard
// vault entry (so checkBoundViolations' setters have somewhere to fold
// into, exactly like a file vault refreshVaultDiskGuards would have
// registered) and a chunk manager whose List() reflects actual deletes.
func newBoundRunnerFixture(t *testing.T, vaultID glid.GLID, metas ...chunk.ChunkMeta) (*retentionRunner, *diskGuard, *alertSpy) {
	t.Helper()
	return newBoundRunnerFixtureWithManager(t, vaultID, newBoundFakeChunkManager(metas...))
}

// newBoundRunnerFixtureWithManager is newBoundRunnerFixture with an
// already-constructed chunk manager, so a restart simulation can build TWO
// independent runner/guard/orchestrator triples against the SAME
// underlying chunk data (standing in for what's still on disk across a
// process restart) — see
// TestCheckBoundViolationsRestartDoesNotCarryOverState.
func newBoundRunnerFixtureWithManager(t *testing.T, vaultID glid.GLID, cm chunk.ChunkManager) (*retentionRunner, *diskGuard, *alertSpy) {
	t.Helper()
	spy := &alertSpy{}
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", Alerts: spy})
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	g.SetVaultGuard(vaultID, "bound-test", []string{"volA"}, 10*gib, "", "")
	orch.diskGuard = g

	im := &retentionFakeIndexManager{}
	vaultInst := &VaultInstance{VaultID: vaultID, Chunks: cm, Indexes: im}
	rec := NewVaultLifecycleReconciler(orch, vaultID, vaultInst, "node-A", slog.Default())
	vaultInst.Reconciler = rec
	r := &retentionRunner{
		vaultID:    vaultID,
		vaultName:  "bound-test",
		orch:       orch,
		cm:         cm,
		im:         im,
		reconciler: rec,
		logger:     slog.Default(),
		now:        time.Now,
		isLeader:   true,
	}
	return r, g, spy
}

// TestCheckBoundViolationsAgeSweepClearsBound pins the "cleared sweep
// releases" pin: a chunk past its max-age bound, on a delete-disposition
// vault (the default), is destroyed within the SAME sweep that matched
// it — the post-sweep re-check finds nothing left violating the bound, so
// admission is never refused.
func TestCheckBoundViolationsAgeSweepClearsBound(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	old := sealedChunkMeta(now.Add(-2 * time.Hour))
	r, g, _ := newBoundRunnerFixture(t, vaultID, old)
	r.now = func() time.Time { return now }

	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	rules := []retentionRule{{policy: agePolicy, refuse: true, agePolicy: agePolicy}}

	if g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("fixture: must start uncapped before any sweep ever ran")
	}

	r.sweep(rules)

	if g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("a sweep that successfully deletes the offending chunk must clear the age bound, not refuse")
	}
}

// TestCheckBoundViolationsAgeSweepFailsToClearRefuses pins the other half
// of the predicate: a chunk past its max-age bound whose configured
// disposition (route) could not run this sweep — findVaultInstance
// returns nil, the same "vault instance unavailable" deferral
// fireRetentionEvent already covers — survives the sweep untouched. The
// post-sweep re-check finds it STILL violating and refuses. Also pins the
// PRE-sweep half: before sweep() is ever called, the guard must NOT read
// capped even though the chunk objectively already violates the bound —
// there is no instantaneous check for age/count, only the post-sweep one.
func TestCheckBoundViolationsAgeSweepFailsToClearRefuses(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	old := sealedChunkMeta(now.Add(-2 * time.Hour))
	r, g, _ := newBoundRunnerFixture(t, vaultID, old)
	r.now = func() time.Time { return now }
	r.disposition = system.RetentionDispositionRoute
	// r.orch.vaults never gets this vaultID registered, so
	// findVaultInstance() returns nil inside fireRetentionEvent — the
	// same "vault instance unavailable" deferral path
	// TestFireRetentionEventAbortsOnCappedDestination's sibling tests
	// exercise, here reached via age-bound matching instead of a size cap.

	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	rules := []retentionRule{{policy: agePolicy, refuse: true, agePolicy: agePolicy}}

	// Pre-sweep pin: an objectively over-age chunk sitting in the chunk
	// manager, with NO sweep having run yet, must not read as capped.
	if g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("no sweep has run yet — the pre-sweep transient must never refuse")
	}

	r.sweep(rules)

	if !g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("a sweep that matched the chunk but failed to clear it (deferred fan-out) must refuse")
	}

	o := r.orch
	if err := o.vaultAdmissionGate(vaultID); err == nil {
		t.Fatal("admission must be refused once the age bound is swept-and-still-violated")
	}
}

// TestCheckBoundViolationsRefuseFalseNeverRefuses pins refuse=false as a
// pure drain-only "soft bound": even when the sweep fails to clear the
// violation (same deferred-fan-out setup as the test above), a policy
// with refuse=false must never engage admission refusal.
func TestCheckBoundViolationsRefuseFalseNeverRefuses(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	old := sealedChunkMeta(now.Add(-2 * time.Hour))
	r, g, _ := newBoundRunnerFixture(t, vaultID, old)
	r.now = func() time.Time { return now }
	r.disposition = system.RetentionDispositionRoute

	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	rules := []retentionRule{{policy: agePolicy, refuse: false, agePolicy: agePolicy}}

	r.sweep(rules)

	if g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("a refuse=false (soft-bound) policy must never engage refusal, even swept-and-still-violated")
	}
}

// TestCheckBoundViolationsChunkCountBound is the age tests' max-chunks
// sibling: clears on a successful drain, refuses when the sweep fails to
// clear it.
func TestCheckBoundViolationsChunkCountBound(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	// Three sealed chunks, budget of one: two must drain.
	metas := []chunk.ChunkMeta{
		sealedChunkMeta(now.Add(-3 * time.Hour)),
		sealedChunkMeta(now.Add(-2 * time.Hour)),
		sealedChunkMeta(now.Add(-1 * time.Hour)),
	}

	t.Run("clears when the sweep drains down to budget", func(t *testing.T) {
		t.Parallel()
		r, g, _ := newBoundRunnerFixture(t, vaultID, metas...)
		r.now = func() time.Time { return now }
		countPolicy := chunk.NewCountRetentionPolicy(1)
		rules := []retentionRule{{policy: countPolicy, refuse: true, countPolicy: countPolicy}}

		r.sweep(rules)

		if g.vaultChunkCountBoundCapped(vaultID) {
			t.Fatal("a sweep that drains down to budget must clear the chunk-count bound")
		}
	})

	t.Run("refuses when disposition defers and the count stays over budget", func(t *testing.T) {
		t.Parallel()
		vaultID := glid.New()
		r, g, _ := newBoundRunnerFixture(t, vaultID, metas...)
		r.now = func() time.Time { return now }
		r.disposition = system.RetentionDispositionRoute // findVaultInstance() nil -> deferred
		countPolicy := chunk.NewCountRetentionPolicy(1)
		rules := []retentionRule{{policy: countPolicy, refuse: true, countPolicy: countPolicy}}

		r.sweep(rules)

		if !g.vaultChunkCountBoundCapped(vaultID) {
			t.Fatal("a sweep that failed to drain below budget must refuse on the chunk-count bound")
		}
	})
}

// TestCheckBoundViolationsHardSoftPolicyMix pins the min-per-kind
// resolution rule: a vault with two attached policies on the SAME
// dimension (age), one soft (refuse=false) and one hard (refuse=true),
// refuses ONLY when the HARD policy's own bound is violated — a violation
// of the soft policy's (possibly tighter) bound alone must never refuse.
func TestCheckBoundViolationsHardSoftPolicyMix(t *testing.T) {
	t.Parallel()

	t.Run("only the soft (tighter) policy matches: no refusal", func(t *testing.T) {
		t.Parallel()
		vaultID := glid.New()
		now := time.Now()
		// 2h old: past the soft 30m bound, NOT past the hard 3h bound.
		chunkMeta := sealedChunkMeta(now.Add(-2 * time.Hour))
		r, g, _ := newBoundRunnerFixture(t, vaultID, chunkMeta)
		r.now = func() time.Time { return now }
		r.disposition = system.RetentionDispositionRoute // deferred either way

		softAge := chunk.NewTTLRetentionPolicy(30 * time.Minute)
		hardAge := chunk.NewTTLRetentionPolicy(3 * time.Hour)
		rules := []retentionRule{
			{policy: softAge, refuse: false, agePolicy: softAge},
			{policy: hardAge, refuse: true, agePolicy: hardAge},
		}

		r.sweep(rules)

		if g.vaultAgeBoundCapped(vaultID) {
			t.Fatal("only the soft policy's bound is violated — a vault mixing hard and soft policies must refuse only on the hard one's bounds")
		}
	})

	t.Run("both policies match (chunk also past the hard bound): refuses", func(t *testing.T) {
		t.Parallel()
		vaultID := glid.New()
		now := time.Now()
		// 4h old: past BOTH the soft 30m bound and the hard 3h bound.
		chunkMeta := sealedChunkMeta(now.Add(-4 * time.Hour))
		r, g, _ := newBoundRunnerFixture(t, vaultID, chunkMeta)
		r.now = func() time.Time { return now }
		r.disposition = system.RetentionDispositionRoute // deferred: findVaultInstance() nil

		softAge := chunk.NewTTLRetentionPolicy(30 * time.Minute)
		hardAge := chunk.NewTTLRetentionPolicy(3 * time.Hour)
		rules := []retentionRule{
			{policy: softAge, refuse: false, agePolicy: softAge},
			{policy: hardAge, refuse: true, agePolicy: hardAge},
		}

		r.sweep(rules)

		if !g.vaultAgeBoundCapped(vaultID) {
			t.Fatal("the hard policy's own bound is also violated and swept-and-failed — admission must refuse")
		}
	})
}

// TestCheckBoundViolationsDeferralAlarmCoexistsWithRefusal pins the
// interaction with the deferral machinery: a route-disposition vault
// whose fan-out keeps deferring raises BOTH the
// retention-deferred alarm (at its 3-consecutive-sweep threshold) AND the
// age-bound-capped alarm (from sweep 1, since the post-sweep predicate has
// no streak requirement) — coherently, one not clobbering the other.
func TestCheckBoundViolationsDeferralAlarmCoexistsWithRefusal(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	old := sealedChunkMeta(now.Add(-2 * time.Hour))
	r, g, spy := newBoundRunnerFixture(t, vaultID, old)
	r.now = func() time.Time { return now }
	r.disposition = system.RetentionDispositionRoute

	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	rules := []retentionRule{{policy: agePolicy, refuse: true, agePolicy: agePolicy}}

	// Sweep 1: age-bound-capped refuses immediately (no streak needed);
	// the deferral streak is only 1, below its 3-sweep alarm threshold.
	r.sweep(rules)
	if !g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("age bound must refuse from the first failed sweep")
	}
	if spy.has(alarmRetentionDeferred + ":" + vaultID.String()) {
		t.Fatal("retention-deferred must not raise before its 3-sweep threshold")
	}
	if !spy.has(alarmVaultBoundCapped + ":" + vaultID.String() + "/age") {
		t.Fatal("age-bound-capped alarm must be standing after sweep 1")
	}

	// Sweeps 2 and 3: same deferral. At sweep 3 the deferral streak
	// alarm joins the still-standing age-bound alarm.
	r.sweep(rules)
	r.sweep(rules)

	if !spy.has(alarmRetentionDeferred + ":" + vaultID.String()) {
		t.Fatal("retention-deferred must raise at the 3-consecutive-sweep threshold")
	}
	if !spy.has(alarmVaultBoundCapped + ":" + vaultID.String() + "/age") {
		t.Fatal("age-bound-capped alarm must still be standing — one alarm's raise must not clear the other")
	}

	if err := r.orch.vaultAdmissionGate(vaultID); err == nil {
		t.Fatal("admission must still be refused while both alarms stand")
	}
}

// TestCheckBoundViolationsRestartDoesNotCarryOverState pins the restart
// dimension: the age/count guard flags are pure in-memory state,
// re-derived by the next sweep, never persisted — the same contract
// deferralStreak documents ("a restart starts a fresh streak"). Simulates
// a restart by building a brand-new orchestrator/guard/runner (standing
// in for the process restarting) against the SAME underlying chunk data
// (the fake chunk manager, standing in for what's still on disk) a first
// runner already swept-and-failed against. The second runner must start
// uncapped — nothing carried over — and only re-derive capped=true once
// IT runs its own sweep against the still-violating data.
func TestCheckBoundViolationsRestartDoesNotCarryOverState(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	old := sealedChunkMeta(now.Add(-2 * time.Hour))

	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	rules := []retentionRule{{policy: agePolicy, refuse: true, agePolicy: agePolicy}}

	// "Before restart": a runner sweeps the (still-on-disk) chunk data and
	// fails to clear it — age bound refuses.
	cm := newBoundFakeChunkManager(old)
	r1, g1, _ := newBoundRunnerFixtureWithManager(t, vaultID, cm)
	r1.now = func() time.Time { return now }
	r1.disposition = system.RetentionDispositionRoute // findVaultInstance() nil -> deferred
	r1.sweep(rules)
	if !g1.vaultAgeBoundCapped(vaultID) {
		t.Fatal("fixture setup: pre-restart runner must have failed to clear the bound")
	}

	// "After restart": a brand-new orchestrator/guard/runner, same
	// underlying chunk data (the chunk was never actually deleted — the
	// fan-out kept deferring). Nothing from g1 exists here.
	r2, g2, _ := newBoundRunnerFixtureWithManager(t, vaultID, cm)
	r2.now = func() time.Time { return now }
	r2.disposition = system.RetentionDispositionRoute
	if g2.vaultAgeBoundCapped(vaultID) {
		t.Fatal("a freshly-restarted guard must start uncapped — nothing persists across restart")
	}

	// Only once the restarted runner's OWN sweep re-observes the
	// still-violating data does it re-derive the same verdict.
	r2.sweep(rules)
	if !g2.vaultAgeBoundCapped(vaultID) {
		t.Fatal("the restarted runner must re-derive capped=true from the still-violating data, not stay uncapped forever")
	}
}

// TestRetentionSweepAllReleasesBoundCapsOnLeadershipLoss pins review fix C1
// (strand on leadership move): retentionSweepAll's prune loop must release
// BOTH age and chunk-count bound flags — and the shared vault-bound-capped
// alarm for both causes — when a runner is garbage-collected (vault
// removed from config, placement moved, or THIS node lost leadership for
// the instance). Before the fix, a deposed leader's guard entry kept
// reporting age_bound_vault_ids/chunk_count_bound_vault_ids in its
// NodeStats broadcast forever, so the cluster-wide peer union refused the
// vault permanently — long after the condition (and the runner that
// observed it) was gone. Mirrors
// TestRetentionSweepAllClearsAlarmOnRunnerGC for the deferral alarm.
func TestRetentionSweepAllReleasesBoundCapsOnLeadershipLoss(t *testing.T) {
	t.Parallel()

	spy := &alertSpy{}
	o := newTestOrch(t, Config{LocalNodeID: "node-A", Alerts: spy})
	g, _ := newGuardFixture(400*gib, map[string]uint64{"volA": 200 * gib})
	vaultID := glid.New()
	g.SetVaultGuard(vaultID, "deposed", []string{"volA"}, 10*gib, "", "")
	o.diskGuard = g
	g.setVaultAgeBoundCapped(spy, vaultID, true)
	g.setVaultChunkCountBoundCapped(spy, vaultID, true)
	if !g.vaultAgeBoundCapped(vaultID) || !g.vaultChunkCountBoundCapped(vaultID) {
		t.Fatal("fixture setup: both bounds must start capped")
	}

	// Seed an unrelated vault so retentionSweepAll's config load doesn't
	// bail out before reaching the GC loop at all (mirrors
	// TestRetentionSweepAllClearsAlarmOnRunnerGC's fixture).
	store := sysmem.NewStore()
	_ = store.PutVault(t.Context(), system.VaultConfig{ID: glid.New(), Name: "other", Type: system.VaultTypeMemory})
	o.setSystemLoader(&transitionSystemLoader{store: store})

	o.mu.Lock()
	if o.retention == nil {
		o.retention = make(map[string]*retentionRunner)
	}
	o.retention[vaultID.String()] = &retentionRunner{vaultID: vaultID, vaultName: "deposed"}
	o.mu.Unlock()

	o.retentionSweepAll()

	if g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("leadership loss (runner pruned) must release the age-bound cap")
	}
	if g.vaultChunkCountBoundCapped(vaultID) {
		t.Fatal("leadership loss (runner pruned) must release the chunk-count-bound cap")
	}
	if spy.has(alarmVaultBoundCapped + ":" + vaultID.String() + "/age") {
		t.Fatal("leadership loss must clear the age-cause vault-bound-capped alarm")
	}
	if spy.has(alarmVaultBoundCapped + ":" + vaultID.String() + "/count") {
		t.Fatal("leadership loss must clear the count-cause vault-bound-capped alarm")
	}
	o.mu.RLock()
	_, stillPresent := o.retention[vaultID.String()]
	o.mu.RUnlock()
	if stillPresent {
		t.Fatal("runner must have been pruned")
	}
}

// TestCheckBoundViolationsGhostChunkNeverCountsTowardViolation pins review
// fix I1 (ghost drift): a sealed chunk on disk with NO FSM manifest entry
// (a ghost — its FSM entry was finalize-deleted but the disk file was
// never reaped) is the orphan reaper's problem, not retention's.
// selectRetentionCandidates already excludes ghosts from what a sweep
// acts on; checkBoundViolations must apply the SAME filter to what it
// counts toward a violation — a sweep can never clear a ghost (retention
// doesn't touch it at all), so counting it would let a vault refuse
// admission forever over a chunk retention will never act on.
func TestCheckBoundViolationsGhostChunkNeverCountsTowardViolation(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	ghost := sealedChunkMeta(now.Add(-2 * time.Hour)) // objectively past the 1h bound

	r, g, _ := newBoundRunnerFixture(t, vaultID, ghost)
	r.now = func() time.Time { return now }

	// Wire a vault instance whose FSM manifest is known (non-nil) but does
	// NOT include the ghost's ID — buildManifestSet's manifestKnown=true,
	// manifest[ghost.ID]=false, exactly the ghost condition.
	vault := NewVaultFromComponents(vaultID, r.cm, &retentionFakeIndexManager{}, nil)
	vault.Instance.ListManifest = func() []chunk.ChunkID { return []chunk.ChunkID{} }
	r.orch.RegisterVault(vault)

	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	rules := []retentionRule{{policy: agePolicy, refuse: true, agePolicy: agePolicy}}

	r.sweep(rules)

	if g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("a ghost chunk (on disk, no FSM manifest entry) must never count toward the age-bound violation")
	}
}

// TestCheckBoundViolationsEngageThenReleaseRunnerLevel is the minor
// engage-then-release runner-level test: a sweep that fails to clear the
// age bound caps it and raises the alarm; a LATER sweep that successfully
// clears it (disposition flips to delete, or the destination recovers)
// must uncap it and clear the alarm — the full round trip through
// sweep(), not just the two halves independently.
func TestCheckBoundViolationsEngageThenReleaseRunnerLevel(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	now := time.Now()
	old := sealedChunkMeta(now.Add(-2 * time.Hour))
	cm := newBoundFakeChunkManager(old)
	r, g, spy := newBoundRunnerFixtureWithManager(t, vaultID, cm)
	r.now = func() time.Time { return now }
	r.disposition = system.RetentionDispositionRoute // deferred: findVaultInstance() nil

	agePolicy := chunk.NewTTLRetentionPolicy(time.Hour)
	rules := []retentionRule{{policy: agePolicy, refuse: true, agePolicy: agePolicy}}

	r.sweep(rules)
	if !g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("fixture setup: first sweep must fail to clear and engage the cap")
	}
	if !spy.has(alarmVaultBoundCapped + ":" + vaultID.String() + "/age") {
		t.Fatal("fixture setup: the alarm must be standing after the first sweep")
	}

	// Recovery: disposition flips to delete (the operator's documented
	// recovery action) — this sweep now succeeds and destroys the chunk.
	r.disposition = ""
	r.sweep(rules)

	if g.vaultAgeBoundCapped(vaultID) {
		t.Fatal("a later successful sweep must release the age-bound cap")
	}
	if spy.has(alarmVaultBoundCapped + ":" + vaultID.String() + "/age") {
		t.Fatal("a later successful sweep must clear the vault-bound-capped alarm")
	}
}
