package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/routing"
	"gastrolog/internal/system"
)

// dispositionFixture wires a source vault holding a sealed chunk plus a started
// pipeline whose published routing table forwards retention output
// (_source="retention" AND _vault=<source>) to an archive vault. Tests fire
// retention and observe the pipeline routing counters to verify the disposition
// gate (route vs delete) — the routing/segmentation mechanics themselves are
// covered by the routing and pipeline packages' own tests.
type dispositionFixture struct {
	orch      *Orchestrator
	sourceCM  chunk.ChunkManager
	sourceID  glid.GLID
	archiveID glid.GLID
	sealedID  chunk.ChunkID
}

// newDispositionFixture registers a source vault seeded with 3 sealed records
// and starts a pipeline routing retention output to a separate archive vault.
func newDispositionFixture(t *testing.T) dispositionFixture {
	t.Helper()
	sourceID := glid.New()
	archiveID := glid.New()

	orch := newRoutingOrch(t, retentionRouteTable(t, sourceID, archiveID))
	sourceCM := seedSealedSourceVault(t, orch, sourceID, 3)

	metas, err := sourceCM.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}
	if sealedID == (chunk.ChunkID{}) {
		t.Fatal("no sealed chunk found")
	}

	return dispositionFixture{
		orch:      orch,
		sourceCM:  sourceCM,
		sourceID:  sourceID,
		archiveID: archiveID,
		sealedID:  sealedID,
	}
}

// newRoutingOrch builds an orchestrator with a started pipeline and (optionally)
// a published routing table. The pipeline routing workers consume submitted
// records asynchronously, incrementing the counters tests assert against.
func newRoutingOrch(t *testing.T, table *routing.Table) *Orchestrator {
	t.Helper()
	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	if table != nil {
		orch.pipeline.SetRoutingTable(table)
	}
	if err := orch.pipeline.Start(context.Background()); err != nil {
		t.Fatalf("pipeline Start: %v", err)
	}
	t.Cleanup(func() { _ = orch.pipeline.Stop() })
	return orch
}

// retentionRouteTable returns a table routing retention output from sourceID to
// archiveID via the synthetic attributes RetentionSource injects.
func retentionRouteTable(t *testing.T, sourceID, archiveID glid.GLID) *routing.Table {
	t.Helper()
	expr := `_source="retention" AND _vault="` + sourceID.String() + `"`
	r, err := routing.CompileRoute(glid.New(), "retain-source-to-archive", 0, expr, []glid.GLID{archiveID})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	return routing.NewTable([]*routing.Route{r})
}

// seedSealedSourceVault registers a memory-backed source vault, appends n
// records, and seals them into a readable sealed chunk.
func seedSealedSourceVault(t *testing.T, orch *Orchestrator, sourceID glid.GLID, n int) chunk.ChunkManager {
	t.Helper()
	cm, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("source CM: %v", err)
	}
	orch.RegisterVault(NewVaultFromComponents(sourceID, cm, nil, nil))

	for i := range n {
		rec := chunk.Record{
			Attrs: chunk.Attributes{"i": string(rune('0' + i))},
			Raw:   []byte{byte('a' + i)},
		}
		if _, _, err := cm.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := cm.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}
	return cm
}

// waitForRouteStats polls the orchestrator's pipeline routing counters until
// cond is satisfied or the deadline elapses (the routing workers process
// submitted records asynchronously).
func waitForRouteStats(t *testing.T, orch *Orchestrator, what string, cond func(*RouteStats) bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond(orch.GetRouteStats()) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s (stats=%+v)", what, orch.GetRouteStats())
}

// TestFireRetentionEventStreamsThroughPipeline verifies that firing a retention
// event submits the chunk's records into the pipeline with synthetic
// _source="retention" / _vault="<id>" attributes, and that a matching route
// fans them out to its destination vault (matched counter advances).
func TestFireRetentionEventStreamsThroughPipeline(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID: fx.sourceID,
		orch:    fx.orch,
		logger:  slog.Default(),
	}
	r.fireRetentionEvent(fx.sealedID)

	waitForRouteStats(t, fx.orch, "3 matched retention records", func(s *RouteStats) bool {
		return s.Matched == 3
	})
	if got := fx.orch.VaultRouteStatsList()[fx.archiveID]; got == nil || got.Matched != 3 {
		t.Errorf("archive vault should have matched 3 retention records, got %+v", got)
	}

	// Sanity: source vault still has its records — destruction is the
	// caller's job, not fireRetentionEvent's.
	srcCount := 0
	cur, err := fx.sourceCM.OpenCursor(fx.sealedID)
	if err != nil {
		t.Fatalf("source OpenCursor: %v", err)
	}
	for {
		_, _, err := cur.Next()
		if err != nil {
			break
		}
		srcCount++
	}
	cur.Close()
	if srcCount != 3 {
		t.Errorf("source vault should still have 3 records (fire doesn't destroy), got %d", srcCount)
	}
}

// TestFireRetentionEventDropsWhenNoRouteMatches verifies that a retention sweep
// whose records match no route is a counted, silent drop — the same observable
// behavior as the legacy expire action. Operators who haven't set up retention
// routes see chunks expire without fan-out.
func TestFireRetentionEventDropsWhenNoRouteMatches(t *testing.T) {
	t.Parallel()

	sourceID := glid.New()
	archiveID := glid.New()

	// Route only matches live ingest, not retention. Records fired by
	// retention should be counted as unmatched drops.
	ingestOnly, err := routing.CompileRoute(glid.New(), "ingest-only", 0, `_source="ingest"`, []glid.GLID{archiveID})
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	orch := newRoutingOrch(t, routing.NewTable([]*routing.Route{ingestOnly}))
	sourceCM := seedSealedSourceVault(t, orch, sourceID, 2)

	metas, _ := sourceCM.List()
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}

	r := &retentionRunner{
		vaultID: sourceID,
		orch:    orch,
		logger:  slog.Default(),
	}
	r.fireRetentionEvent(sealedID)

	// All submitted records ingested but unmatched; none routed.
	waitForRouteStats(t, orch, "2 unmatched retention records", func(s *RouteStats) bool {
		return s.Unmatched == 2
	})
	if s := orch.GetRouteStats(); s.Matched != 0 {
		t.Errorf("no records should have matched the ingest-only route, got Matched=%d", s.Matched)
	}
	if got := orch.VaultRouteStatsList()[archiveID]; got != nil && got.Matched != 0 {
		t.Errorf("archive vault should have matched 0 records, got %+v", got)
	}
}

// TestRetentionDispositionRouteFiresFanOut verifies that with
// disposition="route", the runner streams records through the pipeline — so a
// configured retention-source route forwards them to its destination.
func TestRetentionDispositionRouteFiresFanOut(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID:     fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: system.RetentionDispositionRoute,
	}
	r.applyRetentionDispositionToChunk(fx.sealedID)

	waitForRouteStats(t, fx.orch, "3 matched records (route disposition fires fan-out)", func(s *RouteStats) bool {
		return s.Matched == 3
	})
}

// TestRetentionDispositionDeleteSkipsFanOut verifies that with
// disposition="delete", the runner does NOT submit records to the pipeline —
// even when a retention-source route is configured that would otherwise forward
// them. This is the safe-default behavior that prevents accidental cascades.
func TestRetentionDispositionDeleteSkipsFanOut(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID:     fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: system.RetentionDispositionDelete,
	}
	r.applyRetentionDispositionToChunk(fx.sealedID)

	assertNoRetentionFanOut(t, fx.orch, "delete disposition")
}

// TestRetentionDispositionEmptyTreatedAsDelete verifies that an empty
// disposition string (the natural state for vaults that haven't had the field
// set) behaves as "delete" — the load-bearing fail-safe so a vault with no
// explicit disposition must NOT cascade.
func TestRetentionDispositionEmptyTreatedAsDelete(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID:     fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: "", // explicitly empty — what the runner sees pre-resolution
	}
	r.applyRetentionDispositionToChunk(fx.sealedID)

	assertNoRetentionFanOut(t, fx.orch, "empty disposition")
}

// assertNoRetentionFanOut gives the pipeline a brief grace window then asserts
// that nothing was submitted to routing (no ingest, no match).
func assertNoRetentionFanOut(t *testing.T, orch *Orchestrator, what string) {
	t.Helper()
	time.Sleep(50 * time.Millisecond)
	if s := orch.GetRouteStats(); s.Routed != 0 {
		t.Errorf("%s must skip pipeline fan-out, but %d records were ingested", what, s.Routed)
	}
}

// TestTryRetainChunkSkipsDispositionWhenAlreadyPending pins the regression fix
// from the gastrolog-2eclw-follow-up live incident: when retention sweeps
// re-evaluate a chunk that's already retention-pending (because the source
// delete is stuck), the disposition action MUST NOT fire again. Otherwise every
// sweep re-streams the same records to the route destination, multiplying
// storage at the target each cycle.
func TestTryRetainChunkSkipsDispositionWhenAlreadyPending(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID:     fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: system.RetentionDispositionRoute,
		inflight:    make(map[chunk.ChunkID]bool),
		// No applyRaftRetentionPending stub — when nil, tryRetainChunk
		// skips the FSM mark and proceeds directly to the disposition
		// + expire path. The alreadyPending gate is independent of the
		// FSM-mark gate, so this is sufficient to exercise it.
		// No reconciler / no im / no cm — expireChunk will hit nil
		// derefs in the reconciler-less fallback, which is fine: the
		// gate runs BEFORE expireChunk, so the fan-out completes before
		// any nil-deref. We recover the panic to prove the gate ran.
	}

	// First call: alreadyPending=false → must fire routing (3 records
	// into the pipeline). expireChunk panics from nil cm — caught.
	func() {
		defer func() { _ = recover() }()
		r.tryRetainChunk(fx.sealedID, retentionRule{}, false)
	}()

	waitForRouteStats(t, fx.orch, "first sweep routes 3 records", func(s *RouteStats) bool {
		return s.Matched == 3
	})

	// Re-arm inflight for the second call.
	r.mu.Lock()
	r.inflight = make(map[chunk.ChunkID]bool)
	r.mu.Unlock()

	// Second call: alreadyPending=true → must NOT fire routing.
	func() {
		defer func() { _ = recover() }()
		r.tryRetainChunk(fx.sealedID, retentionRule{}, true)
	}()

	// Give the pipeline a grace window; the matched count must stay at 3.
	time.Sleep(50 * time.Millisecond)
	if s := fx.orch.GetRouteStats(); s.Matched != 3 {
		t.Errorf("second sweep (alreadyPending=true) MUST NOT re-route; matched grew from 3 to %d (the storage-eating cascade bug)", s.Matched)
	}
}

// TestRetentionTargetThreadsDispositionFromVaultConfig verifies that
// retentionTargetForInstance reads VaultConfig.RetentionDisposition (via
// ResolveRetentionDisposition) and writes the resolved value to the runner.
// This is the load-bearing plumbing — without it, the per-chunk gate in
// tryRetainChunk always sees an empty string and defaults to "delete"
// regardless of operator intent. Refreshes on every sweep tick so live config
// edits propagate without restart.
func TestRetentionTargetThreadsDispositionFromVaultConfig(t *testing.T) {
	t.Parallel()

	// Vault and instance share the same ID.
	vaultID := glid.New()

	policyID := glid.New()

	cases := []struct {
		name string
		set  string
		want string
	}{
		{"unset becomes delete", "", system.RetentionDispositionDelete},
		{"explicit delete", system.RetentionDispositionDelete, system.RetentionDispositionDelete},
		{"explicit route", system.RetentionDispositionRoute, system.RetentionDispositionRoute},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg := &system.Config{
				Vaults: []system.VaultConfig{{
					ID:                   vaultID,
					Enabled:              true,
					RetentionDisposition: c.set,
					RetentionRules: []system.RetentionRule{{
						RetentionPolicyID: policyID,
					}},
				}},
				RetentionPolicies: []system.RetentionPolicyConfig{{
					ID:          policyID,
					MaxAgeNanos: int64Ptr(int64(time.Hour)),
				}},
			}

			orch, err := New(Config{
				SystemLoader: testSystemLoader{cfg: cfg},
				Logger:       slog.Default(),
			})
			if err != nil {
				t.Fatal(err)
			}
			defer orch.Stop()

			vaultInst := &VaultInstance{
				VaultID: vaultID,
				Chunks:  &retentionFakeChunkManager{},
				Indexes: &retentionFakeIndexManager{},
			}
			active := make(map[string]bool)
			target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], vaultInst, active)
			if target == nil {
				t.Fatal("expected non-nil sweep target")
			}
			if got := target.runner.disposition; got != c.want {
				t.Errorf("runner.disposition = %q, want %q", got, c.want)
			}
		})
	}
}

// TestResolveRetentionDispositionDefaults locks the resolver behavior for empty
// + unrecognized values. The retention runner uses the resolved value, so
// anything not "route" must coerce to "delete" — no silent passthrough that
// could let a typo become a cascade.
func TestResolveRetentionDispositionDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", system.RetentionDispositionDelete},
		{"explicit delete", "delete", system.RetentionDispositionDelete},
		{"explicit route", "route", system.RetentionDispositionRoute},
		{"unknown coerces to delete", "archive", system.RetentionDispositionDelete},
		{"capitalization is significant — typo coerces to delete", "Route", system.RetentionDispositionDelete},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := system.VaultConfig{RetentionDisposition: c.in}
			if got := v.ResolveRetentionDisposition(); got != c.want {
				t.Errorf("ResolveRetentionDisposition(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}
