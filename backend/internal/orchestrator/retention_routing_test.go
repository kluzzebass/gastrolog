package orchestrator

import (
	"log/slog"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// dispositionFixture wires a source vault, an archive vault, and a
// retention-source route that would forward records source → archive
// when the routing engine fires. Used by both disposition tests below.
//
// The two tests assert opposite behaviors at the same fan-out point —
// the route exists in both fixtures, but only the "route" disposition
// path is supposed to actually invoke it.
type dispositionFixture struct {
	orch      *Orchestrator
	sourceCM  chunk.ChunkManager
	archiveCM chunk.ChunkManager
	sourceID  glid.GLID
	sealedID  chunk.ChunkID
}

func newDispositionFixture(t *testing.T) dispositionFixture {
	t.Helper()
	sourceID := glid.New()
	archiveID := glid.New()

	sourceCM, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("source CM: %v", err)
	}
	archiveCM, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("archive CM: %v", err)
	}

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(NewVaultFromComponents(sourceID, sourceCM, nil, nil))
	orch.RegisterVault(NewVaultFromComponents(archiveID, archiveCM, nil, nil))

	expr := `_source="retention" AND _vault="` + sourceID.String() + `"`
	cr, err := CompileRoute(glid.New(), "retain-source-to-archive", 0, expr,
		[]RouteDestination{{VaultID: archiveID}}, "fanout")
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	for i := range 3 {
		rec := chunk.Record{
			Attrs: chunk.Attributes{"i": string(rune('0' + i))},
			Raw:   []byte{byte('a' + i)},
		}
		if _, _, err := sourceCM.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := sourceCM.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

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
		archiveCM: archiveCM,
		sourceID:  sourceID,
		sealedID:  sealedID,
	}
}

// countArchiveRecords walks every chunk in the archive and returns the
// total record count. Used to assert whether the routing engine
// forwarded retention output through the source→archive route.
func countArchiveRecords(t *testing.T, cm chunk.ChunkManager) int {
	t.Helper()
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("archive List: %v", err)
	}
	count := 0
	for _, m := range metas {
		cur, err := cm.OpenCursor(m.ID)
		if err != nil {
			t.Fatalf("archive OpenCursor: %v", err)
		}
		for {
			_, _, err := cur.Next()
			if err != nil {
				break
			}
			count++
		}
		cur.Close()
	}
	return count
}

// TestFireRetentionEventStreamsThroughRoutingEngine verifies that
// retention firing in Phase 5 (gastrolog-4kkoo) feeds expired records
// back into the routing engine with synthetic _source="retention" and
// _vault="<id>" attributes, and that a route matching those synthetics
// fans the records out to a different vault.
func TestFireRetentionEventStreamsThroughRoutingEngine(t *testing.T) {
	t.Parallel()

	sourceID := glid.New()
	archiveID := glid.New()

	sourceCM, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("source CM: %v", err)
	}
	archiveCM, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("archive CM: %v", err)
	}

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(NewVaultFromComponents(sourceID, sourceCM, nil, nil))
	orch.RegisterVault(NewVaultFromComponents(archiveID, archiveCM, nil, nil))

	// Phase 5 route: retention from source → archive. Express the
	// vault narrowing via _vault so a separate retention sweep on a
	// different vault wouldn't accidentally fire this route.
	expr := `_source="retention" AND _vault="` + sourceID.String() + `"`
	cr, err := CompileRoute(glid.New(), "retain-source-to-archive", 0, expr,
		[]RouteDestination{{VaultID: archiveID}}, "fanout")
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	// Append three records into the source vault and seal so they are
	// readable via OpenCursor.
	for i := range 3 {
		rec := chunk.Record{
			Attrs: chunk.Attributes{"i": string(rune('0' + i))},
			Raw:   []byte{byte('a' + i)},
		}
		if _, _, err := sourceCM.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := sourceCM.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Pull the sealed chunk's ID.
	metas, err := sourceCM.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) == 0 {
		t.Fatal("no chunks after seal")
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

	// Construct a retentionRunner pointed at the source vault. We don't
	// need the full sweep machinery for this test — just the fan-out
	// path that fireRetentionEvent exercises.
	r := &retentionRunner{
		vaultID: sourceID,
		instID:  sourceID, // 1:1 vault:tier
		orch:    orch,
		logger:  slog.Default(),
	}

	// Fire retention. Records should stream through the routing engine
	// and land in the archive vault.
	r.fireRetentionEvent(sealedID)

	// Verify archive received them. Walk all archive chunks and count
	// records — they may be split across active + sealed depending on
	// rotation, but the per-record count should equal 3.
	archiveMetas, err := archiveCM.List()
	if err != nil {
		t.Fatalf("archive List: %v", err)
	}
	count := 0
	for _, m := range archiveMetas {
		cur, err := archiveCM.OpenCursor(m.ID)
		if err != nil {
			t.Fatalf("archive OpenCursor: %v", err)
		}
		for {
			_, _, err := cur.Next()
			if err != nil {
				break
			}
			count++
		}
		cur.Close()
	}
	if count != 3 {
		t.Errorf("archive vault should have 3 records (fanned from retention), got %d", count)
	}

	// Sanity: source vault still has its records — destruction is the
	// caller's job, not fireRetentionEvent's.
	srcCount := 0
	cur, err := sourceCM.OpenCursor(sealedID)
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

// TestFireRetentionEventDropsWhenNoRouteMatches verifies that a
// retention sweep with no matching route silently drops records — the
// same observable behavior as the legacy expire action. This is the
// load-bearing default: operators who haven't set up retention routes
// see chunks expire as before.
func TestFireRetentionEventDropsWhenNoRouteMatches(t *testing.T) {
	t.Parallel()

	sourceID := glid.New()
	archiveID := glid.New()

	sourceCM, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("source CM: %v", err)
	}
	archiveCM, err := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatalf("archive CM: %v", err)
	}

	orch, err := New(Config{LocalNodeID: "node-A"})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(NewVaultFromComponents(sourceID, sourceCM, nil, nil))
	orch.RegisterVault(NewVaultFromComponents(archiveID, archiveCM, nil, nil))

	// Route only matches live ingest, not retention. Records fired by
	// retention should drop silently.
	cr, err := CompileRoute(glid.New(), "ingest-only", 0, `_source="ingest"`,
		[]RouteDestination{{VaultID: archiveID}}, "fanout")
	if err != nil {
		t.Fatalf("CompileRoute: %v", err)
	}
	orch.SetRouteSet(NewRouteSet([]*CompiledRoute{cr}))

	for i := range 2 {
		rec := chunk.Record{
			Attrs: chunk.Attributes{"i": string(rune('0' + i))},
			Raw:   []byte{byte('a' + i)},
		}
		if _, _, err := sourceCM.Append(rec); err != nil {
			t.Fatalf("Append %d: %v", i, err)
		}
	}
	if err := sourceCM.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

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
		instID:  sourceID,
		orch:    orch,
		logger:  slog.Default(),
	}
	r.fireRetentionEvent(sealedID)

	// Archive must be empty — the route only matched _source="ingest".
	archiveMetas, _ := archiveCM.List()
	count := 0
	for _, m := range archiveMetas {
		cur, err := archiveCM.OpenCursor(m.ID)
		if err != nil {
			continue
		}
		for {
			_, _, err := cur.Next()
			if err != nil {
				break
			}
			count++
		}
		cur.Close()
	}
	if count != 0 {
		t.Errorf("archive should be empty (no retention route matched), got %d records", count)
	}
}

// TestRetentionDispositionRouteFiresFanOut verifies that with
// disposition="route", the runner streams records through the routing
// engine — so a configured retention-source route forwards them to its
// destination. Mirrors the "Phase 5 default" behavior preserved by the
// opt-in route disposition.
func TestRetentionDispositionRouteFiresFanOut(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID:     fx.sourceID,
		instID:      fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: system.RetentionDispositionRoute,
	}
	r.applyRetentionDispositionToChunk(fx.sealedID)

	if got := countArchiveRecords(t, fx.archiveCM); got != 3 {
		t.Errorf("archive should have 3 records (route disposition fires fan-out), got %d", got)
	}
}

// TestRetentionDispositionDeleteSkipsFanOut verifies that with
// disposition="delete", the runner does NOT stream records through the
// routing engine — even when a retention-source route is configured
// that would otherwise forward them. This is the safe-default behavior
// that prevents accidental cascades.
func TestRetentionDispositionDeleteSkipsFanOut(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID:     fx.sourceID,
		instID:      fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: system.RetentionDispositionDelete,
	}
	r.applyRetentionDispositionToChunk(fx.sealedID)

	if got := countArchiveRecords(t, fx.archiveCM); got != 0 {
		t.Errorf("archive should be empty (delete disposition skips fan-out), got %d records", got)
	}
}

// TestRetentionDispositionEmptyTreatedAsDelete verifies that an empty
// disposition string (the natural state for vaults that haven't had
// the field set) behaves as "delete". This is the load-bearing
// fail-safe — a vault with no explicit disposition must NOT cascade.
func TestRetentionDispositionEmptyTreatedAsDelete(t *testing.T) {
	t.Parallel()

	fx := newDispositionFixture(t)

	r := &retentionRunner{
		vaultID:     fx.sourceID,
		instID:      fx.sourceID,
		orch:        fx.orch,
		logger:      slog.Default(),
		disposition: "", // explicitly empty — what the runner sees pre-resolution
	}
	r.applyRetentionDispositionToChunk(fx.sealedID)

	if got := countArchiveRecords(t, fx.archiveCM); got != 0 {
		t.Errorf("archive should be empty (empty disposition defaults to delete), got %d records", got)
	}
}

// TestRetentionTargetThreadsDispositionFromVaultConfig verifies that
// retentionTargetForInstance reads VaultConfig.RetentionDisposition (via
// ResolveRetentionDisposition) and writes the resolved value to the
// runner. This is the load-bearing plumbing — without it, the per-chunk
// gate in tryRetainChunk always sees an empty string and defaults to
// "delete" regardless of operator intent. Refreshes on every sweep
// tick so live config edits propagate without restart.
func TestRetentionTargetThreadsDispositionFromVaultConfig(t *testing.T) {
	t.Parallel()

	// 1:1 vault:tier — IDs match.
	vaultID := glid.New()
	instID := vaultID
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
					ID:     policyID,
					MaxAge: strPtr("1h"),
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

			inst := &VaultInstance{
				VaultID:  instID,
				Chunks:  &retentionFakeChunkManager{},
				Indexes: &retentionFakeIndexManager{},
			}
			active := make(map[string]bool)
			target := orch.retentionTargetForInstance(cfg, cfg.Vaults[0], inst, active)
			if target == nil {
				t.Fatal("expected non-nil sweep target")
			}
			if got := target.runner.disposition; got != c.want {
				t.Errorf("runner.disposition = %q, want %q", got, c.want)
			}
		})
	}
}

// TestResolveRetentionDispositionDefaults locks the resolver behavior
// for empty + unrecognized values. The retention runner uses the
// resolved value, so anything not "route" must coerce to "delete" —
// no silent passthrough that could let a typo become a cascade.
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
