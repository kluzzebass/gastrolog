package orchestrator

import (
	"log/slog"
	"testing"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/glid"
)

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
		tierID:  sourceID, // 1:1 vault:tier
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
		tierID:  sourceID,
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
