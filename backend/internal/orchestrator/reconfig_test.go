package orchestrator_test

import (
	"context"
	"gastrolog/internal/glid"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
)

// fakeSystemLoader implements orchestrator.SystemLoader for tests.
type fakeSystemLoader struct {
	cfg *system.Config
}

func (f *fakeSystemLoader) Load(_ context.Context) (*system.System, error) {
	if f.cfg == nil {
		return nil, nil
	}
	return &system.System{Config: *f.cfg}, nil
}

// memVaultCfg creates a VaultConfig for a memory-backed vault.
func memVaultCfg(vaultID glid.GLID, loader *fakeSystemLoader) system.VaultConfig {
	v := system.VaultConfig{
		ID:   vaultID,
		Name: "vault-" + vaultID.String()[:8],
		Type: system.VaultTypeMemory,
	}
	return v
}

func TestReloadFilters(t *testing.T) {
	t.Parallel()
	loader := &fakeSystemLoader{}
	orch, vaults := newFilteredTestSetupWithLoader(t, loader)

	// gastrolog-4kkoo (Phase 5): explicit priorities so the prod route
	// fires before the archive catch-all under first-match-wins.
	loader.cfg = &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Name: "prod", Priority: 10, Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "env=prod"}}}, Destinations: []glid.GLID{vaults.prod}, Enabled: true},
			{ID: glid.New(), Name: "archive", Priority: 100, Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaults.archive}, Enabled: true},
		},
	}
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}

	// First-match-wins: env=prod record fires only the prod route.
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("prod message"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if count := countRecords(t, vaults.cms[vaults.prod]); count != 1 {
		t.Errorf("prod: expected 1 record, got %d", count)
	}
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 0 {
		t.Errorf("archive: expected 0 records (prod claimed the match), got %d", count)
	}

	// Now flip prod's expression to env=staging — env=prod records will
	// now fall through to archive.
	loader.cfg = &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Name: "prod", Priority: 10, Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "env=staging"}}}, Destinations: []glid.GLID{vaults.prod}, Enabled: true},
			{ID: glid.New(), Name: "archive", Priority: 100, Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaults.archive}, Enabled: true},
		},
	}
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters (2nd): %v", err)
	}

	// Ingest another prod message — should fall through to archive now.
	rec2 := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("prod message 2"),
	}
	if err := orch.Ingest(rec2); err != nil {
		t.Fatalf("Ingest (2nd): %v", err)
	}

	// prod still has 1 (the original), archive picks up the second.
	if count := countRecords(t, vaults.cms[vaults.prod]); count != 1 {
		t.Errorf("prod after filter change: expected 1 record, got %d", count)
	}
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 1 {
		t.Errorf("archive after filter change: expected 1 record, got %d", count)
	}
}

func TestReloadFiltersInvalidExpression(t *testing.T) {
	t.Parallel()
	loader := &fakeSystemLoader{}
	orch, vaults := newFilteredTestSetupWithLoader(t, loader)

	loader.cfg = &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "(unclosed"}}}, Destinations: []glid.GLID{vaults.prod}, Enabled: true},
		},
	}
	err := orch.ReloadFilters(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid filter expression")
	}
}

func TestReloadFiltersIgnoresUnknownVaults(t *testing.T) {
	t.Parallel()
	loader := &fakeSystemLoader{}
	orch, vaults := newFilteredTestSetupWithLoader(t, loader)

	nonexistentVaultID := glid.New()

	// Include a vault that doesn't exist - should be ignored.
	loader.cfg = &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "env=prod"}}}, Destinations: []glid.GLID{vaults.prod}, Enabled: true},
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{nonexistentVaultID}, Enabled: true},
		},
	}
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
}

func TestAddVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "env=test"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Verify vault was added.
	cm := orch.ChunkManager(vaultID)
	if cm == nil {
		t.Fatal("ChunkManager not found after AddVault")
	}

	// Verify filtering works.
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "test"},
		Raw:      []byte("test message"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if count := countRecords(t, cm); count != 1 {
		t.Errorf("expected 1 record, got %d", count)
	}
}

func TestAddVaultDuplicate(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Adding again should fail.
	err = orch.AddVault(context.Background(), vaultCfg, factories)
	if err == nil {
		t.Fatal("expected error for duplicate vault")
	}
}

func TestRemoveVaultEmpty(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Remove should succeed since no data.
	if err := orch.RemoveVault(vaultID); err != nil {
		t.Fatalf("RemoveVault: %v", err)
	}

	// Verify vault was removed.
	if cm := orch.ChunkManager(vaultID); cm != nil {
		t.Error("ChunkManager should be nil after RemoveVault")
	}
}

func TestRemoveVaultNotEmpty(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Add some data.
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{},
		Raw:      []byte("data"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Remove should fail.
	err = orch.RemoveVault(vaultID)
	if err == nil {
		t.Fatal("expected error for non-empty vault")
	}
}

func TestRemoveVaultNotFound(t *testing.T) {
	t.Parallel()
	loader := &fakeSystemLoader{cfg: &system.Config{}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	err = orch.RemoveVault(glid.New())
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestForceRemoveVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Ingest data and cause a seal to create sealed chunks.
	cm := orch.ChunkManager(vaultID)
	cm.SetRotationPolicy(chunk.NewRecordCountPolicy(3))

	for range 10 {
		rec := chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("test message"),
		}
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	// Verify vault has data.
	metas, _ := cm.List()
	if len(metas) == 0 {
		t.Fatal("expected chunks in vault")
	}

	// Normal remove should fail.
	if err := orch.RemoveVault(vaultID); err == nil {
		t.Fatal("expected error for non-empty vault")
	}

	// Force remove should succeed.
	if err := orch.ForceRemoveVault(vaultID); err != nil {
		t.Fatalf("ForceRemoveVault: %v", err)
	}

	// Verify vault was completely removed.
	if cm := orch.ChunkManager(vaultID); cm != nil {
		t.Error("ChunkManager should be nil after ForceRemoveVault")
	}
	if im := orch.IndexManager(vaultID); im != nil {
		t.Error("IndexManager should be nil after ForceRemoveVault")
	}
}

func TestForceRemoveVaultNotFound(t *testing.T) {
	t.Parallel()
	loader := &fakeSystemLoader{cfg: &system.Config{}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	err = orch.ForceRemoveVault(glid.New())
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestForceRemoveEmptyVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Force remove empty vault should succeed.
	if err := orch.ForceRemoveVault(vaultID); err != nil {
		t.Fatalf("ForceRemoveVault: %v", err)
	}

	if cm := orch.ChunkManager(vaultID); cm != nil {
		t.Error("ChunkManager should be nil after ForceRemoveVault")
	}
}

func TestAddIngesterWhileRunning(t *testing.T) {
	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))

	// gastrolog-4kkoo (Phase 5): catch-all route into the vault.
	cr, _ := orchestrator.CompileRoute(glid.New(), "all", 0, "*",
		[]orchestrator.RouteDestination{{VaultID: defaultID}}, "fanout")
	orch.SetRouteSet(orchestrator.NewRouteSet([]*orchestrator.CompiledRoute{cr}))

	// Start orchestrator.
	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Stop()

	// Add ingester while running.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"source": "dynamic"}, Raw: []byte("dynamic message")},
	})

	ingesterID := glid.New()
	if err := orch.AddIngester(ingesterID, "test", "mock", false, recv); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	// Wait for message to be processed.
	<-recv.started
	time.Sleep(50 * time.Millisecond)

	// Verify message was received.
	msgs := getRecordMessages(t, s.CM)
	found := slices.Contains(msgs, "dynamic message")
	if !found {
		t.Error("dynamic message not found")
	}
}

func TestAddIngesterReplacesDuplicate(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	ingesterID := glid.New()
	recv1 := newBlockingIngester()
	recv2 := newBlockingIngester()

	if err := orch.AddIngester(ingesterID, "test-1", "mock", false, recv1); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	// Adding with the same ID should replace, not error.
	if err := orch.AddIngester(ingesterID, "test-2", "mock", false, recv2); err != nil {
		t.Fatalf("AddIngester (replace): %v", err)
	}

	if ids := orch.ListIngesters(); len(ids) != 1 {
		t.Fatalf("expected 1 ingester, got %d", len(ids))
	}
}

func TestRemoveIngesterNotRunning(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	ingesterID := glid.New()
	recv := newBlockingIngester()
	if err := orch.AddIngester(ingesterID, "test", "mock", false, recv); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	// Remove while not running should succeed.
	if err := orch.RemoveIngester(ingesterID); err != nil {
		t.Fatalf("RemoveIngester: %v", err)
	}

	// Verify removed.
	ingesters := orch.ListIngesters()
	for _, id := range ingesters {
		if id == ingesterID {
			t.Error("ingester should have been removed")
		}
	}
}

func TestRemoveIngesterWhileRunning(t *testing.T) {
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})

	defaultID := glid.New()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, cm, nil, nil))

	ingesterID := glid.New()
	recv := newBlockingIngester()
	if err := orch.AddIngester(ingesterID, "test", "mock", false, recv); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Stop()

	// Wait for ingester to start.
	<-recv.started

	// Remove while running should succeed and stop the ingester.
	if err := orch.RemoveIngester(ingesterID); err != nil {
		t.Fatalf("RemoveIngester: %v", err)
	}

	// Verify ingester was stopped.
	select {
	case <-recv.stopped:
		// Good - ingester stopped.
	case <-time.After(time.Second):
		t.Fatal("ingester did not stop after RemoveIngester")
	}

	// Verify removed from list.
	ingesters := orch.ListIngesters()
	for _, id := range ingesters {
		if id == ingesterID {
			t.Error("ingester should have been removed from list")
		}
	}
}

func TestRemoveIngesterNotFound(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	err = orch.RemoveIngester(glid.New())
	if err == nil {
		t.Fatal("expected error for nonexistent ingester")
	}
}

func TestVaultConfig(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "env=prod AND level=error"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Get config back.
	gotCfg, err := orch.VaultConfig(vaultID)
	if err != nil {
		t.Fatalf("VaultConfig: %v", err)
	}

	if gotCfg.ID != vaultID {
		t.Errorf("ID: expected %s, got %s", vaultID, gotCfg.ID)
	}
}

func TestVaultConfigNotFound(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = orch.VaultConfig(glid.New())
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

// gastrolog-4kkoo (Phase 5): TestUpdateVaultFilter / TestUpdateVaultFilterNotFound /
// TestUpdateVaultFilterInvalid removed. The Phase-4 UpdateVaultFilter API
// is gone — vaults no longer carry filters; routes do. The equivalent
// behavior (changing where a record lands when its source attrs match)
// is exercised by TestReloadFilters in this file via swapping the
// route's match expression and calling ReloadFilters.

func TestSetRotationPolicyOnVaultDirectly(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	// Create a vault with default rotation policy (10000 records).
	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Get chunk manager and set rotation policy directly.
	cm := orch.ChunkManager(vaultID)
	cm.SetRotationPolicy(chunk.NewRecordCountPolicy(3))

	// Ingest 10 records - should trigger multiple rotations with limit of 3.
	for range 10 {
		rec := chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("test message"),
		}
		if err := orch.Ingest(rec); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	// Should have multiple chunks: 3+3+3+1 = 4 chunks.
	metas, err := cm.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(metas) < 3 {
		t.Errorf("expected at least 3 chunks due to rotation policy, got %d", len(metas))
	}
}

func TestPauseVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Ingest a record before pausing.
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{},
		Raw:      []byte("before pause"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	cm := orch.ChunkManager(vaultID)
	if count := countRecords(t, cm); count != 1 {
		t.Fatalf("expected 1 record before pause, got %d", count)
	}

	// Disable the vault.
	if err := orch.DisableVault(vaultID); err != nil {
		t.Fatalf("DisableVault: %v", err)
	}
	if orch.IsVaultEnabled(vaultID) {
		t.Fatal("vault should be disabled")
	}

	// Ingest another record - should be silently dropped for this vault.
	rec2 := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{},
		Raw:      []byte("while disabled"),
	}
	if err := orch.Ingest(rec2); err != nil {
		t.Fatalf("Ingest while disabled: %v", err)
	}

	if count := countRecords(t, cm); count != 1 {
		t.Errorf("expected 1 record while disabled, got %d", count)
	}
}

func TestResumeVault(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Disable then re-enable.
	if err := orch.DisableVault(vaultID); err != nil {
		t.Fatalf("DisableVault: %v", err)
	}
	if err := orch.EnableVault(vaultID); err != nil {
		t.Fatalf("EnableVault: %v", err)
	}
	if !orch.IsVaultEnabled(vaultID) {
		t.Fatal("vault should be enabled after re-enable")
	}

	// Ingest should work after re-enable.
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{},
		Raw:      []byte("after resume"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest after resume: %v", err)
	}

	cm := orch.ChunkManager(vaultID)
	if count := countRecords(t, cm); count != 1 {
		t.Errorf("expected 1 record after resume, got %d", count)
	}
}

func TestDisableVaultNotFound(t *testing.T) {
	t.Parallel()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}

	if err := orch.DisableVault(glid.New()); err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if err := orch.EnableVault(glid.New()); err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestDisableVaultDoesNotAffectQuery(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
	}}
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := memVaultCfg(vaultID, loader)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Ingest data, then pause.
	for range 5 {
		if err := orch.Ingest(chunk.Record{
			IngestTS: time.Now(),
			Raw:      []byte("test message"),
		}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	if err := orch.DisableVault(vaultID); err != nil {
		t.Fatalf("DisableVault: %v", err)
	}

	// Query should still work while disabled.
	results, _, err := orch.Search(context.Background(), vaultID, query.Query{}, nil)
	if err != nil {
		t.Fatalf("Search while disabled: %v", err)
	}

	count := 0
	for _, err := range results {
		if err != nil {
			t.Fatalf("Search result error: %v", err)
		}
		count++
	}
	if count != 5 {
		t.Errorf("expected 5 results while disabled, got %d", count)
	}
}

func TestRetentionSingleJobRegistered(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	retPolicyID := glid.New()

	loader := &fakeSystemLoader{cfg: &system.Config{
		Routes: []system.RouteConfig{
			{ID: glid.New(), Stages: []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}}, Destinations: []glid.GLID{vaultID}, Enabled: true},
		},
		RetentionPolicies: []system.RetentionPolicyConfig{
			{ID: retPolicyID, Name: "age-2m", MaxAge: strPtr("2m")},
		},
		Vaults: []system.VaultConfig{
			{ID: vaultID, Name: "src"},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{"memory": chunkmem.NewFactory()},
		IndexManagers: map[string]index.ManagerFactory{"memory": indexmem.NewFactory()},
	}
	if err := orch.AddVault(context.Background(), loader.cfg.Vaults[0], factories); err != nil {
		t.Fatal(err)
	}

	// The single "retention" job should be registered, not per-vault jobs.
	sched := orch.Scheduler()
	if !sched.HasJob("retention") {
		t.Fatal("single retention sweep job should exist")
	}
	perVaultJobName := "retention:" + vaultID.String()
	if sched.HasJob(perVaultJobName) {
		t.Fatal("per-vault retention job should NOT exist — retention uses a single discovery-based sweep")
	}
}

// gastrolog-4kkoo (Phase 5): TestUpdateVaultFilterNotFound and
// TestUpdateVaultFilterInvalid removed alongside UpdateVaultFilter.
// Invalid-expression handling at the route level is covered by
// TestReloadFiltersInvalidExpression earlier in this file, which
// asserts that a route carrying a bad expression fails ReloadFilters.

func strPtr(s string) *string { return &s }

// TestReloadRotationPolicies_AppliesSynchronously is the regression test for
// gastrolog-1rj63: the dispatcher must hot-swap the active chunk manager's
// rotation policy the moment the FSM commits a change, not 15 seconds later
// on the next rotationSweep tick.
//
// Setup: a vault is configured with a 1000-record rotation policy and has
// accumulated 100 records. The user then assigns a different policy with
// max=50, which the current state already exceeds. Without the fix, the
// chunk manager keeps the 1000-record policy and the next Append does not
// rotate; with the fix, ReloadRotationPolicies sets the new policy and the
// next Append seals the chunk + opens a fresh one.
func TestReloadRotationPolicies_AppliesSynchronously(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	oldPolicyID := glid.New()
	newPolicyID := glid.New()
	oldMax := int64(1000)
	newMax := int64(50)

	loader := &fakeSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID, Name: "test", Type: system.VaultTypeMemory, RotationPolicyID: &oldPolicyID, Enabled: true},
		},
		RotationPolicies: []system.RotationPolicyConfig{
			{ID: oldPolicyID, Name: "loose", MaxRecords: &oldMax},
			{ID: newPolicyID, Name: "tight", MaxRecords: &newMax},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}
	s := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: recordCountPolicy(oldMax)})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	// Fill the chunk with 100 records — well under the 1000-record bound.
	for i := 0; i < 100; i++ {
		if _, _, err := s.CM.Append(chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	active := s.CM.Active()
	if active == nil {
		t.Fatal("expected an active chunk after 100 appends")
	}
	firstChunkID := active.ID
	if active.RecordCount != 100 {
		t.Fatalf("expected 100 records in active chunk, got %d", active.RecordCount)
	}

	// Reassign the vault to the tighter policy (max=50). Current state of
	// 100 records already exceeds the new bound.
	loader.cfg.Vaults[0].RotationPolicyID = &newPolicyID
	if err := orch.ReloadRotationPolicies(context.Background()); err != nil {
		t.Fatalf("ReloadRotationPolicies: %v", err)
	}

	// One more append must trigger rotation: ShouldRotate sees
	// state.Records+1 = 101 > newMax (50) and seals before appending.
	if _, _, err := s.CM.Append(chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{},
		Raw:      []byte("x"),
	}); err != nil {
		t.Fatalf("append after policy switch: %v", err)
	}

	active = s.CM.Active()
	if active == nil {
		t.Fatal("expected an active chunk after policy switch")
	}
	if active.ID == firstChunkID {
		t.Errorf("expected rotation after policy switch; active chunk is still %s with %d records — new policy not applied",
			active.ID, active.RecordCount)
	}
	if active.RecordCount != 1 {
		t.Errorf("expected new chunk to have 1 record, got %d", active.RecordCount)
	}
}

// TestReloadRotationPolicies_SkipsFollowers verifies the reload path does not
// stomp on follower replicas — the rotationSweep is the sole authority for
// followers (it stamps NeverRotatePolicy each tick). Without this guard a
// follower's policy would briefly flap to a user policy between sweeps.
func TestReloadRotationPolicies_SkipsFollowers(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	maxRecs := int64(50)

	loader := &fakeSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID, Name: "test", Type: system.VaultTypeMemory, RotationPolicyID: &policyID, Enabled: true},
		},
		RotationPolicies: []system.RotationPolicyConfig{
			{ID: policyID, Name: "tight", MaxRecords: &maxRecs},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	// Build a follower vault: NeverRotatePolicy at the chunk manager, IsFollower
	// flag on the orchestrator's VaultInstance.
	s := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NeverRotatePolicy{}})
	v := orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE)
	v.Instance.IsFollower = true
	orch.RegisterVault(v)

	// Fill well past the user-policy bound. If ReloadRotationPolicies were
	// to touch the follower, the next append would rotate.
	for i := 0; i < 200; i++ {
		if _, _, err := s.CM.Append(chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}

	if err := orch.ReloadRotationPolicies(context.Background()); err != nil {
		t.Fatalf("ReloadRotationPolicies: %v", err)
	}

	// One more append: with NeverRotatePolicy preserved, no rotation.
	active := s.CM.Active()
	firstID := active.ID
	if _, _, err := s.CM.Append(chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{},
		Raw:      []byte("x"),
	}); err != nil {
		t.Fatalf("append after reload: %v", err)
	}
	active = s.CM.Active()
	if active.ID != firstID {
		t.Errorf("follower's chunk rotated unexpectedly — reload path must skip followers")
	}
}

// TestReloadRotationPolicies_NilPolicyID is a happy-path edge case: vault has
// no rotation policy assigned. Reload must not panic and must leave the
// chunk manager's current policy untouched.
func TestReloadRotationPolicies_NilPolicyID(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	loader := &fakeSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID, Name: "test", Type: system.VaultTypeMemory, Enabled: true},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}
	s := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: recordCountPolicy(10)})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	if err := orch.ReloadRotationPolicies(context.Background()); err != nil {
		t.Fatalf("ReloadRotationPolicies: %v", err)
	}
	// Original 10-record policy should still hold: 11th append rotates.
	for i := 0; i < 11; i++ {
		if _, _, err := s.CM.Append(chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	active := s.CM.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	if active.RecordCount != 1 {
		t.Errorf("expected new chunk with 1 record (rotation at 11th append), got %d", active.RecordCount)
	}
}

// TestApplyRotationPolicyForRole_LeaderAppliesUserPolicy is the regression
// test for gastrolog-50n4b on the follower→leader transition. After a node
// flips from follower to leader, its chunk manager carries
// NeverRotatePolicy (set previously by the sweep); the dispatcher now
// calls ApplyRotationPolicyForRole to switch it to the user-configured
// policy immediately, instead of waiting up to 15 s for the next
// rotationSweep tick.
func TestApplyRotationPolicyForRole_LeaderAppliesUserPolicy(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	maxRecs := int64(50)

	loader := &fakeSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID, Name: "test", Type: system.VaultTypeMemory, RotationPolicyID: &policyID, Enabled: true},
		},
		RotationPolicies: []system.RotationPolicyConfig{
			{ID: policyID, Name: "tight", MaxRecords: &maxRecs},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	// Build a leader vault instance whose chunk manager still has
	// NeverRotatePolicy — simulating just after a follower→leader flip
	// (IsFollower set to false but chunk manager not yet updated).
	s := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: chunk.NeverRotatePolicy{}})
	orch.RegisterVault(orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE))

	if err := orch.ApplyRotationPolicyForRole(context.Background(), vaultID); err != nil {
		t.Fatalf("ApplyRotationPolicyForRole: %v", err)
	}

	// Append 51 records; the 51st must trigger rotation (state.Records+1 > maxRecs=50).
	for i := 0; i < 51; i++ {
		if _, _, err := s.CM.Append(chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	active := s.CM.Active()
	if active == nil {
		t.Fatal("expected active chunk")
	}
	if active.RecordCount != 1 {
		t.Errorf("expected rotation after 51 appends with maxRecords=50; new chunk should have 1 record, got %d — user policy not applied",
			active.RecordCount)
	}
}

// TestApplyRotationPolicyForRole_FollowerStampsNeverRotate is the
// regression test for gastrolog-50n4b on the leader→follower transition.
// After a node flips from leader to follower, its chunk manager still
// carries the user policy until the next sweep tick (~15 s). The
// dispatcher now stamps NeverRotatePolicy immediately so the follower's
// manager can't rotate independently during the gap.
func TestApplyRotationPolicyForRole_FollowerStampsNeverRotate(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	policyID := glid.New()
	maxRecs := int64(50)

	loader := &fakeSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID, Name: "test", Type: system.VaultTypeMemory, RotationPolicyID: &policyID, Enabled: true},
		},
		RotationPolicies: []system.RotationPolicyConfig{
			{ID: policyID, Name: "tight", MaxRecords: &maxRecs},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}

	// Build a follower vault instance whose chunk manager still has the
	// user policy — simulating just after a leader→follower flip.
	s := memtest.MustNewVault(t, chunkmem.Config{RotationPolicy: recordCountPolicy(maxRecs)})
	v := orchestrator.NewVaultFromComponents(vaultID, s.CM, s.IM, s.QE)
	v.Instance.IsFollower = true
	orch.RegisterVault(v)

	if err := orch.ApplyRotationPolicyForRole(context.Background(), vaultID); err != nil {
		t.Fatalf("ApplyRotationPolicyForRole: %v", err)
	}

	// Append 101 records; with NeverRotatePolicy, none should rotate.
	for i := 0; i < 101; i++ {
		if _, _, err := s.CM.Append(chunk.Record{
			IngestTS: time.Now(),
			Attrs:    chunk.Attributes{},
			Raw:      []byte("x"),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	active := s.CM.Active()
	if active.RecordCount != 101 {
		t.Errorf("expected single chunk with 101 records (NeverRotatePolicy in effect), got %d — follower chunk manager still rotating under user policy",
			active.RecordCount)
	}
}

// TestApplyRotationPolicyForRole_NoInstanceIsNoop covers the warm-up case
// where a vault is registered but its Instance is still nil — the call
// must not panic and must not error.
func TestApplyRotationPolicyForRole_NoInstanceIsNoop(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	loader := &fakeSystemLoader{cfg: &system.Config{
		Vaults: []system.VaultConfig{
			{ID: vaultID, Name: "test", Type: system.VaultTypeMemory, Enabled: true},
		},
	}}

	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: loader})
	if err != nil {
		t.Fatal(err)
	}
	orch.RegisterVault(orchestrator.NewVault(vaultID, nil))

	if err := orch.ApplyRotationPolicyForRole(context.Background(), vaultID); err != nil {
		t.Errorf("ApplyRotationPolicyForRole on instance-less vault: %v", err)
	}
}
