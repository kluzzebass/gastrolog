package orchestrator_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// fakeConfigLoader implements orchestrator.ConfigLoader for tests.
type fakeConfigLoader struct {
	cfg *config.Config
}

func (f *fakeConfigLoader) Load(_ context.Context) (*config.Config, error) {
	return f.cfg, nil
}

func TestReloadFilters(t *testing.T) {
	loader := &fakeConfigLoader{}
	orch, vaults := newFilteredTestSetupWithLoader(t, loader)

	prodFilterID := uuid.Must(uuid.NewV7())
	catchAllFilterID := uuid.Must(uuid.NewV7())

	// Initially set filters: prod gets env=prod, archive is catch-all.
	loader.cfg = &config.Config{
		Filters: []config.FilterConfig{
			{ID: prodFilterID, Expression: "env=prod"},
			{ID: catchAllFilterID, Expression: "*"},
		},
		Vaults: []config.VaultConfig{
			{ID: vaults.prod, Filter: new(prodFilterID)},
			{ID: vaults.archive, Filter: new(catchAllFilterID)},
		},
	}
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}

	// Ingest a prod message - should go to prod and archive.
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
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 1 {
		t.Errorf("archive: expected 1 record, got %d", count)
	}

	// Now update filters: prod gets env=staging instead.
	loader.cfg = &config.Config{
		Filters: []config.FilterConfig{
			{ID: prodFilterID, Expression: "env=staging"},
			{ID: catchAllFilterID, Expression: "*"},
		},
		Vaults: []config.VaultConfig{
			{ID: vaults.prod, Filter: new(prodFilterID)},
			{ID: vaults.archive, Filter: new(catchAllFilterID)},
		},
	}
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters (2nd): %v", err)
	}

	// Ingest another prod message - should only go to archive now.
	rec2 := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("prod message 2"),
	}
	if err := orch.Ingest(rec2); err != nil {
		t.Fatalf("Ingest (2nd): %v", err)
	}

	// prod should still have 1 (old message), archive should have 2.
	if count := countRecords(t, vaults.cms[vaults.prod]); count != 1 {
		t.Errorf("prod after filter change: expected 1 record, got %d", count)
	}
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 2 {
		t.Errorf("archive after filter change: expected 2 records, got %d", count)
	}
}

func TestReloadFiltersInvalidExpression(t *testing.T) {
	loader := &fakeConfigLoader{}
	orch, vaults := newFilteredTestSetupWithLoader(t, loader)

	invalidFilterID := uuid.Must(uuid.NewV7())

	loader.cfg = &config.Config{
		Filters: []config.FilterConfig{
			{ID: invalidFilterID, Expression: "(unclosed"},
		},
		Vaults: []config.VaultConfig{
			{ID: vaults.prod, Filter: new(invalidFilterID)},
		},
	}
	err := orch.ReloadFilters(context.Background())
	if err == nil {
		t.Fatal("expected error for invalid filter expression")
	}
}

func TestReloadFiltersIgnoresUnknownVaults(t *testing.T) {
	loader := &fakeConfigLoader{}
	orch, vaults := newFilteredTestSetupWithLoader(t, loader)

	prodFilterID := uuid.Must(uuid.NewV7())
	catchAllFilterID := uuid.Must(uuid.NewV7())
	nonexistentVaultID := uuid.Must(uuid.NewV7())

	// Include a vault that doesn't exist - should be ignored.
	loader.cfg = &config.Config{
		Filters: []config.FilterConfig{
			{ID: prodFilterID, Expression: "env=prod"},
			{ID: catchAllFilterID, Expression: "*"},
		},
		Vaults: []config.VaultConfig{
			{ID: vaults.prod, Filter: new(prodFilterID)},
			{ID: nonexistentVaultID, Filter: new(catchAllFilterID)},
		},
	}
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
}

func TestAddVault(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "env=test"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Adding again should fail.
	err := orch.AddVault(context.Background(), vaultCfg, factories)
	if err == nil {
		t.Fatal("expected error for duplicate vault")
	}
}

func TestRemoveVaultEmpty(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	err := orch.RemoveVault(vaultID)
	if err == nil {
		t.Fatal("expected error for non-empty vault")
	}
}

func TestRemoveVaultNotFound(t *testing.T) {
	loader := &fakeConfigLoader{cfg: &config.Config{}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	err := orch.RemoveVault(uuid.Must(uuid.NewV7()))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestForceRemoveVault(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	loader := &fakeConfigLoader{cfg: &config.Config{}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	err := orch.ForceRemoveVault(uuid.Must(uuid.NewV7()))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestForceRemoveEmptyVault(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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

	defaultID := uuid.Must(uuid.NewV7())
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVault(defaultID, s.CM, s.IM, s.QE))

	// Set catch-all filter.
	filter, _ := orchestrator.CompileFilter(defaultID, "*")
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{filter}))

	// Start orchestrator.
	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Stop()

	// Add ingester while running.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"source": "dynamic"}, Raw: []byte("dynamic message")},
	})

	ingesterID := uuid.Must(uuid.NewV7())
	if err := orch.AddIngester(ingesterID, "test", "mock", recv); err != nil {
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

func TestAddIngesterDuplicate(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	ingesterID := uuid.Must(uuid.NewV7())
	recv1 := newBlockingIngester()
	recv2 := newBlockingIngester()

	if err := orch.AddIngester(ingesterID, "test-1", "mock", recv1); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	err := orch.AddIngester(ingesterID, "test-2", "mock", recv2)
	if err == nil {
		t.Fatal("expected error for duplicate ingester")
	}
}

func TestRemoveIngesterNotRunning(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	ingesterID := uuid.Must(uuid.NewV7())
	recv := newBlockingIngester()
	if err := orch.AddIngester(ingesterID, "test", "mock", recv); err != nil {
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

	defaultID := uuid.Must(uuid.NewV7())
	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterVault(orchestrator.NewVault(defaultID, cm, nil, nil))

	ingesterID := uuid.Must(uuid.NewV7())
	recv := newBlockingIngester()
	if err := orch.AddIngester(ingesterID, "test", "mock", recv); err != nil {
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
	orch := orchestrator.New(orchestrator.Config{})

	err := orch.RemoveIngester(uuid.Must(uuid.NewV7()))
	if err == nil {
		t.Fatal("expected error for nonexistent ingester")
	}
}

func TestVaultConfig(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "env=prod AND level=error"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	// VaultConfig does not track the original filter UUID reference,
	// so Filter is nil in the returned config.
	if gotCfg.Filter != nil {
		t.Errorf("Filter: expected nil (not tracked by orchestrator), got %v", gotCfg.Filter)
	}
}

func TestVaultConfigNotFound(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	_, err := orch.VaultConfig(uuid.Must(uuid.NewV7()))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestUpdateVaultFilter(t *testing.T) {
	orch, vaults := newFilteredTestSetup(t)

	// Set initial filter: prod gets env=prod.
	prodFilter, _ := orchestrator.CompileFilter(vaults.prod, "env=prod")
	archiveFilter, _ := orchestrator.CompileFilter(vaults.archive, "*")
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{prodFilter, archiveFilter}))

	// Ingest a prod message.
	rec := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("prod message 1"),
	}
	if err := orch.Ingest(rec); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// prod should have 1 message.
	if count := countRecords(t, vaults.cms[vaults.prod]); count != 1 {
		t.Errorf("prod: expected 1 record, got %d", count)
	}

	// Update prod's filter to env=staging.
	if err := orch.UpdateVaultFilter(vaults.prod, "env=staging"); err != nil {
		t.Fatalf("UpdateVaultFilter: %v", err)
	}

	// Ingest another prod message - should NOT go to prod anymore.
	rec2 := chunk.Record{
		IngestTS: time.Now(),
		Attrs:    chunk.Attributes{"env": "prod"},
		Raw:      []byte("prod message 2"),
	}
	if err := orch.Ingest(rec2); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// prod should still have 1 message (filter changed).
	if count := countRecords(t, vaults.cms[vaults.prod]); count != 1 {
		t.Errorf("prod after filter change: expected 1 record, got %d", count)
	}

	// archive should have 2 (catch-all).
	if count := countRecords(t, vaults.cms[vaults.archive]); count != 2 {
		t.Errorf("archive: expected 2 records, got %d", count)
	}
}

func TestSetRotationPolicyOnVaultDirectly(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	// Create a vault with default rotation policy (10000 records).
	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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
	orch := orchestrator.New(orchestrator.Config{})

	if err := orch.DisableVault(uuid.Must(uuid.NewV7())); err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if err := orch.EnableVault(uuid.Must(uuid.NewV7())); err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestDisableVaultDoesNotAffectQuery(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

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

func TestUpdateVaultFilterNotFound(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	err := orch.UpdateVaultFilter(uuid.Must(uuid.NewV7()), "*")
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
}

func TestUpdateVaultFilterInvalid(t *testing.T) {
	filterID := uuid.Must(uuid.NewV7())
	vaultID := uuid.Must(uuid.NewV7())

	loader := &fakeConfigLoader{cfg: &config.Config{
		Filters: []config.FilterConfig{
			{ID: filterID, Expression: "*"},
		},
	}}
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: loader})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	vaultCfg := config.VaultConfig{
		ID:     vaultID,
		Type:   "memory",
		Filter: new(filterID),
	}

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Invalid filter expression.
	err := orch.UpdateVaultFilter(vaultID, "(unclosed")
	if err == nil {
		t.Error("expected error for invalid filter expression")
	}
}
