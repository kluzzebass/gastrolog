package orchestrator_test

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	"gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
)

func TestUpdateRoutes(t *testing.T) {
	orch, stores := newRoutedTestSetup(t)

	// Initially set routes: prod gets env=prod, archive is catch-all.
	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "prod", Route: config.StringPtr("env=prod")},
			{ID: "archive", Route: config.StringPtr("*")},
		},
	}
	if err := orch.UpdateRoutes(cfg); err != nil {
		t.Fatalf("UpdateRoutes: %v", err)
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

	if count := countRecords(t, stores["prod"]); count != 1 {
		t.Errorf("prod: expected 1 record, got %d", count)
	}
	if count := countRecords(t, stores["archive"]); count != 1 {
		t.Errorf("archive: expected 1 record, got %d", count)
	}

	// Now update routes: prod gets env=staging instead.
	cfg2 := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "prod", Route: config.StringPtr("env=staging")},
			{ID: "archive", Route: config.StringPtr("*")},
		},
	}
	if err := orch.UpdateRoutes(cfg2); err != nil {
		t.Fatalf("UpdateRoutes (2nd): %v", err)
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
	if count := countRecords(t, stores["prod"]); count != 1 {
		t.Errorf("prod after route change: expected 1 record, got %d", count)
	}
	if count := countRecords(t, stores["archive"]); count != 2 {
		t.Errorf("archive after route change: expected 2 records, got %d", count)
	}
}

func TestUpdateRoutesInvalidExpression(t *testing.T) {
	orch, _ := newRoutedTestSetup(t)

	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "prod", Route: config.StringPtr("(unclosed")},
		},
	}
	err := orch.UpdateRoutes(cfg)
	if err == nil {
		t.Fatal("expected error for invalid route expression")
	}
}

func TestUpdateRoutesIgnoresUnknownStores(t *testing.T) {
	orch, _ := newRoutedTestSetup(t)

	// Include a store that doesn't exist - should be ignored.
	cfg := &config.Config{
		Stores: []config.StoreConfig{
			{ID: "prod", Route: config.StringPtr("env=prod")},
			{ID: "nonexistent", Route: config.StringPtr("*")},
		},
	}
	if err := orch.UpdateRoutes(cfg); err != nil {
		t.Fatalf("UpdateRoutes: %v", err)
	}
}

func TestAddStore(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	storeCfg := config.StoreConfig{
		ID:    "new-store",
		Type:  "memory",
		Route: config.StringPtr("env=test"),
	}

	if err := orch.AddStore(storeCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
	}

	// Verify store was added.
	cm := orch.ChunkManager("new-store")
	if cm == nil {
		t.Fatal("ChunkManager not found after AddStore")
	}

	// Verify routing works.
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

func TestAddStoreDuplicate(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	storeCfg := config.StoreConfig{
		ID:    "store1",
		Type:  "memory",
		Route: config.StringPtr("*"),
	}

	if err := orch.AddStore(storeCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
	}

	// Adding again should fail.
	err := orch.AddStore(storeCfg, factories)
	if err == nil {
		t.Fatal("expected error for duplicate store")
	}
}

func TestRemoveStoreEmpty(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	storeCfg := config.StoreConfig{
		ID:    "temp-store",
		Type:  "memory",
		Route: config.StringPtr("*"),
	}

	if err := orch.AddStore(storeCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
	}

	// Remove should succeed since no data.
	if err := orch.RemoveStore("temp-store"); err != nil {
		t.Fatalf("RemoveStore: %v", err)
	}

	// Verify store was removed.
	if cm := orch.ChunkManager("temp-store"); cm != nil {
		t.Error("ChunkManager should be nil after RemoveStore")
	}
}

func TestRemoveStoreNotEmpty(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	storeCfg := config.StoreConfig{
		ID:    "store-with-data",
		Type:  "memory",
		Route: config.StringPtr("*"),
	}

	if err := orch.AddStore(storeCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
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
	err := orch.RemoveStore("store-with-data")
	if err == nil {
		t.Fatal("expected error for non-empty store")
	}
}

func TestRemoveStoreNotFound(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	err := orch.RemoveStore("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
}

func TestAddIngesterWhileRunning(t *testing.T) {
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})

	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := kv.NewIndexer(cm)

	im := indexmem.NewManager(
		[]index.Indexer{tokIdx, attrIdx, kvIdx},
		tokIdx,
		attrIdx,
		kvIdx,
		nil,
	)

	qe := query.New(cm, im, nil)

	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", qe)

	// Set catch-all route.
	route, _ := orchestrator.CompileRoute("default", "*")
	orch.SetRouter(orchestrator.NewRouter([]*orchestrator.CompiledRoute{route}))

	// Start orchestrator.
	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Stop()

	// Add ingester while running.
	recv := newMockIngester([]orchestrator.IngestMessage{
		{Attrs: map[string]string{"source": "dynamic"}, Raw: []byte("dynamic message")},
	})

	if err := orch.AddIngester("dynamic", recv); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	// Wait for message to be processed.
	<-recv.started
	time.Sleep(50 * time.Millisecond)

	// Verify message was received.
	msgs := getRecordMessages(t, cm)
	found := false
	for _, msg := range msgs {
		if msg == "dynamic message" {
			found = true
			break
		}
	}
	if !found {
		t.Error("dynamic message not found")
	}
}

func TestAddIngesterDuplicate(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	recv1 := newBlockingIngester()
	recv2 := newBlockingIngester()

	if err := orch.AddIngester("recv", recv1); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	err := orch.AddIngester("recv", recv2)
	if err == nil {
		t.Fatal("expected error for duplicate ingester")
	}
}

func TestRemoveIngesterNotRunning(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	recv := newBlockingIngester()
	if err := orch.AddIngester("recv", recv); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	// Remove while not running should succeed.
	if err := orch.RemoveIngester("recv"); err != nil {
		t.Fatalf("RemoveIngester: %v", err)
	}

	// Verify removed.
	ingesters := orch.Ingesters()
	for _, id := range ingesters {
		if id == "recv" {
			t.Error("ingester should have been removed")
		}
	}
}

func TestRemoveIngesterWhileRunning(t *testing.T) {
	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(10000),
	})

	orch := orchestrator.New(orchestrator.Config{})
	orch.RegisterChunkManager("default", cm)

	recv := newBlockingIngester()
	if err := orch.AddIngester("recv", recv); err != nil {
		t.Fatalf("AddIngester: %v", err)
	}

	if err := orch.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer orch.Stop()

	// Wait for ingester to start.
	<-recv.started

	// Remove while running should succeed and stop the ingester.
	if err := orch.RemoveIngester("recv"); err != nil {
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
	ingesters := orch.Ingesters()
	for _, id := range ingesters {
		if id == "recv" {
			t.Error("ingester should have been removed from list")
		}
	}
}

func TestRemoveIngesterNotFound(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	err := orch.RemoveIngester("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent ingester")
	}
}

func TestStoreConfig(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	storeCfg := config.StoreConfig{
		ID:    "test-store",
		Type:  "memory",
		Route: config.StringPtr("env=prod AND level=error"),
	}

	if err := orch.AddStore(storeCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
	}

	// Get config back.
	gotCfg, err := orch.StoreConfig("test-store")
	if err != nil {
		t.Fatalf("StoreConfig: %v", err)
	}

	if gotCfg.ID != "test-store" {
		t.Errorf("ID: expected %q, got %q", "test-store", gotCfg.ID)
	}
	if gotCfg.Route == nil || *gotCfg.Route != "env=prod AND level=error" {
		t.Errorf("Route: expected %q, got %v", "env=prod AND level=error", gotCfg.Route)
	}
}

func TestStoreConfigNotFound(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	_, err := orch.StoreConfig("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
}

func TestUpdateStoreRoute(t *testing.T) {
	orch, stores := newRoutedTestSetup(t)

	// Set initial route: prod gets env=prod.
	prodRoute, _ := orchestrator.CompileRoute("prod", "env=prod")
	archiveRoute, _ := orchestrator.CompileRoute("archive", "*")
	orch.SetRouter(orchestrator.NewRouter([]*orchestrator.CompiledRoute{prodRoute, archiveRoute}))

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
	if count := countRecords(t, stores["prod"]); count != 1 {
		t.Errorf("prod: expected 1 record, got %d", count)
	}

	// Update prod's route to env=staging.
	if err := orch.UpdateStoreRoute("prod", "env=staging"); err != nil {
		t.Fatalf("UpdateStoreRoute: %v", err)
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

	// prod should still have 1 message (route changed).
	if count := countRecords(t, stores["prod"]); count != 1 {
		t.Errorf("prod after route change: expected 1 record, got %d", count)
	}

	// archive should have 2 (catch-all).
	if count := countRecords(t, stores["archive"]); count != 2 {
		t.Errorf("archive: expected 2 records, got %d", count)
	}
}

func TestSetRotationPolicyDirectly(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	// Create a store with default rotation policy (10000 records).
	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	storeCfg := config.StoreConfig{
		ID:    "test-store",
		Type:  "memory",
		Route: config.StringPtr("*"),
	}

	if err := orch.AddStore(storeCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
	}

	// Get chunk manager and set rotation policy directly.
	cm := orch.ChunkManager("test-store")
	cm.SetRotationPolicy(chunk.NewRecordCountPolicy(3))

	// Ingest 10 records - should trigger multiple rotations with limit of 3.
	for i := 0; i < 10; i++ {
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

func TestUpdateStoreRouteNotFound(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	err := orch.UpdateStoreRoute("nonexistent", "*")
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
}

func TestUpdateStoreRouteInvalid(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	storeCfg := config.StoreConfig{
		ID:    "test-store",
		Type:  "memory",
		Route: config.StringPtr("*"),
	}

	if err := orch.AddStore(storeCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
	}

	// Invalid route expression.
	err := orch.UpdateStoreRoute("test-store", "(unclosed")
	if err == nil {
		t.Error("expected error for invalid route expression")
	}
}
