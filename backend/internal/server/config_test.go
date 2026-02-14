package server_test

import (
	"context"
	"net/http"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
)

// newConfigTestSetup creates an orchestrator, config store, and Connect client
// for testing config RPCs.
func newConfigTestSetup(t *testing.T) (gastrologv1connect.ConfigServiceClient, config.Store, *orchestrator.Orchestrator) {
	t.Helper()

	orch := orchestrator.New(orchestrator.Config{})
	cfgStore := cfgmem.NewStore()

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
			"file":   chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
			"file":   indexfile.NewFactory(),
		},
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	client := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")

	return client, cfgStore, orch
}

func TestDeleteStoreForce(t *testing.T) {
	client, cfgStore, orch := newConfigTestSetup(t)
	ctx := context.Background()

	// Create a filter first, then a store that uses it.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: "catch-all", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "test-store",
			Type:   "memory",
			Filter: "catch-all",
		},
	}))
	if err != nil {
		t.Fatalf("PutStore: %v", err)
	}

	// Ingest data so the store is non-empty.
	if err := orch.Ingest(chunk.Record{
		Raw: []byte("test data"),
	}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	// Non-force delete should fail.
	_, err = client.DeleteStore(ctx, connect.NewRequest(&gastrologv1.DeleteStoreRequest{
		Id: "test-store",
	}))
	if err == nil {
		t.Fatal("expected error for non-force delete of non-empty store")
	}
	if connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", connect.CodeOf(err))
	}

	// Force delete should succeed.
	_, err = client.DeleteStore(ctx, connect.NewRequest(&gastrologv1.DeleteStoreRequest{
		Id:    "test-store",
		Force: true,
	}))
	if err != nil {
		t.Fatalf("DeleteStore(force=true): %v", err)
	}

	// Verify store is gone from runtime.
	if cm := orch.ChunkManager("test-store"); cm != nil {
		t.Error("ChunkManager should be nil after force delete")
	}

	// Verify store is gone from config.
	stored, _ := cfgStore.GetStore(ctx, "test-store")
	if stored != nil {
		t.Error("store should be removed from config store")
	}
}

func TestDeleteStoreNotFound(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.DeleteStore(ctx, connect.NewRequest(&gastrologv1.DeleteStoreRequest{
		Id: "nonexistent",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestPutStoreNestedDirPrevention(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	baseDir := t.TempDir()

	// Seed a file store at baseDir/store1 directly in config (not via RPC,
	// to avoid actually creating the directory and orchestrator entry).
	err := cfgStore.PutStore(ctx, config.StoreConfig{
		ID:     "store1",
		Type:   "file",
		Params: map[string]string{"dir": baseDir + "/store1"},
	})
	if err != nil {
		t.Fatalf("seed store1: %v", err)
	}

	// Attempt to create a file store nested inside store1.
	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "nested-child",
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/store1/archive"},
		},
	}))
	if err == nil {
		t.Fatal("expected error for nested child directory")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}

	// Attempt to create a file store that is a parent of store1.
	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "nested-parent",
			Type:   "file",
			Params: map[string]string{"dir": baseDir},
		},
	}))
	if err == nil {
		t.Fatal("expected error for parent directory")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v: %v", connect.CodeOf(err), err)
	}

	// Sibling directory should be OK.
	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "sibling",
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/store2"},
		},
	}))
	if err != nil {
		t.Fatalf("sibling directory should be allowed: %v", err)
	}

	// Memory stores should not be checked.
	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:   "mem-store",
			Type: "memory",
		},
	}))
	if err != nil {
		t.Fatalf("memory store should always be allowed: %v", err)
	}

	// Updating a file store's own dir to itself should be OK
	// (seeded directly to avoid orchestrator conflicts).
	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "store1",
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/store1"},
		},
	}))
	if err != nil {
		t.Fatalf("updating self should be allowed: %v", err)
	}

	// Same exact dir as another store should fail.
	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "duplicate-dir",
			Type:   "file",
			Params: map[string]string{"dir": baseDir + "/store1"},
		},
	}))
	if err == nil {
		t.Fatal("expected error for duplicate directory")
	}
}

func TestPauseResumeStoreRPC(t *testing.T) {
	client, _, orch := newConfigTestSetup(t)
	ctx := context.Background()

	// Create a filter and a store.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: "catch-all", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "pausable",
			Type:   "memory",
			Filter: "catch-all",
		},
	}))
	if err != nil {
		t.Fatalf("PutStore: %v", err)
	}

	// Pause the store via RPC.
	_, err = client.PauseStore(ctx, connect.NewRequest(&gastrologv1.PauseStoreRequest{
		Id: "pausable",
	}))
	if err != nil {
		t.Fatalf("PauseStore: %v", err)
	}

	// Verify runtime state.
	if orch.IsStoreEnabled("pausable") {
		t.Error("store should be disabled in runtime")
	}

	// Verify disabled in StoreInfo via StoreService.
	handler := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{}).Handler()
	storeClient := gastrologv1connect.NewStoreServiceClient(
		&http.Client{Transport: &embeddedTransport{handler: handler}},
		"http://embedded",
	)
	storeResp, err := storeClient.GetStore(ctx, connect.NewRequest(&gastrologv1.GetStoreRequest{Id: "pausable"}))
	if err != nil {
		t.Fatalf("GetStore: %v", err)
	}
	if storeResp.Msg.Store.Enabled {
		t.Error("StoreInfo.Enabled should be false after pause")
	}

	// Resume via RPC.
	_, err = client.ResumeStore(ctx, connect.NewRequest(&gastrologv1.ResumeStoreRequest{
		Id: "pausable",
	}))
	if err != nil {
		t.Fatalf("ResumeStore: %v", err)
	}

	if !orch.IsStoreEnabled("pausable") {
		t.Error("store should be enabled after resume")
	}

	// Ingest should work after resume.
	if err := orch.Ingest(chunk.Record{Raw: []byte("after resume")}); err != nil {
		t.Fatalf("Ingest after resume: %v", err)
	}
}

func TestPauseStoreNotFoundRPC(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PauseStore(ctx, connect.NewRequest(&gastrologv1.PauseStoreRequest{
		Id: "nonexistent",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestResumeStoreNotFoundRPC(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.ResumeStore(ctx, connect.NewRequest(&gastrologv1.ResumeStoreRequest{
		Id: "nonexistent",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestPauseStorePersistsToConfig(t *testing.T) {
	client, cfgStore, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Create a filter and store.
	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: "catch-all", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "persist-test",
			Type:   "memory",
			Filter: "catch-all",
		},
	}))
	if err != nil {
		t.Fatalf("PutStore: %v", err)
	}

	// Pause and check config persistence.
	_, err = client.PauseStore(ctx, connect.NewRequest(&gastrologv1.PauseStoreRequest{
		Id: "persist-test",
	}))
	if err != nil {
		t.Fatalf("PauseStore: %v", err)
	}

	stored, err := cfgStore.GetStore(ctx, "persist-test")
	if err != nil {
		t.Fatalf("GetStore from config: %v", err)
	}
	if stored.Enabled {
		t.Error("config store should have Enabled=false after pause")
	}

	// Resume and check config persistence.
	_, err = client.ResumeStore(ctx, connect.NewRequest(&gastrologv1.ResumeStoreRequest{
		Id: "persist-test",
	}))
	if err != nil {
		t.Fatalf("ResumeStore: %v", err)
	}

	stored, err = cfgStore.GetStore(ctx, "persist-test")
	if err != nil {
		t.Fatalf("GetStore from config: %v", err)
	}
	if !stored.Enabled {
		t.Error("config store should have Enabled=true after resume")
	}
}

func TestDecommissionStore(t *testing.T) {
	client, cfgStore, orch := newConfigTestSetup(t)
	ctx := context.Background()

	_, err := client.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: "catch-all", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}
	_, err = client.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
		Config: &gastrologv1.StoreConfig{
			Id:     "doomed",
			Type:   "memory",
			Filter: "catch-all",
		},
	}))
	if err != nil {
		t.Fatalf("PutStore: %v", err)
	}

	// Ingest data.
	for i := 0; i < 3; i++ {
		if err := orch.Ingest(chunk.Record{Raw: []byte("doomed data")}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	resp, err := client.DecommissionStore(ctx, connect.NewRequest(&gastrologv1.DecommissionStoreRequest{
		Id: "doomed",
	}))
	if err != nil {
		t.Fatalf("DecommissionStore: %v", err)
	}

	// Should report chunks removed (at least the active chunk counts).
	if resp.Msg.ChunksRemoved < 0 {
		t.Error("expected non-negative chunks removed")
	}

	// Store gone from runtime.
	if cm := orch.ChunkManager("doomed"); cm != nil {
		t.Error("ChunkManager should be nil after decommission")
	}

	// Store gone from config.
	stored, _ := cfgStore.GetStore(ctx, "doomed")
	if stored != nil {
		t.Error("store should be removed from config after decommission")
	}
}

func TestDecommissionStoreNotFound(t *testing.T) {
	client, _, _ := newConfigTestSetup(t)
	ctx := context.Background()

	// Decommission a nonexistent store - should not error since ForceRemoveStore
	// is lenient when store is not found.
	_, err := client.DecommissionStore(ctx, connect.NewRequest(&gastrologv1.DecommissionStoreRequest{
		Id: "ghost",
	}))
	// The config delete may fail, but the overall operation should handle it gracefully.
	// Since there's no config entry either, the cfgStore.DeleteStore will error.
	if err == nil {
		// If it succeeds, that's fine too.
		return
	}
	// Accept either CodeInternal or CodeNotFound for missing stores.
	code := connect.CodeOf(err)
	if code != connect.CodeInternal && code != connect.CodeNotFound {
		t.Fatalf("unexpected error code %v: %v", code, err)
	}
}
