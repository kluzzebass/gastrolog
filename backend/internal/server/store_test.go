package server_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	memattr "gastrolog/internal/index/memory/attr"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
)

// newStoreTestSetup creates an orchestrator with a memory store containing test data,
// and returns a StoreService client.
func newStoreTestSetup(t *testing.T, recordCount int) gastrologv1connect.StoreServiceClient {
	t.Helper()

	orch := orchestrator.New(orchestrator.Config{})

	cm, _ := chunkmem.NewManager(chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(5), // Seal every 5 records.
	})
	tokIdx := memtoken.NewIndexer(cm)
	attrIdx := memattr.NewIndexer(cm)
	kvIdx := memkv.NewIndexer(cm)
	im := indexmem.NewManager([]index.Indexer{tokIdx, attrIdx, kvIdx}, tokIdx, attrIdx, kvIdx, nil)

	t0 := time.Now()
	for i := 0; i < recordCount; i++ {
		cm.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"env": "test"},
			Raw:      []byte("test-record"),
		})
	}

	// Build indexes for sealed chunks.
	metas, _ := cm.List()
	for _, meta := range metas {
		if meta.Sealed {
			im.BuildIndexes(context.Background(), meta.ID)
		}
	}

	orch.RegisterChunkManager("default", cm)
	orch.RegisterIndexManager("default", im)
	orch.RegisterQueryEngine("default", query.New(cm, im, nil))

	// Set filter so orchestrator knows about the store.
	filter, _ := orchestrator.CompileFilter("default", "*")
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{filter}))

	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	return gastrologv1connect.NewStoreServiceClient(httpClient, "http://embedded")
}

func TestReindexStore(t *testing.T) {
	client := newStoreTestSetup(t, 12) // 12 records = 2 sealed (5 each) + 1 active (2)
	ctx := context.Background()

	resp, err := client.ReindexStore(ctx, connect.NewRequest(&gastrologv1.ReindexStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("ReindexStore: %v", err)
	}

	if resp.Msg.ChunksReindexed != 2 {
		t.Errorf("expected 2 chunks reindexed, got %d", resp.Msg.ChunksReindexed)
	}
	if resp.Msg.Errors != 0 {
		t.Errorf("expected 0 errors, got %d: %v", resp.Msg.Errors, resp.Msg.ErrorDetails)
	}
}

func TestReindexStoreNotFound(t *testing.T) {
	client := newStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := client.ReindexStore(ctx, connect.NewRequest(&gastrologv1.ReindexStoreRequest{
		Store: "nonexistent",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestReindexStoreEmpty(t *testing.T) {
	client := newStoreTestSetup(t, 0)
	ctx := context.Background()

	resp, err := client.ReindexStore(ctx, connect.NewRequest(&gastrologv1.ReindexStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("ReindexStore: %v", err)
	}

	if resp.Msg.ChunksReindexed != 0 {
		t.Errorf("expected 0 chunks reindexed for empty store, got %d", resp.Msg.ChunksReindexed)
	}
}

func TestValidateStore(t *testing.T) {
	client := newStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := client.ValidateStore(ctx, connect.NewRequest(&gastrologv1.ValidateStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("ValidateStore: %v", err)
	}

	if !resp.Msg.Valid {
		for _, cv := range resp.Msg.Chunks {
			if !cv.Valid {
				t.Errorf("chunk %s invalid: %v", cv.ChunkId, cv.Issues)
			}
		}
	}

	// Should have 3 chunks (2 sealed + 1 active).
	if len(resp.Msg.Chunks) != 3 {
		t.Errorf("expected 3 chunks, got %d", len(resp.Msg.Chunks))
	}
}

func TestValidateStoreNotFound(t *testing.T) {
	client := newStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := client.ValidateStore(ctx, connect.NewRequest(&gastrologv1.ValidateStoreRequest{
		Store: "nonexistent",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestGetStatsDetailed(t *testing.T) {
	client := newStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := client.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	if resp.Msg.TotalStores != 1 {
		t.Errorf("expected 1 store, got %d", resp.Msg.TotalStores)
	}
	if resp.Msg.TotalChunks != 3 {
		t.Errorf("expected 3 chunks, got %d", resp.Msg.TotalChunks)
	}
	if resp.Msg.SealedChunks != 2 {
		t.Errorf("expected 2 sealed chunks, got %d", resp.Msg.SealedChunks)
	}
	if resp.Msg.TotalRecords != 12 {
		t.Errorf("expected 12 records, got %d", resp.Msg.TotalRecords)
	}

	// Check per-store stats.
	if len(resp.Msg.StoreStats) != 1 {
		t.Fatalf("expected 1 store stat, got %d", len(resp.Msg.StoreStats))
	}

	ss := resp.Msg.StoreStats[0]
	if ss.Id != "default" {
		t.Errorf("expected store ID 'default', got %q", ss.Id)
	}
	if ss.ChunkCount != 3 {
		t.Errorf("store stat: expected 3 chunks, got %d", ss.ChunkCount)
	}
	if ss.SealedChunks != 2 {
		t.Errorf("store stat: expected 2 sealed, got %d", ss.SealedChunks)
	}
	if ss.ActiveChunks != 1 {
		t.Errorf("store stat: expected 1 active, got %d", ss.ActiveChunks)
	}
	if ss.RecordCount != 12 {
		t.Errorf("store stat: expected 12 records, got %d", ss.RecordCount)
	}
	if ss.DataBytes <= 0 {
		t.Errorf("store stat: expected positive data bytes, got %d", ss.DataBytes)
	}
	if ss.OldestRecord == nil {
		t.Error("store stat: expected oldest record timestamp")
	}
	if ss.NewestRecord == nil {
		t.Error("store stat: expected newest record timestamp")
	}
}

func TestGetStatsFilterByStore(t *testing.T) {
	client := newStoreTestSetup(t, 5)
	ctx := context.Background()

	// Filter to a specific store.
	resp, err := client.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	if resp.Msg.TotalStores != 1 {
		t.Errorf("expected 1 store, got %d", resp.Msg.TotalStores)
	}
	if len(resp.Msg.StoreStats) != 1 {
		t.Errorf("expected 1 store stat, got %d", len(resp.Msg.StoreStats))
	}
}

// newFullStoreTestSetup creates a store test setup with cfgStore and factories,
// needed for clone/migrate/export/import tests.
func newFullStoreTestSetup(t *testing.T, recordCount int) (gastrologv1connect.StoreServiceClient, config.Store) {
	t.Helper()

	orch := orchestrator.New(orchestrator.Config{})
	cfgStore := cfgmem.NewStore()

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	// Create default store via config + orchestrator.
	storeCfg := config.StoreConfig{
		ID:   "default",
		Type: "memory",
	}
	cfgStore.PutStore(context.Background(), storeCfg)

	fullCfg, _ := cfgStore.Load(context.Background())
	if err := orch.AddStore(storeCfg, fullCfg, factories); err != nil {
		t.Fatalf("AddStore: %v", err)
	}

	// Ingest test data.
	cm := orch.ChunkManager("default")
	t0 := time.Now()
	for i := 0; i < recordCount; i++ {
		cm.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"env": "test", "idx": string(rune('0' + i%10))},
			Raw:      []byte("test-record-" + string(rune('0'+i%10))),
		})
	}

	// Build indexes for sealed chunks.
	im := orch.IndexManager("default")
	metas, _ := cm.List()
	for _, meta := range metas {
		if meta.Sealed {
			im.BuildIndexes(context.Background(), meta.ID)
		}
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	return gastrologv1connect.NewStoreServiceClient(httpClient, "http://embedded"), cfgStore
}

func TestCloneStore(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := client.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
		Source:      "default",
		Destination: "clone1",
	}))
	if err != nil {
		t.Fatalf("CloneStore: %v", err)
	}

	if resp.Msg.RecordsCopied != 12 {
		t.Errorf("expected 12 records copied, got %d", resp.Msg.RecordsCopied)
	}
	if resp.Msg.ChunksCreated < 1 {
		t.Errorf("expected at least 1 chunk created, got %d", resp.Msg.ChunksCreated)
	}

	// Verify the cloned store has the same records.
	stats, err := client.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Store: "clone1",
	}))
	if err != nil {
		t.Fatalf("GetStats for clone: %v", err)
	}
	if stats.Msg.TotalRecords != 12 {
		t.Errorf("clone should have 12 records, got %d", stats.Msg.TotalRecords)
	}
}

func TestCloneStoreNotFound(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := client.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
		Source:      "nonexistent",
		Destination: "clone1",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestCloneStoreSameName(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := client.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
		Source:      "default",
		Destination: "default",
	}))
	if err == nil {
		t.Fatal("expected error for same source and destination")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestMigrateStore(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := client.MigrateStore(ctx, connect.NewRequest(&gastrologv1.MigrateStoreRequest{
		Source:          "default",
		Destination:     "migrated",
		DestinationType: "memory",
	}))
	if err != nil {
		t.Fatalf("MigrateStore: %v", err)
	}

	if resp.Msg.RecordsMigrated != 12 {
		t.Errorf("expected 12 records migrated, got %d", resp.Msg.RecordsMigrated)
	}

	// Source should be gone.
	_, err = client.GetStore(ctx, connect.NewRequest(&gastrologv1.GetStoreRequest{
		Id: "default",
	}))
	if err == nil {
		t.Error("expected source store to be deleted after migration")
	}

	// Destination should have the records.
	stats, err := client.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Store: "migrated",
	}))
	if err != nil {
		t.Fatalf("GetStats for migrated: %v", err)
	}
	if stats.Msg.TotalRecords != 12 {
		t.Errorf("migrated store should have 12 records, got %d", stats.Msg.TotalRecords)
	}
}

func TestMigrateStoreNotFound(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := client.MigrateStore(ctx, connect.NewRequest(&gastrologv1.MigrateStoreRequest{
		Source:          "nonexistent",
		Destination:     "dest",
		DestinationType: "memory",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestExportStore(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	stream, err := client.ExportStore(ctx, connect.NewRequest(&gastrologv1.ExportStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("ExportStore: %v", err)
	}

	var totalRecords int
	for {
		ok := stream.Receive()
		if !ok {
			break
		}
		msg := stream.Msg()
		totalRecords += len(msg.Records)

		// Verify each record has data.
		for _, rec := range msg.Records {
			if len(rec.Raw) == 0 {
				t.Error("exported record has empty raw data")
			}
		}

		if !msg.HasMore {
			break
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}

	if totalRecords != 12 {
		t.Errorf("expected 12 exported records, got %d", totalRecords)
	}
}

func TestExportStoreNotFound(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	stream, err := client.ExportStore(ctx, connect.NewRequest(&gastrologv1.ExportStoreRequest{
		Store: "nonexistent",
	}))
	if err != nil {
		t.Fatalf("ExportStore call: %v", err)
	}
	// Should get error on first receive.
	if stream.Receive() {
		t.Fatal("expected no messages for nonexistent store")
	}
	if stream.Err() == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(stream.Err()) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(stream.Err()))
	}
}

func TestImportRecords(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 0) // Empty store.
	ctx := context.Background()

	now := time.Now()
	records := make([]*gastrologv1.ExportRecord, 10)
	for i := range records {
		records[i] = &gastrologv1.ExportRecord{
			Raw:   []byte("imported-record"),
			Attrs: map[string]string{"source": "import"},
		}
		_ = now // timestamps optional
	}

	resp, err := client.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Store:   "default",
		Records: records,
	}))
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}

	if resp.Msg.RecordsImported != 10 {
		t.Errorf("expected 10 records imported, got %d", resp.Msg.RecordsImported)
	}

	// Verify records exist in the store.
	stats, err := client.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Msg.TotalRecords != 10 {
		t.Errorf("expected 10 records in store, got %d", stats.Msg.TotalRecords)
	}
}

func TestImportRecordsStoreNotFound(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := client.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Store:   "nonexistent",
		Records: []*gastrologv1.ExportRecord{{Raw: []byte("test")}},
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	client, _ := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	// Export from default store.
	stream, err := client.ExportStore(ctx, connect.NewRequest(&gastrologv1.ExportStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("ExportStore: %v", err)
	}

	var allRecords []*gastrologv1.ExportRecord
	for stream.Receive() {
		msg := stream.Msg()
		allRecords = append(allRecords, msg.Records...)
		if !msg.HasMore {
			break
		}
	}
	if err := stream.Err(); err != nil && err != io.EOF {
		t.Fatalf("stream error: %v", err)
	}

	if len(allRecords) != 12 {
		t.Fatalf("expected 12 exported records, got %d", len(allRecords))
	}

	// Clone a new empty store to import into.
	_, err = client.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
		Source:      "default",
		Destination: "import-target",
	}))
	// Clone will copy records, but we want to test import specifically.
	// Create a fresh empty store instead using import to nonexistent-like scenario.
	// Actually, let's just import into the existing default store as additional records.
	resp, err := client.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Store:   "default",
		Records: allRecords,
	}))
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}

	if resp.Msg.RecordsImported != 12 {
		t.Errorf("expected 12 records imported, got %d", resp.Msg.RecordsImported)
	}

	// Default store should now have 24 records (12 original + 12 imported).
	stats, err := client.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Msg.TotalRecords != 24 {
		t.Errorf("expected 24 records after round-trip, got %d", stats.Msg.TotalRecords)
	}
}

func TestCompactStoreMemory(t *testing.T) {
	client := newStoreTestSetup(t, 12)
	ctx := context.Background()

	// Compact on a memory store should succeed but remove nothing.
	resp, err := client.CompactStore(ctx, connect.NewRequest(&gastrologv1.CompactStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("CompactStore: %v", err)
	}
	if resp.Msg.ChunksRemoved != 0 {
		t.Errorf("expected 0 chunks removed for memory store, got %d", resp.Msg.ChunksRemoved)
	}
}

func TestCompactStoreNotFound(t *testing.T) {
	client := newStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := client.CompactStore(ctx, connect.NewRequest(&gastrologv1.CompactStoreRequest{
		Store: "nonexistent",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent store")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

// newTwoStoreTestSetup creates an orchestrator with two memory stores for merge testing.
func newTwoStoreTestSetup(t *testing.T) (gastrologv1connect.StoreServiceClient, *orchestrator.Orchestrator) {
	t.Helper()

	orch := orchestrator.New(orchestrator.Config{})

	cfgStore := cfgmem.NewStore()

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}

	// Use config client to create stores.
	cfgClient := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")
	ctx := context.Background()

	_, err := cfgClient.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Id:     "catch-all",
		Config: &gastrologv1.FilterConfig{Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	for _, id := range []string{"src", "dst"} {
		_, err := cfgClient.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
			Config: &gastrologv1.StoreConfig{
				Id:     id,
				Type:   "memory",
				Filter: "catch-all",
			},
		}))
		if err != nil {
			t.Fatalf("PutStore(%s): %v", id, err)
		}
	}

	// Ingest data into src.
	t0 := time.Now()
	for i := 0; i < 5; i++ {
		if err := orch.Ingest(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("merge-record"),
		}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	storeClient := gastrologv1connect.NewStoreServiceClient(httpClient, "http://embedded")
	return storeClient, orch
}

func TestMergeStores(t *testing.T) {
	client, orch := newTwoStoreTestSetup(t)
	ctx := context.Background()

	// Both stores should exist.
	srcCM := orch.ChunkManager("src")
	if srcCM == nil {
		t.Fatal("src chunk manager should exist")
	}

	resp, err := client.MergeStores(ctx, connect.NewRequest(&gastrologv1.MergeStoresRequest{
		Source:      "src",
		Destination: "dst",
	}))
	if err != nil {
		t.Fatalf("MergeStores: %v", err)
	}

	if resp.Msg.RecordsMerged <= 0 {
		t.Errorf("expected records merged > 0, got %d", resp.Msg.RecordsMerged)
	}

	// Source should be gone.
	if cm := orch.ChunkManager("src"); cm != nil {
		t.Error("source chunk manager should be nil after merge")
	}

	// Destination should have the merged records.
	dstCM := orch.ChunkManager("dst")
	if dstCM == nil {
		t.Fatal("dst chunk manager should still exist")
	}
}

func TestMergeStoresNotFound(t *testing.T) {
	client := newStoreTestSetup(t, 5)
	ctx := context.Background()

	_, err := client.MergeStores(ctx, connect.NewRequest(&gastrologv1.MergeStoresRequest{
		Source:      "nonexistent",
		Destination: "default",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestMergeStoresSameStore(t *testing.T) {
	client := newStoreTestSetup(t, 5)
	ctx := context.Background()

	_, err := client.MergeStores(ctx, connect.NewRequest(&gastrologv1.MergeStoresRequest{
		Source:      "default",
		Destination: "default",
	}))
	if err == nil {
		t.Fatal("expected error when source == destination")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}
