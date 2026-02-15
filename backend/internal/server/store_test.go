package server_test

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

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
	memattr "gastrolog/internal/index/memory/attr"
	memkv "gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/query"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
)

// waitForJob polls the JobService until the job completes or fails, returning the final job state.
func waitForJob(t *testing.T, jobClient gastrologv1connect.JobServiceClient, jobID string) *gastrologv1.Job {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := jobClient.GetJob(ctx, connect.NewRequest(&gastrologv1.GetJobRequest{Id: jobID}))
		if err != nil {
			t.Fatalf("GetJob(%s): %v", jobID, err)
		}
		switch resp.Msg.Job.Status {
		case gastrologv1.JobStatus_JOB_STATUS_COMPLETED, gastrologv1.JobStatus_JOB_STATUS_FAILED:
			return resp.Msg.Job
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("job %s did not complete within deadline", jobID)
	return nil
}

// newStoreTestSetup creates an orchestrator with a memory store containing test data,
// and returns a StoreService client.
type storeTestClients struct {
	store gastrologv1connect.StoreServiceClient
	job   gastrologv1connect.JobServiceClient
}

func newStoreTestSetup(t *testing.T, recordCount int) storeTestClients {
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
	return storeTestClients{
		store: gastrologv1connect.NewStoreServiceClient(httpClient, "http://embedded"),
		job:   gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded"),
	}
}

func TestReindexStore(t *testing.T) {
	clients := newStoreTestSetup(t, 12) // 12 records = 2 sealed (5 each) + 1 active (2)
	ctx := context.Background()

	resp, err := clients.store.ReindexStore(ctx, connect.NewRequest(&gastrologv1.ReindexStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("ReindexStore: %v", err)
	}

	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	job := waitForJob(t, clients.job, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s)", job.Status, job.Error)
	}
	if job.ChunksDone != 2 {
		t.Errorf("expected 2 chunks done, got %d", job.ChunksDone)
	}
	if len(job.ErrorDetails) != 0 {
		t.Errorf("expected 0 error details, got %v", job.ErrorDetails)
	}
}

func TestReindexStoreNotFound(t *testing.T) {
	clients := newStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := clients.store.ReindexStore(ctx, connect.NewRequest(&gastrologv1.ReindexStoreRequest{
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
	clients := newStoreTestSetup(t, 0)
	ctx := context.Background()

	resp, err := clients.store.ReindexStore(ctx, connect.NewRequest(&gastrologv1.ReindexStoreRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("ReindexStore: %v", err)
	}

	job := waitForJob(t, clients.job, resp.Msg.JobId)
	if job.ChunksDone != 0 {
		t.Errorf("expected 0 chunks done for empty store, got %d", job.ChunksDone)
	}
}

func TestValidateStore(t *testing.T) {
	clients := newStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := clients.store.ValidateStore(ctx, connect.NewRequest(&gastrologv1.ValidateStoreRequest{
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
	clients := newStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := clients.store.ValidateStore(ctx, connect.NewRequest(&gastrologv1.ValidateStoreRequest{
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
	clients := newStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := clients.store.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{}))
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
	clients := newStoreTestSetup(t, 5)
	ctx := context.Background()

	// Filter to a specific store.
	resp, err := clients.store.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
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
type fullStoreTestClients struct {
	store    gastrologv1connect.StoreServiceClient
	job      gastrologv1connect.JobServiceClient
	cfgStore config.Store
}

func newFullStoreTestSetup(t *testing.T, recordCount int) fullStoreTestClients {
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
	return fullStoreTestClients{
		store:    gastrologv1connect.NewStoreServiceClient(httpClient, "http://embedded"),
		job:      gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded"),
		cfgStore: cfgStore,
	}
}

func TestCloneStore(t *testing.T) {
	tc := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := tc.store.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
		Source:      "default",
		Destination: "clone1",
	}))
	if err != nil {
		t.Fatalf("CloneStore: %v", err)
	}

	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	job := waitForJob(t, tc.job, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s)", job.Status, job.Error)
	}

	// Verify the cloned store has the same records.
	stats, err := tc.store.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
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
	tc := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := tc.store.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
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
	tc := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := tc.store.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
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
	tc := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	resp, err := tc.store.MigrateStore(ctx, connect.NewRequest(&gastrologv1.MigrateStoreRequest{
		Source:          "default",
		Destination:     "migrated",
		DestinationType: "memory",
	}))
	if err != nil {
		t.Fatalf("MigrateStore: %v", err)
	}

	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	job := waitForJob(t, tc.job, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s)", job.Status, job.Error)
	}

	// Source should be gone.
	_, err = tc.store.GetStore(ctx, connect.NewRequest(&gastrologv1.GetStoreRequest{
		Id: "default",
	}))
	if err == nil {
		t.Error("expected source store to be deleted after migration")
	}

	// Destination should have the records.
	stats, err := tc.store.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
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
	tc := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := tc.store.MigrateStore(ctx, connect.NewRequest(&gastrologv1.MigrateStoreRequest{
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
	tc := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	stream, err := tc.store.ExportStore(ctx, connect.NewRequest(&gastrologv1.ExportStoreRequest{
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
	tc := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	stream, err := tc.store.ExportStore(ctx, connect.NewRequest(&gastrologv1.ExportStoreRequest{
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
	tc := newFullStoreTestSetup(t, 0) // Empty store.
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

	resp, err := tc.store.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
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
	stats, err := tc.store.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
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
	tc := newFullStoreTestSetup(t, 0)
	ctx := context.Background()

	_, err := tc.store.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
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
	tc := newFullStoreTestSetup(t, 12)
	ctx := context.Background()

	// Export from default store.
	stream, err := tc.store.ExportStore(ctx, connect.NewRequest(&gastrologv1.ExportStoreRequest{
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
	_, err = tc.store.CloneStore(ctx, connect.NewRequest(&gastrologv1.CloneStoreRequest{
		Source:      "default",
		Destination: "import-target",
	}))
	// Clone will copy records, but we want to test import specifically.
	// Create a fresh empty store instead using import to nonexistent-like scenario.
	// Actually, let's just import into the existing default store as additional records.
	resp, err := tc.store.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
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
	stats, err := tc.store.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Store: "default",
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Msg.TotalRecords != 24 {
		t.Errorf("expected 24 records after round-trip, got %d", stats.Msg.TotalRecords)
	}
}

// twoStoreTestClients holds clients and orchestrator for two-store merge tests.
type twoStoreTestClients struct {
	store gastrologv1connect.StoreServiceClient
	job   gastrologv1connect.JobServiceClient
	orch  *orchestrator.Orchestrator
}

// newTwoStoreTestSetup creates an orchestrator with two memory stores for merge testing.
func newTwoStoreTestSetup(t *testing.T) twoStoreTestClients {
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
		Config: &gastrologv1.FilterConfig{Id: "catch-all", Expression: "*"},
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

	return twoStoreTestClients{
		store: gastrologv1connect.NewStoreServiceClient(httpClient, "http://embedded"),
		job:   gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded"),
		orch:  orch,
	}
}

func TestMergeStoresMemory(t *testing.T) {
	tc := newTwoStoreTestSetup(t)
	ctx := context.Background()

	// Memory-backed stores fall back to record-by-record copy.
	resp, err := tc.store.MergeStores(ctx, connect.NewRequest(&gastrologv1.MergeStoresRequest{
		Source:      "src",
		Destination: "dst",
	}))
	if err != nil {
		t.Fatalf("MergeStores: %v", err)
	}
	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	job := waitForJob(t, tc.job, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s)", job.Status, job.Error)
	}

	// Source should be gone.
	if cm := tc.orch.ChunkManager("src"); cm != nil {
		t.Error("source chunk manager should be nil after merge")
	}

	// Destination should have the merged records.
	dstCM := tc.orch.ChunkManager("dst")
	if dstCM == nil {
		t.Fatal("dst chunk manager should still exist")
	}
}

func TestMergeStoresFileBacked(t *testing.T) {
	orch := orchestrator.New(orchestrator.Config{})
	cfgStore := cfgmem.NewStore()
	dataDir := t.TempDir()

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file": chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file": indexfile.NewFactory(),
		},
		DataDir: dataDir,
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{})
	handler := srv.Handler()
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}

	cfgClient := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")
	storeClient := gastrologv1connect.NewStoreServiceClient(httpClient, "http://embedded")
	jobClient := gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded")
	ctx := context.Background()

	_, err := cfgClient.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: "catch-all", Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	for _, id := range []string{"src", "dst"} {
		storeDir := filepath.Join(dataDir, "stores", id)
		_, err := cfgClient.PutStore(ctx, connect.NewRequest(&gastrologv1.PutStoreRequest{
			Config: &gastrologv1.StoreConfig{
				Id:     id,
				Type:   "file",
				Filter: "catch-all",
				Params: map[string]string{"dir": storeDir},
			},
		}))
		if err != nil {
			t.Fatalf("PutStore(%s): %v", id, err)
		}
	}

	// Ingest records into src.
	srcCM := orch.ChunkManager("src")
	if srcCM == nil {
		t.Fatal("src chunk manager should exist")
	}

	t0 := time.Now()
	for i := range 10 {
		_, _, err := srcCM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"env": "test"},
			Raw:      []byte("merge-record"),
		})
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	// Seal to ensure we have sealed chunks.
	if err := srcCM.Seal(); err != nil {
		t.Fatalf("Seal: %v", err)
	}

	// Read WriteTS from source records before merge (for verification).
	srcMetas, _ := srcCM.List()
	var originalWriteTSs []time.Time
	for _, meta := range srcMetas {
		cursor, err := srcCM.OpenCursor(meta.ID)
		if err != nil {
			t.Fatalf("open cursor: %v", err)
		}
		for {
			rec, _, err := cursor.Next()
			if err != nil {
				break
			}
			originalWriteTSs = append(originalWriteTSs, rec.WriteTS)
		}
		cursor.Close()
	}
	if len(originalWriteTSs) != 10 {
		t.Fatalf("expected 10 records in src, got %d", len(originalWriteTSs))
	}

	resp, err := storeClient.MergeStores(ctx, connect.NewRequest(&gastrologv1.MergeStoresRequest{
		Source:      "src",
		Destination: "dst",
	}))
	if err != nil {
		t.Fatalf("MergeStores: %v", err)
	}
	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	job := waitForJob(t, jobClient, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s, details: %v)", job.Status, job.Error, job.ErrorDetails)
	}

	// Source should be gone.
	if cm := orch.ChunkManager("src"); cm != nil {
		t.Error("source chunk manager should be nil after merge")
	}

	// Destination should have all records with preserved WriteTS.
	dstCM := orch.ChunkManager("dst")
	if dstCM == nil {
		t.Fatal("dst chunk manager should still exist")
	}

	dstMetas, _ := dstCM.List()
	var mergedWriteTSs []time.Time
	for _, meta := range dstMetas {
		cursor, err := dstCM.OpenCursor(meta.ID)
		if err != nil {
			t.Fatalf("open dst cursor: %v", err)
		}
		for {
			rec, _, err := cursor.Next()
			if err != nil {
				break
			}
			mergedWriteTSs = append(mergedWriteTSs, rec.WriteTS)
		}
		cursor.Close()
	}

	if len(mergedWriteTSs) != 10 {
		t.Fatalf("expected 10 merged records, got %d", len(mergedWriteTSs))
	}

	// WriteTS should be preserved (not rewritten).
	for i, orig := range originalWriteTSs {
		if !orig.Equal(mergedWriteTSs[i]) {
			t.Errorf("record %d: WriteTS changed from %v to %v", i, orig, mergedWriteTSs[i])
		}
	}
}

func TestMergeStoresNotFound(t *testing.T) {
	clients := newStoreTestSetup(t, 5)
	ctx := context.Background()

	_, err := clients.store.MergeStores(ctx, connect.NewRequest(&gastrologv1.MergeStoresRequest{
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
	clients := newStoreTestSetup(t, 5)
	ctx := context.Background()

	_, err := clients.store.MergeStores(ctx, connect.NewRequest(&gastrologv1.MergeStoresRequest{
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
