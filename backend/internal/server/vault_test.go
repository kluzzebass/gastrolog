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
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
	"github.com/google/uuid"
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

// newVaultTestSetup creates an orchestrator with a memory vault containing test data,
// and returns a VaultService client.
type vaultTestClients struct {
	vault     gastrologv1connect.VaultServiceClient
	job       gastrologv1connect.JobServiceClient
	defaultID uuid.UUID
}

func newVaultTestSetup(t *testing.T, recordCount int) vaultTestClients {
	t.Helper()

	orch := orchestrator.New(orchestrator.Config{})
	defaultID := uuid.Must(uuid.NewV7())

	s := memtest.MustNewVault(t, chunkmem.Config{
		RotationPolicy: chunk.NewRecordCountPolicy(5), // Seal every 5 records.
	})

	t0 := time.Now()
	for i := range recordCount {
		s.CM.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"env": "test"},
			Raw:      []byte("test-record"),
		})
	}

	memtest.BuildIndexes(t, s.CM, s.IM)

	orch.RegisterVault(orchestrator.NewVault(defaultID, s.CM, s.IM, s.QE))

	// Set filter so orchestrator knows about the vault.
	filter, _ := orchestrator.CompileFilter(defaultID, "*")
	orch.SetFilterSet(orchestrator.NewFilterSet([]*orchestrator.CompiledFilter{filter}))

	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}
	return vaultTestClients{
		vault:     gastrologv1connect.NewVaultServiceClient(httpClient, "http://embedded"),
		job:       gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded"),
		defaultID: defaultID,
	}
}

func TestReindexVault(t *testing.T) {
	clients := newVaultTestSetup(t, 12) // 12 records = 2 sealed (5 each) + 1 active (2)
	ctx := context.Background()

	resp, err := clients.vault.ReindexVault(ctx, connect.NewRequest(&gastrologv1.ReindexVaultRequest{
		Vault: clients.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ReindexVault: %v", err)
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

func TestReindexVaultNotFound(t *testing.T) {
	clients := newVaultTestSetup(t, 0)
	ctx := context.Background()

	_, err := clients.vault.ReindexVault(ctx, connect.NewRequest(&gastrologv1.ReindexVaultRequest{
		Vault: uuid.Must(uuid.NewV7()).String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestReindexVaultEmpty(t *testing.T) {
	clients := newVaultTestSetup(t, 0)
	ctx := context.Background()

	resp, err := clients.vault.ReindexVault(ctx, connect.NewRequest(&gastrologv1.ReindexVaultRequest{
		Vault: clients.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ReindexVault: %v", err)
	}

	job := waitForJob(t, clients.job, resp.Msg.JobId)
	if job.ChunksDone != 0 {
		t.Errorf("expected 0 chunks done for empty vault, got %d", job.ChunksDone)
	}
}

func TestValidateVault(t *testing.T) {
	clients := newVaultTestSetup(t, 12)
	ctx := context.Background()

	resp, err := clients.vault.ValidateVault(ctx, connect.NewRequest(&gastrologv1.ValidateVaultRequest{
		Vault: clients.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ValidateVault: %v", err)
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

func TestValidateVaultNotFound(t *testing.T) {
	clients := newVaultTestSetup(t, 0)
	ctx := context.Background()

	_, err := clients.vault.ValidateVault(ctx, connect.NewRequest(&gastrologv1.ValidateVaultRequest{
		Vault: uuid.Must(uuid.NewV7()).String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestGetStatsDetailed(t *testing.T) {
	clients := newVaultTestSetup(t, 12)
	ctx := context.Background()

	resp, err := clients.vault.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	if resp.Msg.TotalVaults != 1 {
		t.Errorf("expected 1 vault, got %d", resp.Msg.TotalVaults)
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

	// Check per-vault stats.
	if len(resp.Msg.VaultStats) != 1 {
		t.Fatalf("expected 1 vault stat, got %d", len(resp.Msg.VaultStats))
	}

	vs := resp.Msg.VaultStats[0]
	if vs.Id != clients.defaultID.String() {
		t.Errorf("expected vault ID %q, got %q", clients.defaultID.String(), vs.Id)
	}
	if vs.ChunkCount != 3 {
		t.Errorf("vault stat: expected 3 chunks, got %d", vs.ChunkCount)
	}
	if vs.SealedChunks != 2 {
		t.Errorf("vault stat: expected 2 sealed, got %d", vs.SealedChunks)
	}
	if vs.ActiveChunks != 1 {
		t.Errorf("vault stat: expected 1 active, got %d", vs.ActiveChunks)
	}
	if vs.RecordCount != 12 {
		t.Errorf("vault stat: expected 12 records, got %d", vs.RecordCount)
	}
	if vs.DataBytes <= 0 {
		t.Errorf("vault stat: expected positive data bytes, got %d", vs.DataBytes)
	}
	if vs.OldestRecord == nil {
		t.Error("vault stat: expected oldest record timestamp")
	}
	if vs.NewestRecord == nil {
		t.Error("vault stat: expected newest record timestamp")
	}
}

func TestGetStatsFilterByVault(t *testing.T) {
	clients := newVaultTestSetup(t, 5)
	ctx := context.Background()

	// Filter to a specific vault.
	resp, err := clients.vault.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Vault: clients.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	if resp.Msg.TotalVaults != 1 {
		t.Errorf("expected 1 vault, got %d", resp.Msg.TotalVaults)
	}
	if len(resp.Msg.VaultStats) != 1 {
		t.Errorf("expected 1 vault stat, got %d", len(resp.Msg.VaultStats))
	}
}

// newFullVaultTestSetup creates a vault test setup with cfgStore and factories,
// needed for clone/migrate/export/import tests.
type fullVaultTestClients struct {
	vault     gastrologv1connect.VaultServiceClient
	job       gastrologv1connect.JobServiceClient
	cfgStore  config.Store
	defaultID uuid.UUID
}

func newFullVaultTestSetup(t *testing.T, recordCount int) fullVaultTestClients {
	t.Helper()

	cfgStore := cfgmem.NewStore()
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore})
	defaultID := uuid.Must(uuid.NewV7())

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	// Create default vault via config + orchestrator.
	vaultCfg := config.VaultConfig{
		ID:   defaultID,
		Type: "memory",
	}
	cfgStore.PutVault(context.Background(), vaultCfg)

	if err := orch.AddVault(context.Background(), vaultCfg, factories); err != nil {
		t.Fatalf("AddVault: %v", err)
	}

	// Ingest test data.
	cm := orch.ChunkManager(defaultID)
	t0 := time.Now()
	for i := range recordCount {
		cm.Append(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Attrs:    chunk.Attributes{"env": "test", "idx": string(rune('0' + i%10))},
			Raw:      []byte("test-record-" + string(rune('0'+i%10))),
		})
	}

	// Build indexes for sealed chunks.
	im := orch.IndexManager(defaultID)
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
	return fullVaultTestClients{
		vault:     gastrologv1connect.NewVaultServiceClient(httpClient, "http://embedded"),
		job:       gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded"),
		cfgStore:  cfgStore,
		defaultID: defaultID,
	}
}

func TestMigrateVault(t *testing.T) {
	tc := newFullVaultTestSetup(t, 12)
	ctx := context.Background()

	// No DestinationType — should default to same as source ("memory").
	resp, err := tc.vault.MigrateVault(ctx, connect.NewRequest(&gastrologv1.MigrateVaultRequest{
		Source:      tc.defaultID.String(),
		Destination: "migrated",
	}))
	if err != nil {
		t.Fatalf("MigrateVault: %v", err)
	}

	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	// Source should be disabled in config (sync phase persists this before returning).
	// Note: the async job may have already deleted the source, so check config directly.
	srcCfg, err := tc.cfgStore.GetVault(ctx, tc.defaultID)
	if err != nil {
		t.Fatalf("cfgStore.GetVault(%s): %v", tc.defaultID, err)
	}
	if srcCfg != nil && srcCfg.Enabled {
		t.Error("expected source config to have enabled=false")
	}

	job := waitForJob(t, tc.job, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s)", job.Status, job.Error)
	}

	// Source should be gone after job completes.
	_, err = tc.vault.GetVault(ctx, connect.NewRequest(&gastrologv1.GetVaultRequest{
		Id: tc.defaultID.String(),
	}))
	if err == nil {
		t.Error("expected source vault to be deleted after migration")
	}

	// Find destination vault by name (ID is a UUID now).
	listResp, err := tc.vault.ListVaults(ctx, connect.NewRequest(&gastrologv1.ListVaultsRequest{}))
	if err != nil {
		t.Fatalf("ListVaults: %v", err)
	}
	var dstID string
	for _, s := range listResp.Msg.Vaults {
		if s.Name == "migrated" {
			dstID = s.Id
			break
		}
	}
	if dstID == "" {
		t.Fatal("destination vault 'migrated' not found in ListVaults")
	}

	// Destination should have the records.
	stats, err := tc.vault.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Vault: dstID,
	}))
	if err != nil {
		t.Fatalf("GetStats for migrated: %v", err)
	}
	if stats.Msg.TotalRecords != 12 {
		t.Errorf("migrated vault should have 12 records, got %d", stats.Msg.TotalRecords)
	}
}

func TestMigrateVaultNotFound(t *testing.T) {
	tc := newFullVaultTestSetup(t, 0)
	ctx := context.Background()

	_, err := tc.vault.MigrateVault(ctx, connect.NewRequest(&gastrologv1.MigrateVaultRequest{
		Source:      uuid.Must(uuid.NewV7()).String(),
		Destination: "dest",
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestExportVault(t *testing.T) {
	tc := newFullVaultTestSetup(t, 12)
	ctx := context.Background()

	stream, err := tc.vault.ExportVault(ctx, connect.NewRequest(&gastrologv1.ExportVaultRequest{
		Vault: tc.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ExportVault: %v", err)
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

func TestExportVaultNotFound(t *testing.T) {
	tc := newFullVaultTestSetup(t, 0)
	ctx := context.Background()

	stream, err := tc.vault.ExportVault(ctx, connect.NewRequest(&gastrologv1.ExportVaultRequest{
		Vault: uuid.Must(uuid.NewV7()).String(),
	}))
	if err != nil {
		t.Fatalf("ExportVault call: %v", err)
	}
	// Should get error on first receive.
	if stream.Receive() {
		t.Fatal("expected no messages for nonexistent vault")
	}
	if stream.Err() == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(stream.Err()) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(stream.Err()))
	}
}

func TestImportRecords(t *testing.T) {
	tc := newFullVaultTestSetup(t, 0) // Empty vault.
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

	resp, err := tc.vault.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Vault:   tc.defaultID.String(),
		Records: records,
	}))
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}

	if resp.Msg.RecordsImported != 10 {
		t.Errorf("expected 10 records imported, got %d", resp.Msg.RecordsImported)
	}

	// Verify records exist in the vault.
	stats, err := tc.vault.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Vault: tc.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Msg.TotalRecords != 10 {
		t.Errorf("expected 10 records in vault, got %d", stats.Msg.TotalRecords)
	}
}

func TestImportRecordsVaultNotFound(t *testing.T) {
	tc := newFullVaultTestSetup(t, 0)
	ctx := context.Background()

	_, err := tc.vault.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Vault:   uuid.Must(uuid.NewV7()).String(),
		Records: []*gastrologv1.ExportRecord{{Raw: []byte("test")}},
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	tc := newFullVaultTestSetup(t, 12)
	ctx := context.Background()

	// Export from default vault.
	stream, err := tc.vault.ExportVault(ctx, connect.NewRequest(&gastrologv1.ExportVaultRequest{
		Vault: tc.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ExportVault: %v", err)
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

	// Import the exported records back into the same vault as additional records.
	resp, err := tc.vault.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Vault:   tc.defaultID.String(),
		Records: allRecords,
	}))
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}

	if resp.Msg.RecordsImported != 12 {
		t.Errorf("expected 12 records imported, got %d", resp.Msg.RecordsImported)
	}

	// Default vault should now have 24 records (12 original + 12 imported).
	stats, err := tc.vault.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Vault: tc.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Msg.TotalRecords != 24 {
		t.Errorf("expected 24 records after round-trip, got %d", stats.Msg.TotalRecords)
	}
}

// twoVaultTestClients holds clients and orchestrator for two-vault merge tests.
type twoVaultTestClients struct {
	vault gastrologv1connect.VaultServiceClient
	job   gastrologv1connect.JobServiceClient
	orch  *orchestrator.Orchestrator
	srcID uuid.UUID
	dstID uuid.UUID
}

// newTwoVaultTestSetup creates an orchestrator with two memory vaults for merge testing.
func newTwoVaultTestSetup(t *testing.T) twoVaultTestClients {
	t.Helper()

	cfgStore := cfgmem.NewStore()
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore})
	filterID := uuid.Must(uuid.NewV7())
	srcID := uuid.Must(uuid.NewV7())
	dstID := uuid.Must(uuid.NewV7())

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{
		AfterConfigApply: testAfterConfigApply(orch, cfgStore, factories),
	})
	handler := srv.Handler()

	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}

	// Use config client to create vaults.
	cfgClient := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")
	ctx := context.Background()

	_, err := cfgClient.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	for _, id := range []uuid.UUID{srcID, dstID} {
		_, err := cfgClient.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
			Config: &gastrologv1.VaultConfig{
				Id:   id.String(),
				Type: "memory",
			},
		}))
		if err != nil {
			t.Fatalf("PutVault(%s): %v", id, err)
		}
	}

	// Ingest data into src.
	t0 := time.Now()
	for i := range 5 {
		if err := orch.Ingest(chunk.Record{
			IngestTS: t0.Add(time.Duration(i) * time.Second),
			Raw:      []byte("merge-record"),
		}); err != nil {
			t.Fatalf("Ingest: %v", err)
		}
	}

	return twoVaultTestClients{
		vault: gastrologv1connect.NewVaultServiceClient(httpClient, "http://embedded"),
		job:   gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded"),
		orch:  orch,
		srcID: srcID,
		dstID: dstID,
	}
}

func TestMergeVaultsMemory(t *testing.T) {
	tc := newTwoVaultTestSetup(t)
	ctx := context.Background()

	// Memory-backed vaults fall back to record-by-record copy.
	// Source is auto-disabled by MergeVaults.
	resp, err := tc.vault.MergeVaults(ctx, connect.NewRequest(&gastrologv1.MergeVaultsRequest{
		Source:      tc.srcID.String(),
		Destination: tc.dstID.String(),
	}))
	if err != nil {
		t.Fatalf("MergeVaults: %v", err)
	}
	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	job := waitForJob(t, tc.job, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s)", job.Status, job.Error)
	}

	// Source should be gone.
	if cm := tc.orch.ChunkManager(tc.srcID); cm != nil {
		t.Error("source chunk manager should be nil after merge")
	}

	// Destination should have the merged records.
	dstCM := tc.orch.ChunkManager(tc.dstID)
	if dstCM == nil {
		t.Fatal("dst chunk manager should still exist")
	}
}

func TestMergeVaultsFileBacked(t *testing.T) {
	cfgStore := cfgmem.NewStore()
	orch := orchestrator.New(orchestrator.Config{ConfigLoader: cfgStore})
	homeDir := t.TempDir()

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"file": chunkfile.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"file": indexfile.NewFactory(),
		},
		HomeDir: homeDir,
	}

	srv := server.New(orch, cfgStore, factories, nil, server.Config{
		AfterConfigApply: testAfterConfigApply(orch, cfgStore, factories),
	})
	handler := srv.Handler()
	httpClient := &http.Client{
		Transport: &embeddedTransport{handler: handler},
	}

	cfgClient := gastrologv1connect.NewConfigServiceClient(httpClient, "http://embedded")
	vaultClient := gastrologv1connect.NewVaultServiceClient(httpClient, "http://embedded")
	jobClient := gastrologv1connect.NewJobServiceClient(httpClient, "http://embedded")
	ctx := context.Background()

	filterID := uuid.Must(uuid.NewV7())
	srcID := uuid.Must(uuid.NewV7())
	dstID := uuid.Must(uuid.NewV7())

	_, err := cfgClient.PutFilter(ctx, connect.NewRequest(&gastrologv1.PutFilterRequest{
		Config: &gastrologv1.FilterConfig{Id: filterID.String(), Expression: "*"},
	}))
	if err != nil {
		t.Fatalf("PutFilter: %v", err)
	}

	for _, id := range []uuid.UUID{srcID, dstID} {
		vaultDir := filepath.Join(homeDir, "vaults", id.String())
		_, err := cfgClient.PutVault(ctx, connect.NewRequest(&gastrologv1.PutVaultRequest{
			Config: &gastrologv1.VaultConfig{
				Id:     id.String(),
				Type:   "file",
				Params: map[string]string{"dir": vaultDir},
			},
		}))
		if err != nil {
			t.Fatalf("PutVault(%s): %v", id, err)
		}
	}

	// Ingest records into src.
	srcCM := orch.ChunkManager(srcID)
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

	// Source is auto-disabled by MergeVaults.
	resp, err := vaultClient.MergeVaults(ctx, connect.NewRequest(&gastrologv1.MergeVaultsRequest{
		Source:      srcID.String(),
		Destination: dstID.String(),
	}))
	if err != nil {
		t.Fatalf("MergeVaults: %v", err)
	}
	if resp.Msg.JobId == "" {
		t.Fatal("expected non-empty job_id")
	}

	job := waitForJob(t, jobClient, resp.Msg.JobId)
	if job.Status != gastrologv1.JobStatus_JOB_STATUS_COMPLETED {
		t.Errorf("expected completed, got %v (error: %s, details: %v)", job.Status, job.Error, job.ErrorDetails)
	}

	// Source should be gone.
	if cm := orch.ChunkManager(srcID); cm != nil {
		t.Error("source chunk manager should be nil after merge")
	}

	// Destination should have all records with preserved WriteTS.
	dstCM := orch.ChunkManager(dstID)
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

func TestMergeVaultsNotFound(t *testing.T) {
	clients := newVaultTestSetup(t, 5)
	ctx := context.Background()

	_, err := clients.vault.MergeVaults(ctx, connect.NewRequest(&gastrologv1.MergeVaultsRequest{
		Source:      uuid.Must(uuid.NewV7()).String(),
		Destination: clients.defaultID.String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestMergeVaultsSameVault(t *testing.T) {
	clients := newVaultTestSetup(t, 5)
	ctx := context.Background()

	_, err := clients.vault.MergeVaults(ctx, connect.NewRequest(&gastrologv1.MergeVaultsRequest{
		Source:      clients.defaultID.String(),
		Destination: clients.defaultID.String(),
	}))
	if err == nil {
		t.Fatal("expected error when source == destination")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestMigrateVaultFileRequiresDir(t *testing.T) {
	tc := newFullVaultTestSetup(t, 5)
	ctx := context.Background()

	// Migrating to "file" type without providing dir should fail.
	_, err := tc.vault.MigrateVault(ctx, connect.NewRequest(&gastrologv1.MigrateVaultRequest{
		Source:          tc.defaultID.String(),
		Destination:     "file-vault",
		DestinationType: "file",
	}))
	if err == nil {
		t.Fatal("expected error for file type without dir")
	}
	if connect.CodeOf(err) != connect.CodeInvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", connect.CodeOf(err))
	}
}

func TestMergeVaultsAutoDisablesSource(t *testing.T) {
	tc := newTwoVaultTestSetup(t)
	ctx := context.Background()

	// Verify source is enabled before merge.
	if !tc.orch.IsVaultEnabled(tc.srcID) {
		t.Fatal("expected source to be enabled before merge")
	}

	_, err := tc.vault.MergeVaults(ctx, connect.NewRequest(&gastrologv1.MergeVaultsRequest{
		Source:      tc.srcID.String(),
		Destination: tc.dstID.String(),
	}))
	if err != nil {
		t.Fatalf("MergeVaults: %v", err)
	}

	// Source should be auto-disabled after MergeVaults returns.
	if tc.orch.IsVaultEnabled(tc.srcID) {
		t.Error("expected source to be auto-disabled by MergeVaults")
	}
}
