package server_test

import (
	"context"
	"gastrolog/internal/glid"
	"io"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/chunk"
	chunkmem "gastrolog/internal/chunk/memory"
	"gastrolog/internal/index"
	indexmem "gastrolog/internal/index/memory"
	"gastrolog/internal/memtest"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"

	"connectrpc.com/connect"
)

// waitForJob polls the JobService until the job completes or fails, returning the final job state.
func waitForJob(t *testing.T, jobClient gastrologv1connect.JobServiceClient, jobID []byte) *gastrologv1.Job {
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
	defaultID glid.GLID
}

func newVaultTestSetup(t *testing.T, recordCount int) vaultTestClients {
	t.Helper()

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defaultID := glid.New()

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

	orch.RegisterVault(orchestrator.NewVaultFromComponents(defaultID, s.CM, s.IM, s.QE))

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
	t.Parallel()
	clients := newVaultTestSetup(t, 12) // 12 records = 2 sealed (5 each) + 1 active (2)
	ctx := context.Background()

	resp, err := clients.vault.ReindexVault(ctx, connect.NewRequest(&gastrologv1.ReindexVaultRequest{
		Vault: clients.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ReindexVault: %v", err)
	}

	if len(resp.Msg.JobId) == 0 {
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
	t.Parallel()
	clients := newVaultTestSetup(t, 0)
	ctx := context.Background()

	_, err := clients.vault.ReindexVault(ctx, connect.NewRequest(&gastrologv1.ReindexVaultRequest{
		Vault: glid.New().String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestReindexVaultEmpty(t *testing.T) {
	t.Parallel()
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
	t.Parallel()
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
	t.Parallel()
	clients := newVaultTestSetup(t, 0)
	ctx := context.Background()

	_, err := clients.vault.ValidateVault(ctx, connect.NewRequest(&gastrologv1.ValidateVaultRequest{
		Vault: glid.New().String(),
	}))
	if err == nil {
		t.Fatal("expected error for nonexistent vault")
	}
	if connect.CodeOf(err) != connect.CodeNotFound {
		t.Fatalf("expected NotFound, got %v", connect.CodeOf(err))
	}
}

func TestGetStatsDetailed(t *testing.T) {
	t.Parallel()
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
	if glid.FromBytes(vs.Id) != clients.defaultID {
		t.Errorf("expected vault ID %q, got %q", clients.defaultID, glid.FromBytes(vs.Id))
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
	t.Parallel()
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
	cfgStore  system.Store
	orch      *orchestrator.Orchestrator
	defaultID glid.GLID
}

func newFullVaultTestSetup(t *testing.T, recordCount int) fullVaultTestClients {
	t.Helper()

	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore, SegmentsDir: filepath.Join(t.TempDir(), "segments")})
	if err != nil {
		t.Fatal(err)
	}
	defaultID := glid.New()

	factories := orchestrator.Factories{
		ChunkManagers: map[string]chunk.ManagerFactory{
			"memory": chunkmem.NewFactory(),
		},
		IndexManagers: map[string]index.ManagerFactory{
			"memory": indexmem.NewFactory(),
		},
	}

	// Create default vault via config + orchestrator.
	vaultCfg := system.VaultConfig{
		ID:   defaultID,
		Type: system.VaultTypeMemory,
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
		orch:      orch,
		defaultID: defaultID,
	}
}

// startImportPipeline brings the orchestrator's pipeline up so the
// direct-to-vault submit path (ImportRecords → SubmitToVault) has a live local
// segmentation queue to ack against. ReloadFilters runs before Start so the
// home vault is origin-registered while the segmentation manager is not yet
// running (registration after Start races the manager's startup window).
//
// This single-node memory harness has no GroupManager, so pipeline-imported
// records land in durable segments but are not chunked into queryable GLCBs;
// the full import→seal→query round-trip is covered at the component level by
// orchestrator pipeline_query_test.go (Rubicon E1) and end-to-end by the
// cluster acceptance suite (Rubicon E3). These RPC tests assert the handler's
// submit contract: records are accepted and durably committed.
func startImportPipeline(t *testing.T, orch *orchestrator.Orchestrator) {
	t.Helper()
	ctx := context.Background()
	if err := orch.ReloadFilters(ctx); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if err := orch.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = orch.Stop() })
}

func TestExportVault(t *testing.T) {
	t.Parallel()
	tc := newFullVaultTestSetup(t, 12)
	ctx := context.Background()

	stream, err := tc.vault.ExportVault(ctx, connect.NewRequest(&gastrologv1.ExportVaultRequest{
		Vault: tc.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("ExportVault: %v", err)
	}

	var totalRecords int
	for stream.Receive() {
		msg := stream.Msg()
		totalRecords += len(msg.Records)

		// Verify each record has data.
		for _, rec := range msg.Records {
			if len(rec.Raw) == 0 {
				t.Error("exported record has empty raw data")
			}
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
	t.Parallel()
	tc := newFullVaultTestSetup(t, 0)
	ctx := context.Background()

	stream, err := tc.vault.ExportVault(ctx, connect.NewRequest(&gastrologv1.ExportVaultRequest{
		Vault: glid.New().String(),
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
	t.Parallel()
	tc := newFullVaultTestSetup(t, 0) // Empty vault.
	startImportPipeline(t, tc.orch)
	ctx := context.Background()

	records := make([]*gastrologv1.ExportRecord, 10)
	for i := range records {
		records[i] = &gastrologv1.ExportRecord{
			Raw:   []byte("imported-record"),
			Attrs: map[string]string{"source": "import"},
		}
	}

	resp, err := tc.vault.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Vault:   tc.defaultID.String(),
		Records: records,
	}))
	if err != nil {
		t.Fatalf("ImportRecords: %v", err)
	}

	// RecordsImported counts records that were durably committed to the
	// pipeline's segmentation queue (each submit waits for its fsync ack), so a
	// full count proves every record landed. Queryability of pipeline-imported
	// records requires chunking (GroupManager-backed), covered elsewhere — see
	// startImportPipeline.
	if resp.Msg.RecordsImported != 10 {
		t.Errorf("expected 10 records imported, got %d", resp.Msg.RecordsImported)
	}
}

func TestImportRecordsVaultNotFound(t *testing.T) {
	t.Parallel()
	tc := newFullVaultTestSetup(t, 0)
	ctx := context.Background()

	_, err := tc.vault.ImportRecords(ctx, connect.NewRequest(&gastrologv1.ImportRecordsRequest{
		Vault:   glid.New().String(),
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
	t.Parallel()
	tc := newFullVaultTestSetup(t, 12)
	startImportPipeline(t, tc.orch)
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

	// Every exported record round-trips back through the pipeline submit path
	// and is durably committed (RecordsImported counts fsync-acked records).
	// The original 12 stay queryable in the vault's chunk manager; the 12
	// re-imported records land in pipeline segments and become queryable only
	// once chunked (GroupManager-backed) — see startImportPipeline for why this
	// harness asserts the submit contract rather than the full query round-trip.
	if resp.Msg.RecordsImported != 12 {
		t.Errorf("expected 12 records imported, got %d", resp.Msg.RecordsImported)
	}

	// The 12 originally-seeded records remain queryable in the vault.
	stats, err := tc.vault.GetStats(ctx, connect.NewRequest(&gastrologv1.GetStatsRequest{
		Vault: tc.defaultID.String(),
	}))
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.Msg.TotalRecords != 12 {
		t.Errorf("expected 12 seeded records queryable, got %d", stats.Msg.TotalRecords)
	}
}
