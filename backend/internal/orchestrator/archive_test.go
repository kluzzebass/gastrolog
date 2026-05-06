package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// TestArchiveChunkViaRetentionSweep runs the full archival lifecycle:
// ingest → seal → upload to cloud → retention sweep with archive action
// → verify chunk is archived (unreadable) → restore → verify readable.
func TestArchiveChunkViaRetentionSweep(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	tierID := glid.New()
	nodeID := "test-node"

	cloudStore := blobstore.NewMemory()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore: cloudStore, VaultID: vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)

	store := sysmem.NewStore()
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "archive-test",
	})
	_ = store.PutTier(context.Background(), system.TierConfig{
		ID: tierID, Name: "cloud", Type: system.VaultTypeFile,
		VaultID: vaultID, Position: 0,
	})

	orch := newTestOrch(t, Config{LocalNodeID: nodeID, SystemLoader: &transitionSystemLoader{store: store}})
	_ = orch.Scheduler().Stop()

	tier := &VaultInstance{TierID: tierID, Type: "cloud", Chunks: cm, Indexes: im, Query: query.New(cm, im, nil)}
	orch.RegisterVault(NewVault(vaultID, tier))
	t.Cleanup(func() { _ = cm.Close() })

	// Ingest 100 records, seal, upload to cloud.
	const recordCount = 100
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range recordCount {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "archive-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = cm.Seal()

	metas, _ := cm.List()
	chunkID := metas[0].ID
	if err := cm.PostSealProcess(context.Background(), chunkID); err != nil {
		t.Fatalf("PostSealProcess: %v", err)
	}

	// Verify cloud-backed and readable.
	meta, _ := cm.Meta(chunkID)
	if !meta.CloudBacked {
		t.Fatal("expected cloud-backed after upload")
	}
	if meta.Archived {
		t.Fatal("should not be archived before sweep")
	}
	preRecords := readAllRecords(t, cm)
	if len(preRecords) != recordCount {
		t.Fatalf("pre-archive: expected %d records, got %d", recordCount, len(preRecords))
	}

	// Archive via orchestrator RPC path.
	if err := orch.ArchiveChunk(context.Background(), vaultID, chunkID, "GLACIER"); err != nil {
		t.Fatalf("ArchiveChunk: %v", err)
	}

	// Verify: chunk is now archived.
	meta, _ = cm.Meta(chunkID)
	if !meta.Archived {
		t.Error("chunk should be archived after sweep")
	}

	// Verify: cursor reads fail with ErrChunkArchived.
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		// Error on open is acceptable for archived chunks.
		if !errors.Is(err, chunk.ErrChunkArchived) {
			t.Fatalf("expected ErrChunkArchived on open, got %v", err)
		}
	} else {
		_, _, err = cursor.Next()
		_ = cursor.Close()
		if !errors.Is(err, chunk.ErrChunkArchived) {
			t.Errorf("expected ErrChunkArchived on read, got %v", err)
		}
	}

	// Verify: blob in cloud store shows GLACIER storage class.
	var archivedBlobs int
	_ = cloudStore.List(context.Background(), "", func(bi blobstore.BlobInfo) error {
		if bi.IsArchived() {
			archivedBlobs++
		}
		return nil
	})
	if archivedBlobs == 0 {
		t.Error("no archived blobs in cloud store after archive sweep")
	}

	// Restore the chunk.
	if err := cm.RestoreChunk(context.Background(), chunkID, "Standard", 7); err != nil {
		t.Fatalf("RestoreChunk: %v", err)
	}

	// Verify: chunk is no longer archived.
	meta, _ = cm.Meta(chunkID)
	if meta.Archived {
		t.Error("chunk should not be archived after restore")
	}

	// Verify: records are readable again.
	postRecords := readAllRecords(t, cm)
	if len(postRecords) != recordCount {
		t.Errorf("post-restore: expected %d records, got %d", recordCount, len(postRecords))
	}
}

// TestArchiveNonCloudChunkFails verifies that archiving a non-cloud-backed
// chunk returns an error (only cloud chunks can be archived).
func TestArchiveNonCloudChunkFails(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	ts := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	if _, _, err := cm.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	_ = cm.Seal()

	metas, _ := cm.List()

	// Should fail — no cloud store.
	err = cm.ArchiveChunk(context.Background(), metas[0].ID, "GLACIER")
	if err == nil {
		t.Error("expected error when archiving non-cloud chunk")
	}
}

// TestArchiveAlreadyArchivedIsNoop verifies idempotency.
func TestArchiveAlreadyArchivedIsNoop(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cloudStore := blobstore.NewMemory()
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore: cloudStore, VaultID: vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()

	ts := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	if _, _, err := cm.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	_ = cm.Seal()
	metas, _ := cm.List()
	_ = cm.PostSealProcess(context.Background(), metas[0].ID)

	// Archive twice — second should be a no-op.
	if err := cm.ArchiveChunk(context.Background(), metas[0].ID, "GLACIER"); err != nil {
		t.Fatal(err)
	}
	if err := cm.ArchiveChunk(context.Background(), metas[0].ID, "DEEP_ARCHIVE"); err != nil {
		t.Fatal(err)
	}

	// Should still be archived (first call won, second was no-op).
	meta, _ := cm.Meta(metas[0].ID)
	if !meta.Archived {
		t.Error("should be archived")
	}
}
