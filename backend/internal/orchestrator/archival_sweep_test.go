package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/config"
	cfgmem "gastrolog/internal/config/memory"
	indexfile "gastrolog/internal/index/file"
	"gastrolog/internal/query"

	"github.com/google/uuid"
)

// --- helpers ---

// archivalTestSetup creates a single-node orchestrator with a cloud tier backed
// by the in-memory blobstore. Returns the orchestrator, cloud store, chunk manager,
// vault/tier IDs, and config store.
func archivalTestSetup(t *testing.T, transitions []config.CloudStorageTransition) (
	*Orchestrator, *blobstore.Memory, *chunkfile.Manager, uuid.UUID, uuid.UUID, *cfgmem.Store,
) {
	t.Helper()
	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	csID := uuid.Must(uuid.NewV7())
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

	store := cfgmem.NewStore()
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "archival-test", TierIDs: []uuid.UUID{tierID},
	})
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tierID, Name: "cloud", Type: config.TierTypeCloud,
		Placements:     syntheticPlacements(nodeID),
		CloudServiceID: &csID,
	})
	_ = store.PutCloudService(context.Background(), config.CloudService{
		ID:           csID,
		Name:         "test-cloud",
		Provider:     "memory",
		ArchivalMode: "active",
		Transitions:  transitions,
		RestoreTier:  "Standard",
		RestoreDays:  7,
	})

	orch, err := New(Config{
		LocalNodeID:  nodeID,
		ConfigLoader: &transitionConfigLoader{store: store},
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = orch.Scheduler().Stop()

	tier := &TierInstance{
		TierID: tierID, Type: "cloud",
		Chunks: cm, Indexes: im, Query: query.New(cm, im, nil),
	}
	orch.RegisterVault(NewVault(vaultID, tier))

	t.Cleanup(func() { _ = cm.Close() })

	return orch, cloudStore, cm, vaultID, tierID, store
}

// ingestSealUpload ingests N records, seals, and runs PostSealProcess (compress + cloud upload).
func ingestSealUpload(t *testing.T, cm *chunkfile.Manager, n int) []chunk.ChunkID {
	t.Helper()
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := range n {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if _, _, err := cm.Append(chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "rec-%d", i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	_ = cm.Seal()
	metas, _ := cm.List()
	var ids []chunk.ChunkID
	for _, m := range metas {
		if m.Sealed && !m.CloudBacked {
			if err := cm.PostSealProcess(context.Background(), m.ID); err != nil {
				t.Fatalf("PostSealProcess: %v", err)
			}
		}
		ids = append(ids, m.ID)
	}
	return ids
}

// --- archival sweep tests ---

func TestArchivalSweepArchivesOldChunks(t *testing.T) {
	t.Parallel()
	orch, _, cm, _, _, _ := archivalTestSetup(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: "GLACIER"},
	})

	ids := ingestSealUpload(t, cm, 50)
	if len(ids) == 0 {
		t.Fatal("no chunks")
	}

	// Chunks are freshly sealed — WriteEnd is now. Sweep shouldn't archive.
	orch.archivalSweepAll()
	meta, _ := cm.Meta(ids[0])
	if meta.Archived {
		t.Error("chunk should not be archived yet (too young)")
	}

	// Hack WriteEnd to be 2 days ago by using a frozen Now on the orchestrator.
	orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	orch.archivalSweepAll()

	meta, _ = cm.Meta(ids[0])
	if !meta.Archived {
		t.Error("chunk should be archived after sweep with age > 1 day")
	}
}

func TestArchivalSweepDeletesExpiredChunks(t *testing.T) {
	t.Parallel()
	orch, _, cm, _, _, _ := archivalTestSetup(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: ""},  // delete after 1 day
	})

	ids := ingestSealUpload(t, cm, 50)

	// Age the chunks past the threshold.
	orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	orch.archivalSweepAll()

	// Chunk should be deleted.
	_, err := cm.Meta(ids[0])
	if !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Errorf("expected chunk deleted after expiry sweep, got err=%v", err)
	}
}

func TestArchivalSweepIgnoresInactiveServices(t *testing.T) {
	t.Parallel()
	orch, _, cm, _, _, store := archivalTestSetup(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: "GLACIER"},
	})

	ids := ingestSealUpload(t, cm, 50)

	// Set archival mode to "none".
	cfg, _ := store.Load(context.Background())
	cs := cfg.CloudServices[0]
	cs.ArchivalMode = "none"
	_ = store.PutCloudService(context.Background(), cs)

	orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	orch.archivalSweepAll()

	meta, _ := cm.Meta(ids[0])
	if meta.Archived {
		t.Error("chunk should NOT be archived when service archival mode is none")
	}
}

func TestArchivalSweepMultiStepTransition(t *testing.T) {
	t.Parallel()
	orch, _, cm, _, _, _ := archivalTestSetup(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: "cold"},
		{AfterDays: 30, StorageClass: "deep-freeze"},
	})

	ids := ingestSealUpload(t, cm, 50)

	// 2 days old → should match first transition (cold).
	orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	orch.archivalSweepAll()

	meta, _ := cm.Meta(ids[0])
	if !meta.Archived {
		t.Error("chunk should be archived after 2 days (cold threshold is 1 day)")
	}
}

// --- reconciliation sweep tests ---

func TestReconcileSweepMarksSuspectOnMissing(t *testing.T) {
	t.Parallel()
	orch, cloudStore, cm, _, _, _ := archivalTestSetup(t, nil)

	ids := ingestSealUpload(t, cm, 50)

	// Delete the blob from cloud store (simulate external lifecycle delete).
	_ = cloudStore.Delete(context.Background(), "")
	// Actually need to find the blob key. Let's delete all blobs.
	_ = cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		_ = cloudStore.Delete(context.Background(), info.Key)
		return nil
	})

	// Reconcile should mark as suspect, NOT remove.
	orch.reconcileSweepAll()

	// Chunk should still be in the index.
	_, err := cm.Meta(ids[0])
	if errors.Is(err, chunk.ErrChunkNotFound) {
		t.Error("chunk should NOT be removed from index on first reconciliation")
	}

	// Suspect tracker should have an entry.
	_, isSuspect := orch.suspects.suspectSince(ids[0])
	if !isSuspect {
		t.Error("chunk should be marked as suspect")
	}
}

func TestReconcileSweepRemovesAfterGracePeriod(t *testing.T) {
	t.Parallel()
	orch, cloudStore, cm, _, _, store := archivalTestSetup(t, nil)

	ids := ingestSealUpload(t, cm, 50)

	// Set grace period to 1 day.
	cfg, _ := store.Load(context.Background())
	cs := cfg.CloudServices[0]
	cs.SuspectGraceDays = 1
	_ = store.PutCloudService(context.Background(), cs)

	// Delete blobs.
	_ = cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		_ = cloudStore.Delete(context.Background(), info.Key)
		return nil
	})

	// First reconcile: marks suspect.
	orch.reconcileSweepAll()
	_, isSuspect := orch.suspects.suspectSince(ids[0])
	if !isSuspect {
		t.Fatal("should be suspect after first reconcile")
	}

	// Advance past grace period.
	orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	orch.reconcileSweepAll()

	// Now the chunk should be removed from the index.
	_, err := cm.Meta(ids[0])
	if !errors.Is(err, chunk.ErrChunkNotFound) {
		t.Error("chunk should be removed from index after grace period")
	}
}

func TestReconcileSweepClearsSuspectWhenBlobReturns(t *testing.T) {
	t.Parallel()
	orch, cloudStore, cm, _, _, _ := archivalTestSetup(t, nil)

	ids := ingestSealUpload(t, cm, 50)

	// Archive the blob (makes it unreadable but still in store).
	_ = cloudStore.Archive(context.Background(), "", "GLACIER")
	// Actually need the real key — archive all blobs.
	_ = cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		return cloudStore.Archive(context.Background(), info.Key, "GLACIER")
	})

	// Reconcile should NOT mark as suspect (archived != missing).
	// But OpenCursor returns ErrChunkArchived, not ErrChunkSuspect.
	// So it shouldn't be suspect.
	orch.reconcileSweepAll()
	_, isSuspect := orch.suspects.suspectSince(ids[0])
	if isSuspect {
		t.Error("archived chunk should not be marked suspect (it's archived, not missing)")
	}

	// Now restore — blob becomes readable again.
	_ = cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		return cloudStore.Restore(context.Background(), info.Key, "Standard", 7)
	})

	// Manually mark as suspect to test the clear path.
	orch.suspects.mark(ids[0], time.Now().Add(-time.Hour))

	// Reconcile should clear suspect because blob is now readable.
	orch.reconcileSweepAll()
	_, stillSuspect := orch.suspects.suspectSince(ids[0])
	if stillSuspect {
		t.Error("suspect should be cleared after blob becomes readable")
	}
}

// --- ErrChunkSuspect flow tests ---

func TestChunkSuspectSkippedInTransition(t *testing.T) {
	t.Parallel()
	orch, cloudStore, cm, vaultID, tierID, _ := archivalTestSetup(t, nil)

	ids := ingestSealUpload(t, cm, 50)

	// Delete blobs to make cursor return ErrChunkSuspect.
	_ = cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		_ = cloudStore.Delete(context.Background(), info.Key)
		return nil
	})

	// Transition should skip without panic or index removal.
	orch.TransitionChunk(vaultID, tierID, ids[0])

	// Chunk should still be in the index.
	_, err := cm.Meta(ids[0])
	if errors.Is(err, chunk.ErrChunkNotFound) {
		t.Error("transition should NOT remove suspect chunk from index")
	}
}

// --- full lifecycle test ---

func TestArchivalFullLifecycle(t *testing.T) {
	t.Parallel()
	orch, _, cm, vaultID, _, _ := archivalTestSetup(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: "cold"},
	})

	// 1. Ingest and upload to cloud.
	ids := ingestSealUpload(t, cm, 500)
	chunkID := ids[0]

	meta, _ := cm.Meta(chunkID)
	if !meta.CloudBacked {
		t.Fatal("expected cloud-backed")
	}
	if meta.Archived {
		t.Fatal("should not be archived yet")
	}

	// 2. Verify cloud-backed chunk metadata is correct.
	t.Logf("chunk %s: cloud=%v, archived=%v, records=%d",
		chunkID, meta.CloudBacked, meta.Archived, meta.RecordCount)
	if meta.RecordCount != 500 {
		t.Fatalf("expected 500 records in metadata, got %d", meta.RecordCount)
	}

	// 3. Archival sweep with aged chunks.
	orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	orch.archivalSweepAll()

	meta, _ = cm.Meta(chunkID)
	if !meta.Archived {
		t.Fatal("expected archived after sweep")
	}

	// 4. Records NOT readable (archived).
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		if !errors.Is(err, chunk.ErrChunkArchived) && !errors.Is(err, chunk.ErrChunkSuspect) {
			t.Fatalf("expected archive/suspect error, got %v", err)
		}
	} else {
		_, _, readErr := cursor.Next()
		_ = cursor.Close()
		if readErr == nil {
			t.Error("expected read error on archived chunk")
		}
	}

	// 5. Restore.
	if err := orch.RestoreChunk(context.Background(), vaultID, chunkID, "Standard", 7); err != nil {
		t.Fatalf("RestoreChunk: %v", err)
	}

	meta, _ = cm.Meta(chunkID)
	if meta.Archived {
		t.Error("should not be archived after restore")
	}

	// 6. Metadata shows not-archived after restore.
	meta, _ = cm.Meta(chunkID)
	if meta.Archived {
		t.Error("should not be archived after restore")
	}
	if !meta.CloudBacked {
		t.Error("should still be cloud-backed after restore")
	}
	if meta.RecordCount != 500 {
		t.Errorf("record count should be preserved, got %d", meta.RecordCount)
	}
}

// --- suspect tracker tests ---

func TestSuspectTrackerMarkClearLookup(t *testing.T) {
	t.Parallel()
	tracker := newSuspectTracker()
	id := chunk.NewChunkID()
	now := time.Now()

	// Not suspect initially.
	_, ok := tracker.suspectSince(id)
	if ok {
		t.Error("should not be suspect initially")
	}

	// Mark.
	tracker.mark(id, now)
	since, ok := tracker.suspectSince(id)
	if !ok {
		t.Fatal("should be suspect after mark")
	}
	if since != now {
		t.Errorf("suspectSince=%v, want %v", since, now)
	}

	// Mark again doesn't overwrite.
	later := now.Add(time.Hour)
	tracker.mark(id, later)
	since, _ = tracker.suspectSince(id)
	if since != now {
		t.Error("second mark should not overwrite first")
	}

	// Clear.
	tracker.clear(id)
	_, ok = tracker.suspectSince(id)
	if ok {
		t.Error("should not be suspect after clear")
	}
}

// --- config round-trip test ---

func TestCloudServiceArchivalConfigRoundTrip(t *testing.T) {
	t.Parallel()
	store := cfgmem.NewStore()
	ctx := context.Background()

	cs := config.CloudService{
		ID:                uuid.Must(uuid.NewV7()),
		Name:              "roundtrip-test",
		Provider:          "memory",
		ArchivalMode:      "active",
		Transitions: []config.CloudStorageTransition{
			{AfterDays: 30, StorageClass: "cold"},
			{AfterDays: 90, StorageClass: "deep-freeze"},
			{AfterDays: 365, StorageClass: ""},
		},
		RestoreTier:       "Expedited",
		RestoreDays:       14,
		SuspectGraceDays:  3,
		ReconcileSchedule: "0 */6 * * *",
	}

	if err := store.PutCloudService(ctx, cs); err != nil {
		t.Fatal(err)
	}

	loaded, err := store.GetCloudService(ctx, cs.ID)
	if err != nil {
		t.Fatal(err)
	}

	if loaded.ArchivalMode != "active" {
		t.Errorf("ArchivalMode=%q", loaded.ArchivalMode)
	}
	if len(loaded.Transitions) != 3 {
		t.Fatalf("Transitions len=%d, want 3", len(loaded.Transitions))
	}
	if loaded.Transitions[0].AfterDays != 30 || loaded.Transitions[0].StorageClass != "cold" {
		t.Errorf("Transition 0: %+v", loaded.Transitions[0])
	}
	if loaded.Transitions[2].StorageClass != "" {
		t.Errorf("Transition 2 (delete): StorageClass=%q", loaded.Transitions[2].StorageClass)
	}
	if loaded.RestoreTier != "Expedited" {
		t.Errorf("RestoreTier=%q", loaded.RestoreTier)
	}
	if loaded.RestoreDays != 14 {
		t.Errorf("RestoreDays=%d", loaded.RestoreDays)
	}
	if loaded.SuspectGraceDays != 3 {
		t.Errorf("SuspectGraceDays=%d", loaded.SuspectGraceDays)
	}
	if loaded.ReconcileSchedule != "0 */6 * * *" {
		t.Errorf("ReconcileSchedule=%q", loaded.ReconcileSchedule)
	}
}

// --- helpers ---

func readAll(rc interface{ Read([]byte) (int, error); Close() error }) ([]byte, error) {
	var buf []byte
	tmp := make([]byte, 4096)
	for {
		n, err := rc.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if err != nil {
			_ = rc.Close()
			return buf, nil
		}
	}
}

func byteReader(data []byte) *byteReaderImpl {
	return &byteReaderImpl{data: data}
}

type byteReaderImpl struct {
	data []byte
	pos  int
}

func (r *byteReaderImpl) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
