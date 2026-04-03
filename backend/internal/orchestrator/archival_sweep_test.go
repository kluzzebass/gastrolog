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

// ==========================================================================
// Multi-node cloud archival cluster tests
// ==========================================================================

// cloudClusterHarness extends clusterHarness with a shared cloud store.
type cloudClusterHarness struct {
	*clusterHarness
	cloudStore *blobstore.Memory
	csID       uuid.UUID
}

// setupCloudCluster creates a 4-node cluster where the single tier is cloud-backed
// using a shared in-memory blobstore. The leader has a file-backed chunk manager
// with CloudStore set; followers have file-backed chunk managers without CloudStore
// (matching production: followers don't upload to cloud).
func setupCloudCluster(t *testing.T, transitions []config.CloudStorageTransition) *cloudClusterHarness {
	t.Helper()
	nodeIDs := []string{"leader", "f1", "f2", "f3"}
	leaderID := nodeIDs[0]
	vaultID := uuid.Must(uuid.NewV7())
	tierID := uuid.Must(uuid.NewV7())
	csID := uuid.Must(uuid.NewV7())

	cloudStore := blobstore.NewMemory()

	store := cfgmem.NewStore()
	placements := []config.TierPlacement{
		{StorageID: config.SyntheticStorageID(leaderID), Leader: true},
	}
	for _, fid := range nodeIDs[1:] {
		placements = append(placements, config.TierPlacement{
			StorageID: config.SyntheticStorageID(fid), Leader: false,
		})
	}
	_ = store.PutTier(context.Background(), config.TierConfig{
		ID: tierID, Name: "cloud-tier", Type: config.TierTypeCloud,
		Placements: placements, CloudServiceID: &csID,
	})
	_ = store.PutVault(context.Background(), config.VaultConfig{
		ID: vaultID, Name: "cloud-vault", TierIDs: []uuid.UUID{tierID},
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

	followerTargets := make([]config.ReplicationTarget, 0, len(nodeIDs)-1)
	for _, fid := range nodeIDs[1:] {
		followerTargets = append(followerTargets, config.ReplicationTarget{NodeID: fid})
	}

	orchs := make(map[string]*Orchestrator)
	nodes := make(map[string]*clusterTestNode)

	for _, nid := range nodeIDs {
		orch, err := New(Config{
			LocalNodeID:  nid,
			ConfigLoader: &transitionConfigLoader{store: store},
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = orch.Scheduler().Stop()
		orchs[nid] = orch

		isLeader := nid == leaderID
		dir := t.TempDir()

		var cmCfg chunkfile.Config
		cmCfg.Dir = dir
		cmCfg.Now = time.Now
		cmCfg.RotationPolicy = chunk.NewRecordCountPolicy(100)
		if isLeader {
			cmCfg.CloudStore = cloudStore
			cmCfg.VaultID = vaultID
		}

		cm, err := chunkfile.NewManager(cmCfg)
		if err != nil {
			t.Fatal(err)
		}
		im := indexfile.NewManager(dir, nil, nil)

		tier := &TierInstance{
			TierID: tierID, Type: "cloud",
			Chunks: cm, Indexes: im, Query: query.New(cm, im, nil),
		}
		if isLeader {
			tier.FollowerTargets = followerTargets
		} else {
			tier.IsFollower = true
			tier.LeaderNodeID = leaderID
		}

		orch.RegisterVault(NewVault(vaultID, tier))
		nodes[nid] = &clusterTestNode{
			nodeID:   nid,
			orch:     orch,
			tiers:    []*TierInstance{tier},
			tierDirs: []string{dir},
		}
	}

	for _, nid := range nodeIDs {
		remotes := make(map[string]*Orchestrator)
		for _, other := range nodeIDs {
			if other != nid {
				remotes[other] = orchs[other]
			}
		}
		orchs[nid].SetRemoteTransferrer(&directTransferrer{nodes: remotes})
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.orch.Stop()
		}
		for _, n := range nodes {
			for _, tier := range n.tiers {
				_ = tier.Chunks.Close()
			}
		}
	})

	return &cloudClusterHarness{
		clusterHarness: &clusterHarness{
			nodes:    nodes,
			cfgStore: store,
			vaultID:  vaultID,
			tierIDs:  []uuid.UUID{tierID},
		},
		cloudStore: cloudStore,
		csID:       csID,
	}
}

func TestCloudClusterArchivalSweepSetsArchivedOnLeader(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: "cold"},
	})

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	// Ingest, seal, upload to cloud on leader.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 500 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		if err := leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "cluster-archive-%d", i),
		}); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if active := leaderCM.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderCM.Seal()
	}
	metas, _ := leaderCM.List()
	for _, m := range metas {
		if m.Sealed {
			_ = leaderCM.PostSealProcess(context.Background(), m.ID)
		}
	}

	// Verify cloud-backed before sweep.
	metasAfter, _ := leaderCM.List()
	cloudCount := 0
	for _, m := range metasAfter {
		if m.CloudBacked {
			cloudCount++
		}
	}
	if cloudCount == 0 {
		t.Fatal("expected cloud-backed chunks after PostSealProcess")
	}
	t.Logf("leader: %d cloud-backed chunks", cloudCount)

	// Run archival sweep with aged chunks.
	leaderNode.orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	leaderNode.orch.archivalSweepAll()

	// Verify archived on leader.
	archivedCount := 0
	metasFinal, _ := leaderCM.List()
	for _, m := range metasFinal {
		if m.Archived {
			archivedCount++
		}
	}
	if archivedCount == 0 {
		t.Error("expected at least one archived chunk on leader after sweep")
	}
	t.Logf("leader: %d archived chunks", archivedCount)
}

func TestCloudClusterArchivalSweepOnlyRunsOnLeader(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: "cold"},
	})

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	// Ingest and upload on leader.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "leader-only-%d", i),
		})
	}
	if active := leaderCM.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderCM.Seal()
	}
	metas, _ := leaderCM.List()
	for _, m := range metas {
		if m.Sealed {
			_ = leaderCM.PostSealProcess(context.Background(), m.ID)
		}
	}

	// Run archival sweep on a FOLLOWER — should be a no-op.
	follower := h.nodes["f1"]
	follower.orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	follower.orch.archivalSweepAll()

	// Leader's chunks should NOT be archived (follower can't archive them).
	metasFinal, _ := leaderCM.List()
	for _, m := range metasFinal {
		if m.Archived {
			t.Errorf("chunk %s should not be archived by follower sweep", m.ID)
		}
	}
}

func TestCloudClusterRestoreChunkViaOrchestrator(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, []config.CloudStorageTransition{
		{AfterDays: 1, StorageClass: "cold"},
	})

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	// Ingest, seal, upload, archive.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "restore-%d", i),
		})
	}
	if active := leaderCM.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderCM.Seal()
	}
	metas, _ := leaderCM.List()
	for _, m := range metas {
		if m.Sealed {
			_ = leaderCM.PostSealProcess(context.Background(), m.ID)
		}
	}

	// Archive via sweep.
	leaderNode.orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	leaderNode.orch.archivalSweepAll()

	// Verify archived.
	metasArchived, _ := leaderCM.List()
	var archivedID chunk.ChunkID
	for _, m := range metasArchived {
		if m.Archived {
			archivedID = m.ID
			break
		}
	}
	if archivedID == (chunk.ChunkID{}) {
		t.Fatal("no archived chunk found")
	}

	// Restore via orchestrator.
	if err := leaderNode.orch.RestoreChunk(context.Background(), h.vaultID, archivedID, "Standard", 7); err != nil {
		t.Fatalf("RestoreChunk: %v", err)
	}

	// Verify not archived.
	meta, err := leaderCM.Meta(archivedID)
	if err != nil {
		t.Fatalf("Meta after restore: %v", err)
	}
	if meta.Archived {
		t.Error("chunk should not be archived after restore")
	}
}

func TestCloudClusterReconcileSweepDetectsMissingBlobs(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, nil)

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	// Ingest, seal, upload.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "reconcile-%d", i),
		})
	}
	if active := leaderCM.Active(); active != nil && active.RecordCount > 0 {
		_ = leaderCM.Seal()
	}
	metas, _ := leaderCM.List()
	for _, m := range metas {
		if m.Sealed {
			_ = leaderCM.PostSealProcess(context.Background(), m.ID)
		}
	}

	// Delete blobs from cloud store (simulate external lifecycle).
	_ = h.cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		return h.cloudStore.Delete(context.Background(), info.Key)
	})

	// Reconcile on leader — should mark suspect, NOT remove.
	leaderNode.orch.reconcileSweepAll()

	metasAfter, _ := leaderCM.List()
	if len(metasAfter) == 0 {
		t.Error("chunks should NOT be removed from index on first reconciliation")
	}

	// Verify suspect tracker has entries.
	suspectCount := 0
	for _, m := range metasAfter {
		if _, ok := leaderNode.orch.suspects.suspectSince(m.ID); ok {
			suspectCount++
		}
	}
	if suspectCount == 0 {
		t.Error("expected suspect entries after reconciliation with missing blobs")
	}
	t.Logf("leader: %d suspect chunks after reconciliation", suspectCount)
}
