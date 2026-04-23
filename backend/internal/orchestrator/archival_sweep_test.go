package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"os"
	"path/filepath"
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

// --- helpers ---

// archivalTestSetup creates a single-node orchestrator with a cloud tier backed
// by the in-memory blobstore. Returns the orchestrator, cloud store, chunk manager,
// vault/tier IDs, and config store.
func archivalTestSetup(t *testing.T, transitions []system.CloudStorageTransition) (
	*Orchestrator, *blobstore.Memory, *chunkfile.Manager, glid.GLID, glid.GLID, *sysmem.Store,
) {
	t.Helper()
	vaultID := glid.New()
	tierID := glid.New()
	csID := glid.New()
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
		ID: vaultID, Name: "archival-test",
	})
	_ = store.PutTier(context.Background(), system.TierConfig{
		ID: tierID, Name: "cloud", Type: system.TierTypeCloud, CloudServiceID: &csID,
		VaultID: vaultID, Position: 0,
	})
	_ = store.SetTierPlacements(context.Background(), tierID, []system.TierPlacement{{StorageID: system.SyntheticStorageID("test-node"), Leader: true}})
	_ = store.PutCloudService(context.Background(), system.CloudService{
		ID:           csID,
		Name:         "test-cloud",
		Provider:     "memory",
		ArchivalMode: "active",
		Transitions:  transitions,
		RestoreTier:  "Standard",
		RestoreDays:  7,
	})

	orch := newTestOrch(t, Config{
		LocalNodeID:  nodeID,
		SystemLoader: &transitionSystemLoader{store: store},
	})
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
	orch, _, cm, _, _, _ := archivalTestSetup(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "GLACIER"},
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
	orch, _, cm, _, _, _ := archivalTestSetup(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: ""}, // delete after 1 day
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
	orch, _, cm, _, _, store := archivalTestSetup(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "GLACIER"},
	})

	ids := ingestSealUpload(t, cm, 50)

	// Set archival mode to "none".
	sys, _ := store.Load(context.Background())
	cs := sys.Config.CloudServices[0]
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
	orch, _, cm, _, _, _ := archivalTestSetup(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "cold"},
		{After: "30d", StorageClass: "deep-freeze"},
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
	sys, _ := store.Load(context.Background())
	cs := sys.Config.CloudServices[0]
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
	orch.TransitionChunkForTesting(vaultID, tierID, ids[0])

	// Chunk should still be in the index.
	_, err := cm.Meta(ids[0])
	if errors.Is(err, chunk.ErrChunkNotFound) {
		t.Error("transition should NOT remove suspect chunk from index")
	}
}

// --- full lifecycle test ---

func TestArchivalFullLifecycle(t *testing.T) {
	t.Parallel()
	orch, _, cm, vaultID, _, _ := archivalTestSetup(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "cold"},
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
	store := sysmem.NewStore()
	ctx := context.Background()

	cs := system.CloudService{
		ID:           glid.New(),
		Name:         "roundtrip-test",
		Provider:     "memory",
		ArchivalMode: "active",
		Transitions: []system.CloudStorageTransition{
			{After: "30d", StorageClass: "cold"},
			{After: "90d", StorageClass: "deep-freeze"},
			{After: "365d", StorageClass: ""},
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
	if loaded.Transitions[0].After != "30d" || loaded.Transitions[0].StorageClass != "cold" {
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

func readAll(rc interface {
	Read([]byte) (int, error)
	Close() error
}) ([]byte, error) {
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
	csID       glid.GLID
}

// setupCloudCluster creates a 4-node cluster where the single tier is cloud-backed
// using a shared in-memory blobstore. The leader has a file-backed chunk manager
// with CloudStore set; followers have file-backed chunk managers without CloudStore
// (matching production: followers don't upload to cloud).
func setupCloudCluster(t *testing.T, transitions []system.CloudStorageTransition) *cloudClusterHarness {
	t.Helper()
	nodeIDs := []string{"leader", "f1", "f2", "f3"}
	leaderID := nodeIDs[0]
	vaultID := glid.New()
	tierID := glid.New()
	csID := glid.New()

	cloudStore := blobstore.NewMemory()

	store := sysmem.NewStore()
	placements := []system.TierPlacement{
		{StorageID: system.SyntheticStorageID(leaderID), Leader: true},
	}
	for _, fid := range nodeIDs[1:] {
		placements = append(placements, system.TierPlacement{
			StorageID: system.SyntheticStorageID(fid), Leader: false,
		})
	}
	_ = store.PutTier(context.Background(), system.TierConfig{
		ID: tierID, Name: "cloud-tier", Type: system.TierTypeCloud, CloudServiceID: &csID,
		VaultID: vaultID, Position: 0,
	})
	_ = store.SetTierPlacements(context.Background(), tierID, placements)
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "cloud-vault",
	})
	_ = store.PutCloudService(context.Background(), system.CloudService{
		ID:           csID,
		Name:         "test-cloud",
		Provider:     "memory",
		ArchivalMode: "active",
		Transitions:  transitions,
		RestoreTier:  "Standard",
		RestoreDays:  7,
	})

	followerTargets := make([]system.ReplicationTarget, 0, len(nodeIDs)-1)
	for _, fid := range nodeIDs[1:] {
		followerTargets = append(followerTargets, system.ReplicationTarget{NodeID: fid})
	}

	orchs := make(map[string]*Orchestrator)
	nodes := make(map[string]*clusterTestNode)

	for _, nid := range nodeIDs {
		orch := newTestOrch(t, Config{
			LocalNodeID:  nid,
			SystemLoader: &transitionSystemLoader{store: store},
		})
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
			tierIDs:  []glid.GLID{tierID},
		},
		cloudStore: cloudStore,
		csID:       csID,
	}
}

func TestCloudClusterArchivalSweepSetsArchivedOnLeader(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "cold"},
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
	h := setupCloudCluster(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "cold"},
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
	h := setupCloudCluster(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "cold"},
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

func TestCloudClusterArchivedChunkUnreadableOnLeader(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "cold"},
	})

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "unreadable-%d", i),
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

	// Try to read — should get ErrChunkArchived or ErrChunkSuspect.
	metasAfter, _ := leaderCM.List()
	for _, m := range metasAfter {
		if !m.Archived {
			continue
		}
		_, err := leaderCM.OpenCursor(m.ID)
		if err == nil {
			t.Errorf("chunk %s: expected error reading archived chunk, got nil", m.ID)
		} else if !errors.Is(err, chunk.ErrChunkArchived) {
			// ErrChunkSuspect is also acceptable (the download returns 404-like from archived blob).
			if !errors.Is(err, chunk.ErrChunkSuspect) {
				t.Errorf("chunk %s: expected ErrChunkArchived or ErrChunkSuspect, got %v", m.ID, err)
			}
		}
	}
}

func TestCloudClusterSweepThresholdBoundary(t *testing.T) {
	t.Parallel()
	// AfterDays=5: chunks younger than 5 days should NOT be archived.
	h := setupCloudCluster(t, []system.CloudStorageTransition{
		{After: "5d", StorageClass: "cold"},
	})

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "boundary-%d", i),
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

	// 3 days later — below threshold, should NOT archive.
	leaderNode.orch.now = func() time.Time { return time.Now().Add(72 * time.Hour) }
	leaderNode.orch.archivalSweepAll()

	metasMid, _ := leaderCM.List()
	for _, m := range metasMid {
		if m.Archived {
			t.Errorf("chunk %s archived at 3 days (threshold is 5)", m.ID)
		}
	}

	// 6 days later — above threshold, should archive.
	leaderNode.orch.now = func() time.Time { return time.Now().Add(144 * time.Hour) }
	leaderNode.orch.archivalSweepAll()

	metasFinal, _ := leaderCM.List()
	archivedCount := 0
	for _, m := range metasFinal {
		if m.Archived {
			archivedCount++
		}
	}
	if archivedCount == 0 {
		t.Error("expected chunks archived at 6 days (threshold is 5)")
	}
	t.Logf("archived %d chunks at 6 days", archivedCount)
}

func TestCloudClusterGracePeriodBoundary(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, nil)

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	// Set grace period to 3 days.
	sys, _ := h.cfgStore.Load(context.Background())
	cs := sys.Config.CloudServices[0]
	cs.SuspectGraceDays = 3
	_ = h.cfgStore.PutCloudService(context.Background(), cs)

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 100 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "grace-%d", i),
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

	// Delete blobs.
	_ = h.cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		return h.cloudStore.Delete(context.Background(), info.Key)
	})

	// Day 0: reconcile marks suspect.
	leaderNode.orch.reconcileSweepAll()
	metasDay0, _ := leaderCM.List()
	if len(metasDay0) == 0 {
		t.Fatal("chunks should not be removed on day 0")
	}

	// Day 2 (within grace): still not removed.
	leaderNode.orch.now = func() time.Time { return time.Now().Add(48 * time.Hour) }
	leaderNode.orch.reconcileSweepAll()
	metasDay2, _ := leaderCM.List()
	if len(metasDay2) == 0 {
		t.Fatal("chunks should not be removed within grace period (day 2 of 3)")
	}

	// Day 4 (past grace): removed.
	leaderNode.orch.now = func() time.Time { return time.Now().Add(96 * time.Hour) }
	leaderNode.orch.reconcileSweepAll()
	metasDay4, _ := leaderCM.List()
	cloudRemaining := 0
	for _, m := range metasDay4 {
		if m.CloudBacked {
			cloudRemaining++
		}
	}
	if cloudRemaining > 0 {
		t.Errorf("expected cloud chunks removed after grace period, %d remain", cloudRemaining)
	}
}

func TestCloudClusterArchivalSurvivesRestart(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, []system.CloudStorageTransition{
		{After: "1d", StorageClass: "cold"},
	})

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "restart-%d", i),
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
	archivedBefore := 0
	for _, m := range metasArchived {
		if m.Archived {
			archivedBefore++
		}
	}
	if archivedBefore == 0 {
		t.Fatal("expected archived chunks before restart")
	}

	// Simulate restart: close and reopen the chunk manager with the same directory.
	dir := leaderNode.tierDirs[0]
	_ = leaderCM.Close()

	cm2, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(100),
		CloudStore: h.cloudStore, VaultID: h.vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cm2.Close()

	// The archived flag should be persisted in the cloud B+ tree index.
	metasAfterRestart, _ := cm2.List()
	archivedAfter := 0
	for _, m := range metasAfterRestart {
		if m.Archived {
			archivedAfter++
		}
	}
	if archivedAfter != archivedBefore {
		t.Errorf("archived chunks before restart=%d, after=%d — flag not persisted", archivedBefore, archivedAfter)
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

// TestCloudClusterReconcileSkipsTombstoned verifies that reconcile ignores
// chunks our own retention just deleted. Without this, every retention
// sweep on a cloud tier produced a paired WARN per evicted chunk
// (cache/download fallback + reconcile/mark-suspect) and corresponding UI
// alerts — dozens per second. See gastrolog-2c96i.
func TestCloudClusterReconcileSkipsTombstoned(t *testing.T) {
	t.Parallel()
	h := setupCloudCluster(t, nil)

	leaderNode := h.nodes["leader"]
	leaderCM := leaderNode.tiers[0].Chunks.(*chunkfile.Manager)

	// Produce some sealed + uploaded cloud chunks.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = leaderNode.orch.AppendToTier(h.vaultID, h.tierIDs[0], chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "reconcile-tombstone-%d", i),
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
	metasBefore, _ := leaderCM.List()
	if len(metasBefore) == 0 {
		t.Fatal("setup produced no chunks")
	}

	// Simulate what retention looks like from reconcile's point of view:
	// the blobs are gone from cloud AND the tier FSM has tombstoned every
	// chunk. The local cloud index still carries stale entries (that's
	// the window reconcile historically noticed).
	_ = h.cloudStore.List(context.Background(), "", func(info blobstore.BlobInfo) error {
		return h.cloudStore.Delete(context.Background(), info.Key)
	})
	tombstoned := make(map[chunk.ChunkID]bool, len(metasBefore))
	for _, m := range metasBefore {
		tombstoned[m.ID] = true
	}
	leaderNode.tiers[0].IsTombstoned = func(id chunk.ChunkID) bool {
		return tombstoned[id]
	}

	// Reconcile: must skip every tombstoned chunk silently — no suspects,
	// no alerts, no log spam.
	leaderNode.orch.reconcileSweepAll()

	if leaderNode.orch.suspects != nil {
		for _, m := range metasBefore {
			if _, ok := leaderNode.orch.suspects.suspectSince(m.ID); ok {
				t.Errorf("tombstoned chunk %s should not be marked suspect by reconcile", m.ID)
			}
		}
	}
}

// --- Warm cache cluster test ---

func TestCloudClusterCachePopulatedAfterUpload(t *testing.T) {
	t.Parallel()

	// Create a cloud cluster where the leader has a cache dir.
	vaultID := glid.New()
	tierID := glid.New()
	csID := glid.New()
	nodeID := "leader"

	cloudStore := blobstore.NewMemory()
	cacheDir := t.TempDir()

	store := sysmem.NewStore()
	_ = store.PutTier(context.Background(), system.TierConfig{
		ID: tierID, Name: "cloud", Type: system.TierTypeCloud, CloudServiceID: &csID,
		VaultID: vaultID, Position: 0,
	})
	_ = store.SetTierPlacements(context.Background(), tierID, []system.TierPlacement{{StorageID: system.SyntheticStorageID("test-node"), Leader: true}})
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "cache-test",
	})
	_ = store.PutCloudService(context.Background(), system.CloudService{
		ID: csID, Name: "test-cloud", Provider: "memory",
	})

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(100),
		CloudStore: cloudStore, VaultID: vaultID, CacheDir: cacheDir,
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)

	orch := newTestOrch(t, Config{
		LocalNodeID:  nodeID,
		SystemLoader: &transitionSystemLoader{store: store},
	})
	_ = orch.Scheduler().Stop()

	tier := &TierInstance{
		TierID: tierID, Type: "cloud",
		Chunks: cm, Indexes: im, Query: query.New(cm, im, nil),
	}
	orch.RegisterVault(NewVault(vaultID, tier))

	t.Cleanup(func() { orch.Stop(); _ = cm.Close() })

	// Ingest, seal, upload.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 500 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "cache-cluster-%d", i),
		})
	}
	if active := cm.Active(); active != nil && active.RecordCount > 0 {
		_ = cm.Seal()
	}
	metas, _ := cm.List()
	for _, m := range metas {
		if m.Sealed {
			_ = cm.PostSealProcess(context.Background(), m.ID)
		}
	}

	// Verify cache files exist.
	metasAfter, _ := cm.List()
	for _, m := range metasAfter {
		if !m.CloudBacked {
			continue
		}
		cachePath := filepath.Join(cacheDir, m.ID.String()+".glcb")
		info, err := os.Stat(cachePath)
		if err != nil {
			t.Errorf("chunk %s: expected cache file at %s", m.ID, cachePath)
			continue
		}
		if info.Size() == 0 {
			t.Errorf("chunk %s: cache file is empty", m.ID)
		}
	}

	// Read records via cursor — should hit cache, not cloud.
	for _, m := range metasAfter {
		if !m.CloudBacked {
			continue
		}
		cursor, err := cm.OpenCursor(m.ID)
		if err != nil {
			t.Errorf("OpenCursor(%s): %v", m.ID, err)
			continue
		}
		var count int
		for {
			_, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				_ = cursor.Close()
				t.Errorf("chunk %s record %d: %v", m.ID, count, err)
				break
			}
			count++
		}
		_ = cursor.Close()
		if int64(count) != m.RecordCount {
			t.Errorf("chunk %s: read %d records, meta says %d", m.ID, count, m.RecordCount)
		}
	}

	// Delete a cloud chunk — cache file should also be removed.
	if len(metasAfter) > 0 {
		deleteID := metasAfter[0].ID
		cachePath := filepath.Join(cacheDir, deleteID.String()+".glcb")
		if err := cm.Delete(deleteID); err != nil {
			t.Fatalf("Delete(%s): %v", deleteID, err)
		}
		if _, err := os.Stat(cachePath); !os.IsNotExist(err) {
			t.Errorf("cache file should be removed after Delete: %v", err)
		}
	}
}

// --- Cache eviction orchestrator tests ---

func TestCacheEvictionViaRetentionSweep(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	tierID := glid.New()
	csID := glid.New()
	nodeID := "leader"

	cloudStore := blobstore.NewMemory()
	cacheDir := t.TempDir()

	store := sysmem.NewStore()
	_ = store.PutTier(context.Background(), system.TierConfig{
		ID: tierID, Name: "cloud", Type: system.TierTypeCloud, CloudServiceID: &csID,
		VaultID: vaultID, Position: 0,
	})
	_ = store.SetTierPlacements(context.Background(), tierID, []system.TierPlacement{{StorageID: system.SyntheticStorageID("test-node"), Leader: true}})
	_ = store.PutVault(context.Background(), system.VaultConfig{
		ID: vaultID, Name: "eviction-test",
	})
	_ = store.PutCloudService(context.Background(), system.CloudService{
		ID: csID, Name: "test-cloud", Provider: "memory",
	})

	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(50),
		CloudStore: cloudStore, VaultID: vaultID,
		CacheDir: cacheDir, CacheEviction: "lru", CacheBudget: "1", // tiny budget
	})
	if err != nil {
		t.Fatal(err)
	}
	im := indexfile.NewManager(dir, nil, nil)

	orch := newTestOrch(t, Config{
		LocalNodeID:  nodeID,
		SystemLoader: &transitionSystemLoader{store: store},
	})

	tier := &TierInstance{
		TierID: tierID, Type: "cloud",
		Chunks: cm, Indexes: im, Query: query.New(cm, im, nil),
	}
	orch.RegisterVault(NewVault(vaultID, tier))

	// Ingest, seal, upload — populates cache.
	t0 := time.Date(2025, 6, 15, 10, 0, 0, 0, time.UTC)
	for i := range 200 {
		ts := t0.Add(time.Duration(i) * time.Microsecond)
		_ = orch.AppendToTier(vaultID, tierID, chunk.ChunkID{}, chunk.Record{
			IngestTS: ts, WriteTS: ts, Raw: fmt.Appendf(nil, "evict-%d", i),
		})
	}
	if active := cm.Active(); active != nil && active.RecordCount > 0 {
		_ = cm.Seal()
	}
	metas, _ := cm.List()
	for _, m := range metas {
		if m.Sealed {
			_ = cm.PostSealProcess(context.Background(), m.ID)
		}
	}

	// Verify cache has files.
	cacheEntries, _ := os.ReadDir(cacheDir)
	var glcbBefore int
	for _, e := range cacheEntries {
		if filepath.Ext(e.Name()) == ".glcb" {
			glcbBefore++
		}
	}
	if glcbBefore == 0 {
		t.Fatal("expected cache files after upload")
	}

	// Run retention sweep — triggers cache eviction.
	orch.retentionSweepAll()

	// With budget=1 byte, all cached files should be evicted.
	cacheEntries, _ = os.ReadDir(cacheDir)
	var glcbAfter int
	for _, e := range cacheEntries {
		if filepath.Ext(e.Name()) == ".glcb" {
			glcbAfter++
		}
	}
	if glcbAfter >= glcbBefore {
		t.Errorf("expected eviction to reduce cache files: before=%d, after=%d", glcbBefore, glcbAfter)
	}
}
