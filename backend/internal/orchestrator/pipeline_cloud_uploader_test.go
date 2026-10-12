package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// newPlacementStore returns a config store holding the vault and no placement.
func newPlacementStore(t *testing.T, vaultID glid.GLID) *sysmem.Store {
	t.Helper()
	cfg := sysmem.NewStore()
	if err := cfg.PutVault(context.Background(), system.VaultConfig{ID: vaultID, Name: "cloud", Type: system.VaultTypeFile}); err != nil {
		t.Fatal(err)
	}
	return cfg
}

// setPlacementLeader places the vault on leader and followers, each on its
// node's synthetic storage.
func setPlacementLeader(t *testing.T, cfg *sysmem.Store, vaultID glid.GLID, leader string, followers ...string) {
	t.Helper()
	placements := []system.VaultPlacement{{StorageID: system.SyntheticStoragePrefix + leader, Leader: true}}
	for _, f := range followers {
		placements = append(placements, system.VaultPlacement{StorageID: system.SyntheticStoragePrefix + f})
	}
	if err := cfg.SetVaultPlacements(context.Background(), vaultID, placements); err != nil {
		t.Fatal(err)
	}
}

// registerCloudMockVault registers a pipeline vault on orch whose chunk
// manager is a cloud-wired mock holding one sealed chunk.
func registerCloudMockVault(t *testing.T, orch *Orchestrator, vaultID glid.GLID) (*mockCloudBackedChunkManager, chunk.ChunkID) {
	t.Helper()
	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{chunks: []chunk.ChunkMeta{{ID: chunkID, Sealed: true}}}
	mock.cloudStoreConfigured.Store(true)
	markPipelineIngestVault(t, orch, vaultID, true)
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{VaultID: vaultID, Type: "file", Chunks: mock}))
	return mock, chunkID
}

// A vault built before any placement existed has no cloud uploader: no node
// uploads until a placement names one, and then only that node does.
func TestSchedulePipelineCloudUpload_UnplacedVaultHasNoUploader(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cfg := newPlacementStore(t, vaultID)
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", SystemLoader: cfg})
	mock, chunkID := registerCloudMockVault(t, orch, vaultID)

	orch.schedulePipelineCloudUpload(vaultID, chunkID)
	if n := settledUploadCount(t, orch, mock); n != 0 {
		t.Fatalf("unplaced vault: %d uploads, want 0", n)
	}

	setPlacementLeader(t, cfg, vaultID, "node-A", "node-B")
	orch.schedulePipelineCloudUpload(vaultID, chunkID)
	if n := settledUploadCount(t, orch, mock); n != 1 {
		t.Fatalf("placement leader: %d uploads, want 1", n)
	}
}

// A follower promoted to placement leader uploads: the role is read when the
// upload is decided, not when the vault instance was built.
func TestSchedulePipelineCloudUpload_PromotedFollowerUploads(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cfg := newPlacementStore(t, vaultID)
	setPlacementLeader(t, cfg, vaultID, "node-A", "node-B")
	orch := newTestOrch(t, Config{LocalNodeID: "node-B", SystemLoader: cfg})
	mock, chunkID := registerCloudMockVault(t, orch, vaultID)

	orch.schedulePipelineCloudUpload(vaultID, chunkID)
	if n := settledUploadCount(t, orch, mock); n != 0 {
		t.Fatalf("follower: %d uploads, want 0", n)
	}

	setPlacementLeader(t, cfg, vaultID, "node-B", "node-A")
	orch.schedulePipelineCloudUpload(vaultID, chunkID)
	if n := settledUploadCount(t, orch, mock); n != 1 {
		t.Fatalf("promoted follower: %d uploads, want 1", n)
	}
}

// occupyScheduler fills a one-slot scheduler with a job that runs until the
// returned release is called, so jobs scheduled meanwhile queue behind it.
func occupyScheduler(t *testing.T, orch *Orchestrator) (release func()) {
	t.Helper()
	started := make(chan struct{})
	done := make(chan struct{})
	if err := orch.Scheduler().RunOnce("occupy", func() {
		close(started)
		<-done
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	return func() { close(done) }
}

// An upload queued while this node was the placement leader, still queued when
// the role moves to another node, is left to the new leader.
func TestSchedulePipelineCloudUpload_QueuedUploadYieldsToNewLeader(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cfg := newPlacementStore(t, vaultID)
	setPlacementLeader(t, cfg, vaultID, "node-A", "node-B")
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", SystemLoader: cfg, MaxConcurrentJobs: 1})
	mock, chunkID := registerCloudMockVault(t, orch, vaultID)

	release := occupyScheduler(t, orch)
	orch.schedulePipelineCloudUpload(vaultID, chunkID)
	if !orch.Scheduler().HasJob(cloudUploadJobName(vaultID, chunkID)) {
		t.Fatal("upload not queued while this node was the placement leader")
	}
	setPlacementLeader(t, cfg, vaultID, "node-B", "node-A")
	release()

	if n := settledUploadCount(t, orch, mock); n != 0 {
		t.Fatalf("former leader ran its queued upload: %d uploads, want 0", n)
	}
}

// The catch-up sweep's queued uploads yield to a new leader the same way.
func TestBackfillCloudUploads_QueuedUploadYieldsToNewLeader(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cfg := newPlacementStore(t, vaultID)
	setPlacementLeader(t, cfg, vaultID, "node-A", "node-B")
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", SystemLoader: cfg, MaxConcurrentJobs: 1})
	mock, chunkID := registerCloudMockVault(t, orch, vaultID)

	release := occupyScheduler(t, orch)
	orch.cloudUploadCatchupForVault(vaultID)
	if !orch.Scheduler().HasJob(cloudUploadJobName(vaultID, chunkID)) {
		t.Fatal("catch-up upload not queued while this node was the placement leader")
	}
	setPlacementLeader(t, cfg, vaultID, "node-B", "node-A")
	release()

	if n := settledUploadCount(t, orch, mock); n != 0 {
		t.Fatalf("former leader ran its queued catch-up upload: %d uploads, want 0", n)
	}
}

// refusingUploader is a chunk manager whose upload finds this node is no
// longer the cloud uploader by the time it would write the object.
type refusingUploader struct {
	*mockCloudBackedChunkManager
}

func (r refusingUploader) UploadToCloud(id chunk.ChunkID) error {
	_ = r.mockCloudBackedChunkManager.UploadToCloud(id)
	return chunk.ErrNotCloudUploader
}

// An upload refused because the role moved mid-upload is not a failure: it
// neither backs off nor raises the stuck-upload alarm.
func TestBackfillCloudUploads_LostUploaderRoleIsNotAFailure(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	mock := &mockCloudBackedChunkManager{}
	chunkID := chunk.NewChunkID()
	mock.chunks = []chunk.ChunkMeta{{ID: chunkID, Sealed: true}}
	mock.cloudStoreConfigured.Store(true)
	vaultInst := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: refusingUploader{mock}}
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	orch.backfillCloudUploads(vaultInst)
	if n := settledUploadCount(t, orch, mock); n != 1 {
		t.Fatalf("upload attempts = %d, want 1", n)
	}
	if orch.vaultHasBackfillFailures(vaultID) {
		t.Fatal("a refused upload was recorded as a backfill failure")
	}
}

// writeLandedGLCB builds a real GLCB for one sealed chunk and places it on the
// vault's pipeline chunk root, returning the chunk's sealed manifest entry and
// the FSM holding it.
func writeLandedGLCB(t *testing.T, orch *Orchestrator, vaultID glid.GLID) (vaultctlfsm.ManifestEntry, *vaultctlfsm.FSM) {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Microsecond)
	scratchDir := t.TempDir()
	scratch, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            scratchDir,
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(100),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = scratch.Close() }()
	for i := range 8 {
		ts := now.Add(time.Duration(i) * time.Millisecond)
		if _, _, err := scratch.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte("landed")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := scratch.Seal(); err != nil {
		t.Fatal(err)
	}
	metas, err := scratch.List()
	if err != nil {
		t.Fatal(err)
	}
	var id chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			id = m.ID
		}
	}
	if err := scratch.PostSealProcess(context.Background(), id); err != nil {
		t.Fatal(err)
	}

	fsm := vaultctlfsm.New()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 8, 512, now, now, now, false, now, [32]byte{})}); err != nil {
		t.Fatal(err)
	}

	root, ok := orch.pipelineVaultChunkRoot(vaultID)
	if !ok {
		t.Fatal("vault has no pipeline chunk root on this node")
	}
	dst := chunking.ChunkGLCBPath(root, id)
	if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(filepath.Join(scratchDir, id.String(), "data.glcb"), dst); err != nil {
		t.Fatal(err)
	}
	return *fsm.Get(id), fsm
}

// The seal of a chunk whose GLCB this home has not built yet schedules no
// upload — an upload then could only fail "chunk not found". The upload is
// scheduled when the copy lands.
func TestCloudUploadWaitsForLocalCopy(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	cfg := newPlacementStore(t, vaultID)
	setPlacementLeader(t, cfg, vaultID, "node-A", "node-B")
	orch := newTestOrch(t, Config{LocalNodeID: "node-A", SystemLoader: cfg})
	markPipelineIngestVault(t, orch, vaultID, true)

	mock := &mockCloudBackedChunkManager{}
	mock.cloudStoreConfigured.Store(true)
	root, ok := orch.pipelineVaultChunkRoot(vaultID)
	if !ok {
		t.Fatal("vault has no pipeline chunk root on this node")
	}
	entry, fsm := writeLandedGLCB(t, orch, vaultID)
	glcbPath := chunking.ChunkGLCBPath(root, entry.ID)
	built := glcbPath + ".built"
	if err := os.Rename(glcbPath, built); err != nil {
		t.Fatal(err)
	}

	vaultInst := &VaultInstance{
		VaultID: vaultID,
		Type:    "file",
		Chunks:  mock,
		ManifestReadFacet: ManifestReadFacet{
			ManifestEntry: func(id chunk.ChunkID) (vaultctlfsm.ManifestEntry, bool) {
				e := fsm.Get(id)
				if e == nil {
					return vaultctlfsm.ManifestEntry{}, false
				}
				return *e, true
			},
		},
	}
	vaultInst.Reconciler = NewVaultLifecycleReconciler(orch, vaultID, vaultInst, "node-A", orch.baseLogger)
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	vaultInst.Reconciler.onSeal(entry)
	if n := settledUploadCount(t, orch, mock); n != 0 {
		t.Fatalf("seal before the local build: %d uploads, want 0", n)
	}

	if err := os.Rename(built, glcbPath); err != nil {
		t.Fatal(err)
	}
	orch.onGLCBLanded(vaultID, vaultInst, entry)
	if n := settledUploadCount(t, orch, mock); n != 1 {
		t.Fatalf("after the local copy landed: %d uploads, want 1", n)
	}
}
