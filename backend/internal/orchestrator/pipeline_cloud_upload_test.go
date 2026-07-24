package orchestrator

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// TestSchedulePipelineCloudUpload_LeaderUploadsExternalGLCB verifies a pipeline
// vault leader uploads a sealed GLCB registered via RegisterExternalGLCB.
func TestSchedulePipelineCloudUpload_LeaderUploadsExternalGLCB(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	now := time.Now().UTC().Truncate(time.Microsecond)

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	markPipelineIngestVault(t, orch, vaultID, true)

	// Build a valid data.glcb in a scratch manager, then place it on the pipeline ChunkRoot.
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
		if _, _, err := scratch.Append(chunk.Record{IngestTS: ts, WriteTS: ts, Raw: []byte("pipeline-cloud")}); err != nil {
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
	var sealedID chunk.ChunkID
	for _, m := range metas {
		if m.Sealed {
			sealedID = m.ID
			break
		}
	}
	if sealedID == (chunk.ChunkID{}) {
		t.Fatal("no sealed chunk")
	}

	fsm := vaultctlfsm.New()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(sealedID, now, now, now)}); err != nil {
		t.Fatal(err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(sealedID, now, 8, 512, now, now, now, false, now)}); err != nil {
		t.Fatal(err)
	}

	if err := scratch.PostSealProcess(context.Background(), sealedID); err != nil {
		t.Fatal(err)
	}
	scratchGLCB := filepath.Join(scratchDir, sealedID.String(), "data.glcb")
	if _, err := os.Stat(scratchGLCB); err != nil {
		t.Fatal(err)
	}

	chunkRoot := filepath.Join(orch.segmentsDir, vaultID.String(), "chunks")
	pipelineGLCB := chunking.ChunkGLCBPath(chunkRoot, sealedID)
	if err := os.MkdirAll(filepath.Dir(pipelineGLCB), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := copyFile(scratchGLCB, pipelineGLCB); err != nil {
		t.Fatal(err)
	}

	store := blobstore.NewMemory()
	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
		CloudStore:     store,
		VaultID:        vaultID,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cm.Close() }()

	if err := cm.RegisterExternalGLCB(sealedID, pipelineGLCB, chunk.ExternalGLCBInfo{
		WriteStart:  now,
		WriteEnd:    now,
		IngestStart: now,
		IngestEnd:   now,
		RecordCount: 8,
		Bytes:       512,
		DiskBytes:   512,
	}); err != nil {
		t.Fatal(err)
	}

	vaultInst := &VaultInstance{
		VaultID: vaultID,
		Type:    "file",
		Chunks:  cm,
		RaftLeadershipFacet: RaftLeadershipFacet{
			IsRaftLeader: func() bool { return false },
		},
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
	orch.RegisterVault(NewVault(vaultID, vaultInst))

	orch.schedulePipelineCloudUpload(vaultID, sealedID)

	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()

	deadline := time.Now().Add(5 * time.Second)
	var blobCount int
	for time.Now().Before(deadline) {
		blobCount = 0
		_ = store.List(context.Background(), "", func(blobstore.BlobInfo) error {
			blobCount++
			return nil
		})
		if blobCount > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if blobCount == 0 {
		t.Fatal("expected blob in cloud store after pipeline upload")
	}
}

// TestSchedulePipelineCloudUpload_SkipsPlacementFollower verifies nodes without
// cloud write access do not upload.
func TestSchedulePipelineCloudUpload_SkipsPlacementFollower(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	mock := &mockCloudBackedChunkManager{
		chunks: []chunk.ChunkMeta{{ID: chunkID, Sealed: true}},
	}
	mock.cloudStoreConfigured.Store(false)

	orch := newTestOrch(t, Config{LocalNodeID: "follower"})
	markPipelineIngestVault(t, orch, vaultID, true)
	orch.RegisterVault(NewVault(vaultID, &VaultInstance{
		VaultID: vaultID,
		Type:    "file",
		Chunks:  mock,
		RaftLeadershipFacet: RaftLeadershipFacet{
			IsRaftLeader: func() bool { return true },
		},
	}))

	orch.schedulePipelineCloudUpload(vaultID, chunkID)
	orch.Scheduler().Start()
	defer func() { _ = orch.Scheduler().Stop() }()
	time.Sleep(100 * time.Millisecond)

	if mock.uploadCallCount() != 0 {
		t.Fatalf("placement follower uploaded %d chunks, want 0", mock.uploadCallCount())
	}
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Close()
}
