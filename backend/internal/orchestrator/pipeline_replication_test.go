package orchestrator

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func markPipelineIngestVault(t *testing.T, o *Orchestrator, vaultID glid.GLID, home bool) {
	t.Helper()
	o.mu.Lock()
	o.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: home, hasHandle: true})
	o.mu.Unlock()
}

// TestSweepMissingReplicas_PipelineVault_SyncsGLCBNotCatchup verifies pipeline
// ingest vaults never request record-stream catchup from peers, and that a
// sealed chunk with its GLCB on disk resolves LAZILY at first chunk-manager
// lookup via the on-miss resolver — the sweep registers nothing.
func TestSweepMissingReplicas_PipelineVault_SyncsGLCBNotCatchup(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	now := time.Now()

	fsm := vaultctlfsm.New()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(chunkID, now, 100, 4096, now, now, now, false, now)})

	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	markPipelineIngestVault(t, orch, vaultID, true)

	chunkRoot := filepath.Join(orch.segmentsDir, vaultID.String(), "chunks")
	glcbPath := chunking.ChunkGLCBPath(chunkRoot, chunkID)
	if err := os.MkdirAll(filepath.Dir(glcbPath), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(glcbPath, []byte("pipeline-glcb"), 0o600); err != nil {
		t.Fatal(err)
	}

	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	fake := &captureCatchupReplicator{scheduledRet: 1}
	orch.SetChunkReplicator(fake)

	vaultInst := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: cm}
	rec := NewVaultLifecycleReconciler(orch, vaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)
	orch.installLazyGLCBResolverOn(vaultInst, vaultID, true, fsm, chunkRoot)

	rec.SweepMissingReplicas()

	if fake.calls.Load() != 0 {
		t.Fatalf("RequestReplicaCatchup calls = %d, want 0 for pipeline vault", fake.calls.Load())
	}
	if _, err := cm.Meta(chunkID); err != nil {
		t.Fatalf("sealed on-disk GLCB did not lazily resolve at lookup: %v", err)
	}
}

// TestSchedulePostSeal_PipelineVault_NoRecordStreamReplication verifies that
// pipeline registration suppresses the thing the pipeline actually replaces —
// follower record-stream replication — and nothing else. It must NOT suppress
// the post-seal pipeline: Seal() announces only Active → Sealing and the
// matching AnnounceSeal lives in PostSealProcess, so skipping the post-seal
// job parks the manifest entry in Sealing forever.
func TestSchedulePostSeal_PipelineVault_NoRecordStreamReplication(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	markPipelineIngestVault(t, orch, vaultID, true)

	cm, err := chunkfile.NewManager(chunkfile.Config{
		Dir:            t.TempDir(),
		Now:            time.Now,
		RotationPolicy: chunk.NewRecordCountPolicy(1000),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cm.Close() })

	orch.RegisterVault(NewVault(vaultID, &VaultInstance{
		VaultID: vaultID,
		Type:    "file",
		Chunks:  cm,
		FollowerTargets: []system.ReplicationTarget{
			{NodeID: "node-B", StorageID: "storage-B"},
		},
	}))

	orch.schedulePostSeal(vaultID, cm, chunkID)

	postSealJob := "post-seal:" + vaultID.String() + ":" + chunkID.String()
	replicateJob := "replicate:" + vaultID.String() + ":" + chunkID.String()
	var sawPostSeal bool
	for _, job := range orch.scheduler.ListJobs() {
		switch job.Name {
		case postSealJob:
			sawPostSeal = true
		case replicateJob:
			t.Fatalf("record-stream replication scheduled for pipeline vault: %+v", job)
		}
	}
	if !sawPostSeal {
		t.Fatalf("post-seal job %q not scheduled; the Sealing → Sealed announce rides it", postSealJob)
	}
}

// TestImportToInstanceStorage_PipelineVault_SkipsRecordStream verifies record
// streaming import is refused for pipeline ingest vaults.
func TestImportToInstanceStorage_PipelineVault_SkipsRecordStream(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	orch := newTestOrch(t, Config{LocalNodeID: "node-A"})
	markPipelineIngestVault(t, orch, vaultID, true)

	inst := newMemoryInstance(t, vaultID)
	orch.RegisterVault(&Vault{ID: vaultID, Instance: inst})

	iter := func() (chunk.Record, error) {
		return chunk.Record{}, chunk.ErrNoMoreRecords
	}

	if err := orch.ImportToInstanceStorage(context.Background(), vaultID, "", chunkID, iter); err != nil {
		t.Fatalf("ImportToInstanceStorage: %v", err)
	}
	if metas, err := inst.Chunks.List(); err != nil {
		t.Fatalf("List: %v", err)
	} else if len(metas) != 0 {
		t.Fatalf("expected no imported chunks, got %d", len(metas))
	}
}
