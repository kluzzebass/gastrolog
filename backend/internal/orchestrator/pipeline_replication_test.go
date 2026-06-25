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
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

func markPipelineIngestVault(t *testing.T, o *Orchestrator, vaultID glid.GLID, home bool) {
	t.Helper()
	o.mu.Lock()
	o.pipelineVaults[vaultID] = pipelineVaultReg{home: home, hasHandle: true}
	o.mu.Unlock()
}

// TestSweepMissingReplicas_PipelineVault_SyncsGLCBNotCatchup verifies pipeline
// ingest vaults register local pipeline GLCBs instead of requesting record-stream
// catchup from peers.
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

	rec.SweepMissingReplicas()

	if fake.calls.Load() != 0 {
		t.Fatalf("RequestReplicaCatchup calls = %d, want 0 for pipeline vault", fake.calls.Load())
	}
	if _, err := cm.Meta(chunkID); err != nil {
		t.Fatalf("chunk not registered after sync sweep: %v", err)
	}
}

// TestSchedulePostSeal_PipelineVault_NoJob verifies the legacy post-seal
// scheduler is not used for pipeline ingest vaults.
func TestSchedulePostSeal_PipelineVault_NoJob(t *testing.T) {
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

	orch.schedulePostSeal(vaultID, cm, chunkID)

	for _, job := range orch.scheduler.ListJobs() {
		if job.Name == "post-seal:"+vaultID.String()+":"+chunkID.String() {
			t.Fatalf("post-seal job scheduled for pipeline vault: %+v", job)
		}
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
