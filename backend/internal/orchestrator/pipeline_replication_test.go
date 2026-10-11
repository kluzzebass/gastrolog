package orchestrator

import (
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
	o.setPipelineVaultLocked(vaultID, pipelineVaultReg{home: home, hasHandle: true})
	o.mu.Unlock()
}

// TestSweepMissingReplicas_PipelineVault_ResolvesGLCBLazily verifies that a
// sealed chunk with its GLCB on disk resolves LAZILY at first chunk-manager
// lookup via the on-miss resolver — the sweep registers nothing.
func TestSweepMissingReplicas_PipelineVault_ResolvesGLCBLazily(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	chunkID := chunk.NewChunkID()
	now := time.Now()

	fsm := vaultctlfsm.New()
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(chunkID, now, now, now)})
	_ = fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(chunkID, now, 100, 4096, now, now, now, false, now, [32]byte{})})

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

	vaultInst := &VaultInstance{VaultID: vaultID, Type: "file", Chunks: cm}
	rec := NewVaultLifecycleReconciler(orch, vaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)
	installGLCBResolverOn(vaultInst, func() (*vaultctlfsm.FSM, string, bool) { return fsm, chunkRoot, true })

	rec.SweepMissingReplicas()

	if _, err := cm.Meta(chunkID); err != nil {
		t.Fatalf("sealed on-disk GLCB did not lazily resolve at lookup: %v", err)
	}
}

// TestSchedulePostSeal_PipelineVault_SchedulesPostSeal verifies that pipeline
// registration does not suppress the post-seal pipeline of a chunk-manager
// chunk: Seal() announces only Active → Sealing and the matching AnnounceSeal
// lives in PostSealProcess, so skipping the post-seal job parks the manifest
// entry in Sealing forever.
func TestSchedulePostSeal_PipelineVault_SchedulesPostSeal(t *testing.T) {
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
	}))

	orch.schedulePostSeal(vaultID, cm, chunkID)

	postSealJob := "post-seal:" + vaultID.String() + ":" + chunkID.String()
	var sawPostSeal bool
	for _, job := range orch.scheduler.ListJobs() {
		if job.Name == postSealJob {
			sawPostSeal = true
		}
	}
	if !sawPostSeal {
		t.Fatalf("post-seal job %q not scheduled; the Sealing → Sealed announce rides it", postSealJob)
	}
}
