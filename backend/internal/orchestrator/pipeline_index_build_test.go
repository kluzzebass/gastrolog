package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	chunkfile "gastrolog/internal/chunk/file"
	"gastrolog/internal/index"
	indexfile "gastrolog/internal/index/file"
	fileattr "gastrolog/internal/index/file/attr"
	filejson "gastrolog/internal/index/file/json"
	filekv "gastrolog/internal/index/file/kv"
	filetoken "gastrolog/internal/index/file/token"
	"gastrolog/internal/query"
	"gastrolog/internal/record"
	"gastrolog/internal/waittest"
)

// pipelineVaultWithSealedGLCB registers, on a fresh orchestrator, a file vault
// that already holds one pipeline-built sealed GLCB and a real index manager,
// mirroring a home node right after chunking finished a build.
func pipelineVaultWithSealedGLCB(t *testing.T, ctx context.Context, withBuilders bool) (*Orchestrator, sealedGLCBFixture, *chunkfile.Manager, index.IndexManager) {
	t.Helper()
	fx := buildSealedPipelineGLCB(t, ctx, 12, e1Payload, record.Attributes{"service": "api", "level": "info"})
	dir := t.TempDir()
	cm, err := chunkfile.NewManager(chunkfile.Config{Dir: dir, Now: time.Now, RotationPolicy: chunk.NewRecordCountPolicy(1000)})
	if err != nil {
		t.Fatalf("chunk manager: %v", err)
	}
	t.Cleanup(func() { _ = cm.Close() })
	im := indexfile.NewManager(dir, []index.Indexer{
		filetoken.NewIndexer(dir, cm, nil),
		fileattr.NewIndexer(dir, cm, nil),
		filekv.NewIndexer(dir, cm, nil),
		filejson.NewIndexer(dir, cm, nil),
	}, nil, cm)
	if withBuilders {
		cm.SetIndexBuilders([]chunk.ChunkIndexBuilder{im.BuildAdapter()})
	}
	if err := cm.RegisterExternalGLCB(fx.sealed.ID, fx.glcbPath, externalInfoFromEntry(fx.sealed)); err != nil {
		t.Fatalf("RegisterExternalGLCB: %v", err)
	}
	orch := newTestOrch(t, Config{LocalNodeID: "node-home"})
	orch.RegisterVault(NewVaultFromComponents(fx.vaultID, cm, im, query.New(cm, im, nil)))
	if complete, err := im.IndexesComplete(fx.sealed.ID); err != nil || complete {
		t.Fatalf("premise: a freshly built pipeline chunk has no secondary indexes yet (complete=%v err=%v)", complete, err)
	}
	return orch, fx, cm, im
}

func awaitIndexes(t *testing.T, orch *Orchestrator, im index.IndexManager, id chunk.ChunkID) {
	t.Helper()
	waittest.Progress(t, fmt.Sprintf("secondary indexes for %s built", id), func() (string, bool) {
		complete, err := im.IndexesComplete(id)
		return fmt.Sprintf("complete=%v err=%v %s", complete, err, jobsProgress(orch.Scheduler())), err == nil && complete
	})
}

// When chunking finishes a build on this node, the chunk's secondary indexes
// are built without waiting for a restart or an operator's reindex.
func TestPipelineChunkBuiltSchedulesSecondaryIndexes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	orch, fx, _, im := pipelineVaultWithSealedGLCB(t, ctx, false)

	vaultInst := orch.findLocalVaultInstance(fx.vaultID)
	vaultInst.Reconciler = NewVaultLifecycleReconciler(orch, fx.vaultID, vaultInst, "node-home", slog.Default())
	orch.onPipelineChunkBuilt(fx.vaultID, fx.fsm, fx.sealed.ID)
	awaitIndexes(t, orch, im, fx.sealed.ID)
}

// The startup sweep visits a vault only when its chunk manager carries index
// builders, which every file vault instance now has; with them the sealed
// pipeline chunk gets its indexes at startup.
func TestMissingIndexSweepBuildsPipelineChunkIndexes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	orchWithout, fxWithout, _, imWithout := pipelineVaultWithSealedGLCB(t, ctx, false)
	if err := orchWithout.rebuildVaultIndexes(ctx, fxWithout.vaultID, orchWithout.findLocalVaultInstance(fxWithout.vaultID)); err != nil {
		t.Fatal(err)
	}
	if complete, _ := imWithout.IndexesComplete(fxWithout.sealed.ID); complete {
		t.Fatal("premise: without index builders the sweep skips the vault")
	}

	orch, fx, _, im := pipelineVaultWithSealedGLCB(t, ctx, true)
	if err := orch.rebuildVaultIndexes(ctx, fx.vaultID, orch.findLocalVaultInstance(fx.vaultID)); err != nil {
		t.Fatal(err)
	}
	awaitIndexes(t, orch, im, fx.sealed.ID)
}
