package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// An operator's seal commits the open manifest ahead of policy — on the
// chunking leader only, and only when there is something to seal.
func TestSealOpenManifestSealsOnTheLeaderOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fsm := vaultctlfsm.New()
	vaultID := glid.New()
	origin := newOriginFixture(t, ctx, vaultID, fsm)
	segID := origin.ingestAndPublish(t, ctx)

	followerHome := t.TempDir()
	copyCompletedToHead(t, origin.root, followerHome, segID)
	follower := chunking.New(chunking.Config{})
	if err := follower.RegisterVault(vaultID, chunkingSpec(followerHome, fsm, func() bool { return false })); err != nil {
		t.Fatalf("follower RegisterVault: %v", err)
	}

	leaderHome := t.TempDir()
	copyCompletedToHead(t, origin.root, leaderHome, segID)
	leader := chunking.New(chunking.Config{})
	if err := leader.RegisterVault(vaultID, chunkingSpec(leaderHome, fsm, func() bool { return true })); err != nil {
		t.Fatalf("leader RegisterVault: %v", err)
	}

	// Nothing open yet: nothing to seal, on either side.
	if sealed, err := leader.SealOpenManifest(vaultID); err != nil || sealed {
		t.Fatalf("seal with no open manifest = (%v, %v), want (false, nil)", sealed, err)
	}

	planUntilOpenRef(t, ctx, leader, fsm, vaultID)
	if open := fsm.OpenChunk(); open == nil || open.TotalRecords == 0 {
		t.Fatal("premise: the planner left an open manifest with records")
	}

	if sealed, err := follower.SealOpenManifest(vaultID); err != nil || sealed {
		t.Fatalf("follower seal = (%v, %v), want (false, nil)", sealed, err)
	}
	if fsm.SealedManifest() != nil {
		t.Fatal("a follower sealed the manifest")
	}

	sealed, err := leader.SealOpenManifest(vaultID)
	if err != nil || !sealed {
		t.Fatalf("leader seal = (%v, %v), want (true, nil)", sealed, err)
	}
	if fsm.SealedManifest() == nil || fsm.OpenChunk() != nil {
		t.Fatal("leader seal did not move the open manifest to sealed")
	}
	if _, err := chunking.New(chunking.Config{}).SealOpenManifest(vaultID); err == nil {
		t.Fatal("an unknown vault must report ErrUnknownVault")
	}
}

// pipelineSealHost is the reconciler's view of an orchestrator for a pipeline
// vault; only the methods the seal path reaches are implemented.
type pipelineSealHost struct {
	reconcilerHost
}

func (pipelineSealHost) isPipelineIngestVault(glid.GLID) bool                 { return true }
func (pipelineSealHost) pipelineVaultChunkRoot(glid.GLID) (string, bool)      { return "", false }
func (pipelineSealHost) schedulePipelineCloudUpload(glid.GLID, chunk.ChunkID) {}
func (pipelineSealHost) EmitChunkSealed(glid.GLID, chunk.ChunkMeta)           {}
func (pipelineSealHost) alertSink() alert.Sink                                { return nil }

// A pipeline vault has no legacy active file to project the seal onto, so
// the reconciler leaves the chunk manager alone instead of failing loudly on
// every seal from every node that does not home the vault.
func TestReconcilerOnSealSkipsLegacyProjectionForPipelineVaults(t *testing.T) {
	t.Parallel()
	fsm := vaultctlfsm.New()
	cm := &reconcilerFakeSealEnsurerChunkManager{}
	vaultInst := &VaultInstance{VaultID: glid.New(), Chunks: cm}
	rec := NewVaultLifecycleReconciler(pipelineSealHost{}, vaultInst.VaultID, vaultInst, "node-A", slog.Default())
	rec.Wire(fsm)

	id := chunk.NewChunkID()
	now := time.Now()
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalCreateChunk(id, now, now, now)}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalSealChunk(id, now, 100, 1234, now, now, now, false, now)}); err != nil {
		t.Fatalf("seal: %v", err)
	}
	if len(cm.ensured) != 0 {
		t.Fatalf("EnsureSealed was projected onto a pipeline vault's chunk manager: %v", cm.ensured)
	}
}
