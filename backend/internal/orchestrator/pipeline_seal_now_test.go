package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/system"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// An operator's seal commits the open manifest ahead of policy — on the
// chunking leader only, and only when there is something to seal. A follower
// home asked to seal a manifest that holds records refuses with ErrNotLeader
// instead of reporting that nothing needed sealing.
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
	if sealed, err := follower.SealOpenManifest(vaultID); err != nil || sealed {
		t.Fatalf("follower seal with no open manifest = (%v, %v), want (false, nil)", sealed, err)
	}

	planUntilOpenRef(t, ctx, leader, fsm, vaultID)
	if open := fsm.OpenChunk(); open == nil || open.TotalRecords == 0 {
		t.Fatal("premise: the planner left an open manifest with records")
	}

	if sealed, err := follower.SealOpenManifest(vaultID); !errors.Is(err, chunking.ErrNotLeader) || sealed {
		t.Fatalf("follower seal of a manifest holding records = (%v, %v), want (false, ErrNotLeader)", sealed, err)
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

// A node that holds a vault instance but does not chunk for the vault cannot
// seal its open manifest. It reports nothing to seal while the manifest is
// empty, and refuses with ErrNotChunkingLeader once the manifest holds
// records — never a zero that reads as "nothing was open".
func TestSealActiveOnNodeThatDoesNotChunkRefusesWhenManifestHoldsRecords(t *testing.T) {
	vaultID := glid.New()
	sys := &system.System{Config: system.Config{
		Vaults: []system.VaultConfig{{ID: vaultID, Name: "elsewhere", Type: system.VaultTypeFile, Enabled: true}},
		Routes: []system.RouteConfig{{
			ID: glid.New(), Name: "all", Priority: 10, Enabled: true,
			Stages:       []system.RouteStage{{Match: &system.MatchStage{Expression: "*"}}},
			Destinations: []glid.GLID{vaultID},
		}},
	}}
	orch := newTestOrch(t, Config{LocalNodeID: "node-local", SystemLoader: &staticSystemLoader{sys: sys}})
	orch.groupMgr = singleNodeVaultCtlGroup(t, "node-local", vaultID)
	if err := orch.ReloadFilters(context.Background()); err != nil {
		t.Fatalf("ReloadFilters: %v", err)
	}
	if !orch.isPipelineIngestVault(vaultID) {
		t.Fatal("premise: the vault is registered with the pipeline")
	}
	orch.RegisterVault(NewVault(vaultID, newMemoryInstance(t, vaultID)))

	if sealed, err := orch.SealActive(vaultID); err != nil || sealed != 0 {
		t.Fatalf("SealActive with nothing open = (%d, %v), want (0, nil)", sealed, err)
	}

	fsm, applier, _, ok := orch.vaultCtlHandle(vaultID)
	if !ok {
		t.Fatal("premise: no vault-ctl handle")
	}
	segID := glid.New()
	chunkID := chunk.NewChunkID()
	now := time.Now()
	for _, data := range [][]byte{
		vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
			SegmentID: segID, RecordCount: 3, OriginNodeID: "node-local",
		}),
		vaultctlfsm.MarshalOpenChunkManifest(chunkID, now),
		vaultctlfsm.MarshalAddOpenChunkSegmentRef(chunkID, vaultctlfsm.OpenChunkSegmentRef{
			SegmentID: segID, FirstRecordNumber: 0, LastRecordNumber: 2, SliceBytes: 3, RefAddedAt: now,
		}),
	} {
		if err := applier.Apply(data); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if open := fsm.OpenChunk(); open == nil || open.TotalRecords != 3 {
		t.Fatalf("premise: open manifest = %+v, want 3 records", open)
	}

	sealed, err := orch.SealActive(vaultID)
	if !errors.Is(err, ErrNotChunkingLeader) || sealed != 0 {
		t.Fatalf("SealActive with records open on a node that does not chunk = (%d, %v), want (0, ErrNotChunkingLeader)", sealed, err)
	}
	if open := fsm.OpenChunk(); open == nil || open.ChunkID != chunkID {
		t.Fatal("a refused seal changed the open manifest")
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
