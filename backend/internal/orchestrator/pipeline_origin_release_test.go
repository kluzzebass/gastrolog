package orchestrator

import (
	"os"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator/pipeline"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"

	hraft "github.com/hashicorp/raft"
)

// The spec buildPipelineVaultSpec produces for a node that originates segments
// for a vault it does not home must consume vault-ctl release: every node
// holds the vault-ctl FSM, so a non-home origin purges its completed/ copies
// once they are released, and only then.
func TestNonHomeOriginSpecPurgesCompletedOnRelease(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	released, live := glid.New(), glid.New()
	orch := newTestOrch(t, Config{LocalNodeID: "node-D", SegmentsDir: t.TempDir()})
	fsm := vaultctlfsm.New()

	spec, err := orch.buildPipelineVaultSpec(vaultID, pipelineVaultReg{hasHandle: true}, fsm, nil, nil)
	if err != nil {
		t.Fatalf("buildPipelineVaultSpec: %v", err)
	}
	sup := pipeline.New(pipeline.Config{NodeID: glid.New()})
	if err := sup.RegisterVault(spec); err != nil {
		t.Fatalf("RegisterVault: %v", err)
	}
	if sup.ChunkingRegistered(vaultID) {
		t.Fatal("a non-home origin must not register chunking")
	}

	root := spec.OriginRoot
	for _, id := range []glid.GLID{released, live} {
		if err := os.WriteFile(paths.CompletedSegment(root, id), []byte("segment"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	for _, id := range []glid.GLID{released, live} {
		if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalPublishCompletedSegment(vaultctlfsm.CompletedSegmentEntry{
			SegmentID: id, RecordCount: 1, ByteSize: 1,
			FirstIngestTS: base, LastIngestTS: base, Checksum: 1, PublishedAt: base, OriginNodeID: "node-D",
		})}); err != nil {
			t.Fatalf("PublishCompletedSegment: %v", err)
		}
	}
	if err := fsm.Apply(&hraft.Log{Data: vaultctlfsm.MarshalReleaseSegments([]glid.GLID{released})}); err != nil {
		t.Fatalf("ReleaseSegments: %v", err)
	}

	if _, err := os.Stat(paths.CompletedSegment(root, released)); !os.IsNotExist(err) {
		t.Fatalf("released segment must leave completed/ on a non-home origin, stat err=%v", err)
	}
	if _, err := os.Stat(paths.CompletedSegment(root, live)); err != nil {
		t.Fatalf("unreleased segment must stay in completed/: %v", err)
	}
}

// Without a vault-ctl handle there is no FSM to observe release in, so the
// spec carries none and nothing is ever purged on this path.
func TestNoHandleOriginSpecCarriesNoFSM(t *testing.T) {
	t.Parallel()
	orch := newTestOrch(t, Config{LocalNodeID: "node-D", SegmentsDir: t.TempDir()})
	spec, err := orch.buildPipelineVaultSpec(glid.New(), pipelineVaultReg{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("buildPipelineVaultSpec: %v", err)
	}
	if spec.FSM != nil {
		t.Fatal("spec without a vault-ctl handle must carry no FSM")
	}
}
