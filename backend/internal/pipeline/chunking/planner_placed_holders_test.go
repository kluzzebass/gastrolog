package chunking_test

import (
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestLeaderPlannerGateCountsOnlyPlacedSegmentHolders: a segment receipt from
// a node that has left the vault's placement does not close the replication
// window. Planning a segment whose only managed copy is on one home wedges
// the seal queue if that home dies, exactly as a single-copy segment does.
func TestLeaderPlannerGateCountsOnlyPlacedSegmentHolders(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	segID := glid.New()
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	writeCompletedSegment(t, vaultRoot, segID, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	now := base.Add(10 * time.Minute)

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot:  vaultRoot,
		ChunkRoot:  filepath.Join(vaultRoot, "chunks"),
		FSM:        fsm,
		Locate:     chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:    applier,
		IsLeader:   func() bool { return true },
		NewChunkID: chunk.NewChunkID,
		Policy:     chunking.ManifestRotationPolicy{MaxRecords: 100},
		Now:        func() time.Time { return now },
		RequiredHolders: func() ([]string, bool) {
			return []string{"node-a", "node-c"}, true
		},
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segID, base.Add(time.Minute), 2, base, base.Add(time.Second))
	for _, n := range []string{"node-a", "node-departed"} {
		if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, n)); err != nil {
			t.Fatal(err)
		}
	}
	ctx := t.Context()
	for range 4 {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
	}
	if open := fsm.OpenChunk(); open != nil && len(open.Refs) > 0 {
		t.Fatalf("planner counted a holder outside the placement: %+v", open.Refs)
	}

	if err := applier.Apply(vaultctlfsm.MarshalAckSegmentHolder(segID, "node-c")); err != nil {
		t.Fatal(err)
	}
	for range 8 {
		if open := fsm.OpenChunk(); open != nil && len(open.Refs) > 0 {
			break
		}
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatalf("PlanOnce: %v", err)
		}
	}
	open := fsm.OpenChunk()
	if open == nil || len(open.Refs) != 1 || open.Refs[0].SegmentID != segID {
		t.Fatalf("segment not planned once two placed homes hold it: %+v", open)
	}
}
