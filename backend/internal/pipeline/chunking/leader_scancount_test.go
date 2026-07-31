package chunking_test

import (
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// catchUpScanDelta sets up a leader vault with segmentCount eligible completed
// segments, runs one PlanCatchUp, and returns how many full registry scans
// (ListCompletedSegments) the pass cost.
func catchUpScanDelta(t *testing.T, segmentCount int) uint64 {
	t.Helper()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segIDs := make([]glid.GLID, segmentCount)
	for i := range segIDs {
		segIDs[i] = glid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segIDs[i], vaultID, []recordForSeg{{0, ts, "x"}})
	}

	fsm := vaultctlfsm.New()
	var applyLog [][]byte
	applier := &fsmApplier{fsm: fsm, log: &applyLog}
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		RequiredHolders: chunking.NoRequiredHolders,
		VaultRoot:       vaultRoot,
		ChunkRoot:       filepath.Join(vaultRoot, "chunks"),
		FSM:             fsm,
		Locate:          chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:         applier,
		IsLeader:        func() bool { return true },
		Policy:          chunking.ManifestRotationPolicy{MaxRecords: 10_000},
	}); err != nil {
		t.Fatal(err)
	}
	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, pubAt.Add(time.Duration(i)*time.Millisecond), 1, ts, ts)
	}

	ctx := t.Context()
	if err := mgr.PlanOnce(ctx, vaultID); err != nil {
		t.Fatalf("open manifest: %v", err)
	}
	before := fsm.CompletedListScans()
	if err := mgr.PlanCatchUp(ctx, vaultID); err != nil {
		t.Fatalf("PlanCatchUp: %v", err)
	}
	// Sanity: the pass consumed the whole backlog (no leak) in one batched apply.
	open := fsm.OpenChunk()
	planned := 0
	if open != nil {
		planned = len(open.Refs)
	}
	if planned != segmentCount {
		t.Fatalf("catch-up planned %d of %d segments (want all — a leak leaves some unplanned)", planned, segmentCount)
	}
	return fsm.CompletedListScans() - before
}

// TestPlannerCatchUpScanCountIndependentOfBacklog pins the O(N^2)->O(N)
// planner cost: a catch-up pass must cost a CONSTANT number of registry
// scans regardless of backlog depth. A per-step re-scan (budget ∝ N steps)
// grows scans with N; the pass snapshots once and threads the state.
func TestPlannerCatchUpScanCountIndependentOfBacklog(t *testing.T) {
	t.Parallel()
	small := catchUpScanDelta(t, 8)
	large := catchUpScanDelta(t, 40)
	if small != large {
		t.Fatalf("catch-up registry scans grew with backlog: %d scans at N=8 vs %d at N=40 (want equal — O(1) per pass)", small, large)
	}
	if small == 0 {
		t.Fatal("expected at least one registry scan per catch-up pass")
	}
}
