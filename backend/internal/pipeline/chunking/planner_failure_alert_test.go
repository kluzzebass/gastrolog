package chunking_test

// Planner error visibility (gastrolog-6wwdos): a registry segment whose
// on-disk index cannot be opened or read was skipped silently by every
// planner pass — records never planned into a sealed manifest, head purge
// blocked, zero diagnostics. The planner now logs the failure (state-based,
// once per segment per failure state), raises an operator alert on repeated
// failures, and keeps planning every other segment.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

func TestCorruptSegmentIndexAlertsAndPlansOthers(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	pubAt := base.Add(time.Minute)
	vaultID := glid.New()
	vaultRoot := t.TempDir()
	segGood := glid.New()
	segBad := glid.New()

	writeCompletedSegment(t, vaultRoot, segGood, vaultID, []recordForSeg{
		{0, base, "a"},
		{1, base.Add(time.Second), "b"},
	})
	// segBad's on-disk file is garbage: the planner can never build its index.
	if err := os.MkdirAll(paths.CompletedDir(vaultRoot), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(paths.CompletedSegment(vaultRoot, segBad), []byte("not a segment file"), 0o644); err != nil {
		t.Fatal(err)
	}

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	sink := &recordingAlertSink{}

	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: vaultRoot,
		ChunkRoot: filepath.Join(vaultRoot, "chunks"),
		FSM:       fsm,
		Locate:    chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:   applier,
		IsLeader:  func() bool { return true },
		Policy:    chunking.ManifestRotationPolicy{MaxRecords: 100},
		Alerts:    sink,
	}); err != nil {
		t.Fatal(err)
	}

	publishSegment(t, fsm, segBad, pubAt, 2, base, base.Add(time.Second))
	publishSegment(t, fsm, segGood, pubAt.Add(time.Millisecond), 2, base, base.Add(time.Second))

	ctx := t.Context()
	for range 4 {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatal(err)
		}
	}

	// The healthy segment must still be planned; the corrupt one must not
	// poison the pass.
	open := fsm.OpenChunk()
	if open == nil || len(open.Refs) == 0 {
		t.Fatalf("open manifest = %+v, want a ref to the healthy segment", open)
	}
	for _, ref := range open.Refs {
		if ref.SegmentID == segBad {
			t.Fatal("corrupt segment must not be planned into the manifest")
		}
		if ref.SegmentID != segGood {
			t.Fatalf("unexpected manifest ref %s", ref.SegmentID)
		}
	}

	alertID := "chunking-unplannable-segment:" + vaultID.String()
	active, _ := sink.snapshot()
	if _, ok := active[alertID]; !ok {
		t.Fatalf("unplannable-segment alert not raised after repeated failures; active=%v", active)
	}

	// Operator restores the segment file: planning resumes and the alert clears.
	writeCompletedSegment(t, vaultRoot, segBad, vaultID, []recordForSeg{
		{0, base, "x"},
		{1, base.Add(time.Second), "y"},
	})
	for range 4 {
		if err := mgr.PlanOnce(ctx, vaultID); err != nil {
			t.Fatal(err)
		}
	}
	open = fsm.OpenChunk()
	foundRestored := false
	if open != nil {
		for _, ref := range open.Refs {
			if ref.SegmentID == segBad {
				foundRestored = true
			}
		}
	}
	if !foundRestored {
		t.Fatalf("restored segment never planned; open=%+v", open)
	}
	active, cleared := sink.snapshot()
	if len(active) != 0 || cleared == 0 {
		t.Fatalf("alert not cleared after restore: active=%v cleared=%d", active, cleared)
	}
}
