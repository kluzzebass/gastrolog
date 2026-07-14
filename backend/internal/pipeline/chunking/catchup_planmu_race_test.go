package chunking_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/chunking"
	"gastrolog/internal/vaultraft/vaultctlfsm"
)

// TestPlanCatchUpConcurrentWithPlanOnceNoRace pins the concurrency contract
// planCatchUp and planStepLocked share (gastrolog-cqz1ef): planCatchUp's own
// newPlannerPass snapshot must be taken under planMu, exactly like the
// pass=nil path inside planStepLocked, because newPlannerPass reaches
// noteUnderReplicated — documented "caller holds planMu" — which mutates
// vaultChunking.underReplicatedAlerted.
//
// In production these two paths run on different goroutines for the same
// vault: RotateCron's planOnce (PlanOnce here) fires from the orchestrator
// scheduler's own goroutine (reconcileChunkCron registers RotateChunkCron as
// a scheduler job in internal/orchestrator/pipeline.go), while planCatchUp
// runs on the per-vault worker's wake loop (manager.go startWorkerLocked ->
// runBuildPass). Nothing serializes the two call sites against each other;
// only planMu does. This test reproduces that shape directly against the
// exported Manager API and relies on `-race` to catch a regression: before
// the fix, planCatchUp read/wrote v.underReplicatedAlerted outside planMu
// while a concurrent PlanOnce touched the same field under the lock.
func TestPlanCatchUpConcurrentWithPlanOnceNoRace(t *testing.T) {
	t.Parallel()
	base := time.Date(2024, 8, 1, 12, 0, 0, 0, time.UTC)
	vaultID := glid.New()
	vaultRoot := t.TempDir()

	const segmentCount = 40
	segIDs := make([]glid.GLID, segmentCount)
	for i := range segIDs {
		segIDs[i] = glid.New()
		ts := base.Add(time.Duration(i) * time.Second)
		writeCompletedSegment(t, vaultRoot, segIDs[i], vaultID, []recordForSeg{{0, ts, "x"}})
	}

	fsm := vaultctlfsm.New()
	applier := &fsmApplier{fsm: fsm}
	// evalNow sits 5 minutes past every segment's publish time (well past
	// underReplicatedAlertAfter = 2m), and RequiredHolders demands 2 holders
	// that no AckSegmentHolder ever supplies. Every segment stays gated for
	// the whole test, so the very first newPlannerPass call anywhere flips
	// vaultChunking.underReplicatedAlerted false->true (a write); every call
	// after that reads it back true. That single write, unsynchronized on
	// the planCatchUp side pre-fix, is what -race needs to catch.
	evalNow := base.Add(10 * time.Minute)
	mgr := chunking.New(chunking.Config{})
	if err := mgr.RegisterVault(vaultID, chunking.VaultConfig{
		VaultRoot: vaultRoot,
		ChunkRoot: filepath.Join(vaultRoot, "chunks"),
		FSM:       fsm,
		Locate:    chunking.VaultSegmentLocator{Root: vaultRoot},
		Applier:   applier,
		IsLeader:  func() bool { return true },
		// Large enough that MaxRecords rotation never fires mid-test — this
		// test is about lock discipline, not seal timing.
		Policy: chunking.ManifestRotationPolicy{MaxRecords: 1_000_000},
		Now:    func() time.Time { return evalNow },
		RequiredHolders: func() []string {
			return []string{"node-a", "node-b"}
		},
	}); err != nil {
		t.Fatal(err)
	}
	for i, segID := range segIDs {
		ts := base.Add(time.Duration(i) * time.Second)
		publishSegment(t, fsm, segID, base.Add(time.Minute).Add(time.Duration(i)*time.Millisecond), 1, ts, ts)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Simulates the scheduler goroutine driving RotateCron -> planOnce for
	// the same vault, independent of the worker goroutine below.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if err := mgr.PlanOnce(ctx, vaultID); err != nil && ctx.Err() == nil {
				t.Errorf("PlanOnce: %v", err)
				return
			}
		}
	}()

	// Simulates the per-vault worker goroutine's runBuildPass -> planCatchUp.
	const catchUpIterations = 200
	for range catchUpIterations {
		if err := mgr.PlanCatchUp(context.Background(), vaultID); err != nil {
			cancel()
			wg.Wait()
			t.Fatalf("PlanCatchUp: %v", err)
		}
	}

	cancel()
	wg.Wait()
}
