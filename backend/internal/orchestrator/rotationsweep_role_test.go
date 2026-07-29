package orchestrator

import (
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// TestReconcileInstanceRole: the placement sweep must heal a stale
// instance role — a raced dispatch once left every node follower and a
// vault leaderless (no retention, no backfill) for seven hours. It must
// NOT flip roles when placements resolve to no leader (mid-flap state),
// and must not touch instances outside the placement.
func TestReconcileInstanceRole(t *testing.T) {
	t.Parallel()
	vaultID := glid.New()
	leaderStorage := glid.New()
	followerStorage := glid.New()

	mkSys := func(withLeader bool) *system.System {
		placements := []system.VaultPlacement{
			{StorageID: followerStorage.String()},
		}
		if withLeader {
			placements = append(placements, system.VaultPlacement{StorageID: leaderStorage.String(), Leader: true})
		}
		return &system.System{
			Runtime: system.Runtime{
				NodeStorageConfigs: []system.NodeStorageConfig{
					{NodeID: "node-L", FileStorages: []system.FileStorage{{ID: leaderStorage}}},
					{NodeID: "node-F", FileStorages: []system.FileStorage{{ID: followerStorage}}},
				},
				VaultPlacements: map[glid.GLID][]system.VaultPlacement{vaultID: placements},
			},
			Config: system.Config{
				Vaults: []system.VaultConfig{{ID: vaultID}},
			},
		}
	}

	// Stale-follower leader heals.
	o := &Orchestrator{localNodeID: "node-L", rotationLogger: slog.Default()}
	inst := &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "node-X"}
	sys := mkSys(true)
	o.reconcileInstanceRole(sys, sys.Config.Vaults[0], inst)
	if inst.IsFollower || inst.LeaderNodeID != "" {
		t.Fatalf("leader not healed: %+v", inst)
	}

	// Follower keeps role, leader pointer refreshes.
	o = &Orchestrator{localNodeID: "node-F", rotationLogger: slog.Default()}
	inst = &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "node-X"}
	o.reconcileInstanceRole(sys, sys.Config.Vaults[0], inst)
	if !inst.IsFollower || inst.LeaderNodeID != "node-L" {
		t.Fatalf("follower pointer not refreshed: %+v", inst)
	}

	// No resolvable leader: do not touch anything.
	o = &Orchestrator{localNodeID: "node-L", rotationLogger: slog.Default()}
	inst = &VaultInstance{VaultID: vaultID, IsFollower: true, LeaderNodeID: "node-X"}
	sysNoLeader := mkSys(false)
	o.reconcileInstanceRole(sysNoLeader, sysNoLeader.Config.Vaults[0], inst)
	if !inst.IsFollower || inst.LeaderNodeID != "node-X" {
		t.Fatalf("roles flipped on unresolvable placement: %+v", inst)
	}

	// Node outside the placement: untouched.
	o = &Orchestrator{localNodeID: "node-Z", rotationLogger: slog.Default()}
	inst = &VaultInstance{VaultID: vaultID, IsFollower: false}
	o.reconcileInstanceRole(sys, sys.Config.Vaults[0], inst)
	if inst.IsFollower {
		t.Fatalf("out-of-placement instance touched: %+v", inst)
	}
}

// TestStartPipelineConfigReconcileRegistersOnce pins the registration
// contract: ApplyConfig calls this on every config apply, so re-registration
// must be a silent no-op — but exactly one job may ever end up registered,
// even when applies land concurrently. The HasJob pre-check this replaced was
// a check-then-act race; AddJob's own ErrJobExists answers the same question
// under one lock hold, and a registration failure that is NOT "already there"
// now propagates instead of being masked. See gastrolog-69sjlj.
func TestStartPipelineConfigReconcileRegistersOnce(t *testing.T) {
	t.Parallel()
	o := newTestOrch(t, Config{LocalNodeID: "node-1"})

	const applies = 12
	var wg sync.WaitGroup
	start := make(chan struct{})
	errs := make([]error, applies)
	for i := range applies {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = o.startPipelineConfigReconcile()
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("apply %d: startPipelineConfigReconcile = %v, want nil (already-registered is not a failure)", i, err)
		}
	}
	if n := countJobsNamed(o, pipelineConfigReconcileJobName); n != 1 {
		t.Fatalf("pipeline-config-reconcile jobs registered = %d, want exactly 1", n)
	}
	// Compare against the resolver, not the production constant: the sweep
	// registers sweepCron(pipelineConfigReconcileSchedule), which this test
	// binary compresses (gastrolog-4yzpcj). What is being pinned is that the
	// surviving registration carries the configured schedule, not which
	// cadence this profile happens to configure.
	if want := sweepCron(pipelineConfigReconcileSchedule); o.Scheduler().JobSchedule(pipelineConfigReconcileJobName) != want {
		t.Errorf("registered schedule = %q, want %q",
			o.Scheduler().JobSchedule(pipelineConfigReconcileJobName), want)
	}
}

// TestAddJobReportsExistingJobAsErrJobExists pins the sentinel the caller
// above keys off. Without a typed error the caller would have to string-match
// or keep a racy pre-check.
func TestAddJobReportsExistingJobAsErrJobExists(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	if err := sched.AddJob("dup", "*/15 * * * * *", func() {}); err != nil {
		t.Fatalf("first AddJob: %v", err)
	}
	err = sched.AddJob("dup", "*/15 * * * * *", func() {})
	if !errors.Is(err, ErrJobExists) {
		t.Fatalf("second AddJob = %v, want an error matching ErrJobExists", err)
	}
	// A bad cron expression is a real failure and must NOT look like a
	// duplicate — the caller swallows only ErrJobExists.
	err = sched.AddJob("bad-cron", "not a cron expression", func() {})
	if err == nil || errors.Is(err, ErrJobExists) {
		t.Fatalf("AddJob with an invalid schedule = %v, want a non-ErrJobExists error", err)
	}
}
