package orchestrator

// Coverage for a review finding on gastrolog-9akebz: evaluateStorages had NO
// production call site — startDiskGuard's scheduler job ran evaluate ->
// refreshVaultDiskGuards -> refreshBacklogBudget -> evaluateVaults, and the
// free-space pass had moved out of evaluateVaults into evaluateStorages,
// which nothing ever called. On a live node this meant: no storage protect
// ever engaged, no storage alarms, an empty broadcast — the floor
// protection the base commit had was silently gone, and every test in
// disk_guard_test.go still passed because they all call
// g.evaluate()/g.evaluateStorages()/g.evaluateVaults() directly, pinning the
// guard's internal methods rather than the job body startDiskGuard actually
// registers.
//
// This file closes that gap: it drives the EXACT gocron.Job object
// startDiskGuard hands the scheduler (via the schedulerJob test accessor,
// which takes the same lock Scheduler's own methods do) rather than calling
// diskGuardTick directly or re-deriving its steps — so a future regression
// that again detaches a guard pass from the job closure fails here, not
// silently.

import (
	"errors"
	"testing"
	"time"

	"github.com/go-co-op/gocron/v2"

	"gastrolog/internal/glid"
	"gastrolog/internal/system"
)

// schedulerJob is a test-only accessor for a scheduler's real registered
// gocron job by name, taking the same lock Scheduler's exported methods do.
// Lets a test invoke RunNow() on the ACTUAL job object AddJob registered —
// proving the registered job body, not a hand-reconstructed stand-in for it.
func schedulerJob(s *Scheduler, name string) (gocron.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[name]
	return j, ok
}

// waitForJobBody polls cond until it's true or timeout elapses. RunNow()
// only confirms the job was HANDED to gocron's executor goroutine — see
// gocron's selectRunJobRequest, which replies on its out channel the moment
// the job enters the executor's input queue, not when the task function
// returns — so a cron job's actual side effects land asynchronously after
// RunNow returns. This is the same class of wait Scheduler.WaitIdle already
// uses for one-time jobs (poll + short sleep); WaitIdle itself only tracks
// jobs with schedule "once", so it does not cover a recurring cron job like
// diskGuardJobName.
func waitForJobBody(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("registered job body did not take effect within 2s of RunNow")
}

// TestDiskGuardJobBodyProtectsStorageEndToEnd drives the disk guard's
// REGISTERED scheduler job (not diskGuardTick called directly, and not the
// guard's evaluate*/refresh* methods called directly) end to end: a real
// Orchestrator with a live scheduler, a config store reporting one storage
// below its floor with one vault placed on it, and a statfs fake. Triggering
// the actual gocron job via RunNow() must protect the vault, raise the
// storage alarm, and refuse admission — proving evaluateStorages (and the
// rest of the pass) is reachable from what startDiskGuard actually wired up,
// which the guard-API-level tests elsewhere in this package cannot prove.
func TestDiskGuardJobBodyProtectsStorageEndToEnd(t *testing.T) {
	t.Parallel()

	vaultID := glid.New()
	storageID := glid.New()
	const nodeID = "node-1"

	cfg := &system.Config{
		Vaults: []system.VaultConfig{{
			ID:      vaultID,
			Name:    "on-disk",
			Enabled: true,
			Type:    system.VaultTypeFile,
			Placements: []system.VaultPlacement{
				{StorageID: storageID.String(), Leader: true},
			},
		}},
	}
	rt := system.Runtime{
		NodeStorageConfigs: []system.NodeStorageConfig{{
			NodeID: nodeID,
			FileStorages: []system.FileStorage{{
				ID:            storageID,
				Path:          "volA",
				DiskFreeWarn:  "50%",
				DiskFreeFloor: "40%",
			}},
		}},
	}

	spy := &alertSpy{}
	orch := newTestOrch(t, Config{
		LocalNodeID:    nodeID,
		DiskGuardPaths: []string{"nodepath"}, // non-empty: startDiskGuard only registers the job when the node itself has paths to guard
		Alerts:         spy,
	})
	orch.sysLoader = testSystemLoaderWithRuntime{cfg: cfg, rt: rt}
	// Path resolution against vaultsDir is tested on its own
	// (disk_guard_storage_path_test.go); disable it here so "volA" stays the
	// literal fake-sampler key this test's assertions depend on — this test
	// is about the scheduler job wiring, not path resolution.
	orch.vaultsDir = ""
	orch.diskGuard.sample = func(path string) (uint64, uint64, error) {
		switch path {
		case "nodepath":
			// Node-level guard stays healthy so the assertions below are
			// unambiguously about the STORAGE dimension, not a node-wide trip.
			return 900 * gib, 1000 * gib, nil
		case "volA":
			return 30 * gib, 100 * gib, nil // 30% free — below the storage's 40% floor
		default:
			return 0, 0, errors.New("no such volume")
		}
	}

	job, ok := schedulerJob(orch.scheduler, diskGuardJobName)
	if !ok {
		t.Fatal("disk guard job was not registered by startDiskGuard")
	}
	if err := job.RunNow(); err != nil {
		t.Fatalf("RunNow on the registered disk guard job: %v", err)
	}

	// RunNow only confirms the job reached gocron's executor queue — wait
	// for the executor goroutine to actually run diskGuardTick.
	waitForJobBody(t, func() bool { return orch.diskGuard.vaultStorageProtected(vaultID) })

	if !spy.has("disk-space-exhausted:" + storageID.String()) {
		t.Fatalf("the registered job body must raise the storage alarm too, standing alarms: %v", spy.set)
	}
	if err := orch.vaultAdmissionGate(vaultID); !errors.Is(err, ErrStorageDiskProtect) {
		t.Fatalf("vaultAdmissionGate must refuse via the storage the registered job protected: %v", err)
	}

	// Broadcast side: StorageProtectedVaults (the NodeStats source) must
	// also reflect what the registered job derived, not just the internal
	// vaultStorageProtected query.
	if got := orch.StorageProtectedVaults(); len(got) != 1 || got[0] != vaultID {
		t.Fatalf("StorageProtectedVaults after the registered job ran = %v, want [%s]", got, vaultID)
	}
}
