package orchestrator

// Test-timing profile for this package's test binary.
//
// The orchestrator package's non-short runtime was WAIT bound, not compute
// bound: a full pass burned ~520s wall on ~51s of CPU (~10% of one core on a
// 16-core box). The wait had exactly two sources, and this file removes both
// WITHOUT touching a single assertion:
//
//  1. Raft failure-detector timing. Every multi-node harness boots its nodes
//     through a real hashicorp/raft election, and at the shipped 2s base the
//     vault-ctl detector is 3s, so first leadership lands 3-6s after boot —
//     per harness, serialized by orchRelHarnessSlots.
//
//  2. Periodic reconcile sweeps. Acceptance tests that assert on retention or
//     catch-up outcomes wait for those sweeps to tick, at 60s and 20s
//     respectively.
//
// Both are configuration, not behaviour: the code paths under test are
// identical, they simply run on a compressed clock. Nothing here is a sleep,
// a fixed deadline, or a widened timeout — the waits stay progress-bound
// (see waitProgress in reliability_orch_harness_test.go); only the cadence of
// the events those waits observe changes.
//
// Production defaults are untouched: this file is a _test file, and the seams
// it writes (raftgroup.ConfigureTimeouts, sweepCadenceOverride) both resolve
// to the shipped values in every non-test build.

import (
	"fmt"
	"os"
	"testing"
	"time"

	"gastrolog/internal/raftgroup"
)

const (
	// testRaftHeartbeat is the node-wide base failure-detector window for this
	// test binary, down from the 2s default. Vault-ctl groups derive
	// 300ms from it (base + slack capped at base/2), so a fresh group elects
	// in 300-600ms instead of 3-6s.
	//
	// 200ms is not "as low as it goes": hashicorp/raft probes followers every
	// HeartbeatTimeout/10, so the floor is set by how long a goroutine can
	// stay descheduled without the cluster calling an unnecessary election.
	// Under full-suite load on a busy box that is tens of milliseconds, so
	// 200ms keeps roughly an order of magnitude of headroom. Spurious
	// elections would not fail anything — the harness treats term changes as
	// progress — but they would spend the wall time this profile exists to
	// reclaim.
	testRaftHeartbeat = 200 * time.Millisecond
	// testRaftLease must not exceed testRaftHeartbeat (hashicorp/raft rejects
	// that, and a lease outliving the detector would let a deposed leader
	// serve lease-gated reads). Keeps the default's 3:4 lease:detector ratio.
	testRaftLease = 150 * time.Millisecond

	// testSweepCadence is the cron every periodic reconcile sweep registers
	// with in this binary (see sweepCron): every second, the finest cadence
	// 6-field cron expresses. The sweeps still fire on their own timer and
	// still do their full pass — a test that needs three consecutive deferred
	// retention sweeps to raise an alarm still needs three of them, it just
	// gets them in three seconds instead of three minutes.
	testSweepCadence = "* * * * * *"
)

func TestMain(m *testing.M) {
	// Before any group starts — ConfigureTimeouts does not affect running
	// groups, and TestMain runs before the first test.
	if err := raftgroup.ConfigureTimeouts(testRaftHeartbeat, testRaftLease); err != nil {
		fmt.Fprintf(os.Stderr, "test profile: ConfigureTimeouts: %v\n", err)
		os.Exit(1)
	}
	sweepCadenceOverride = testSweepCadence
	os.Exit(m.Run())
}
