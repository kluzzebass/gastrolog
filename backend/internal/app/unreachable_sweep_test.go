package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/alert"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// newSweepTest stands up an unreachableSweep talking to an in-process
// memory Store and PeerState. The clusterSrv field is left nil because
// the tests drive tick(ctx) directly, bypassing the IsLeader gate.
func newSweepTest(t *testing.T, threshold, peerTTL time.Duration) (*unreachableSweep, *sysmem.Store, *cluster.PeerState, glid.GLID, glid.GLID) {
	t.Helper()
	store := sysmem.NewStore()
	peerState := cluster.NewPeerState(peerTTL, 0)
	localID := glid.New()
	peerID := glid.New()

	now := time.Now()
	if err := store.PutNode(context.Background(), system.NodeConfig{
		ID: localID, Name: "local", State: system.NodeStateLive, StateSince: now,
	}); err != nil {
		t.Fatalf("PutNode local: %v", err)
	}
	if err := store.PutNode(context.Background(), system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateLive, StateSince: now,
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}

	sweep := &unreachableSweep{
		cfgStore:    store,
		peerState:   peerState,
		localNodeID: localID.String(),
		threshold:   threshold,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:         time.Now,
	}
	return sweep, store, peerState, localID, peerID
}

func setPeerLastSeen(t *testing.T, ps *cluster.PeerState, peerID string, lastSeen time.Time) {
	t.Helper()
	ps.Update(peerID, &gastrologv1.NodeStats{}, lastSeen)
}

func TestUnreachableSweep_LiveToUnreachable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, peerState, _, peerID := newSweepTest(t, threshold, 10*time.Second)

	// Last-seen is older than threshold → expect Live → Unreachable.
	sweep.now = func() time.Time { return time.Unix(2000, 0) }
	setPeerLastSeen(t, peerState, peerID.String(), time.Unix(1000, 0))

	sweep.tick(ctx)

	n, err := store.GetNode(ctx, peerID)
	if err != nil || n == nil {
		t.Fatalf("GetNode peer: %v / %v", n, err)
	}
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("expected Unreachable, got %s", n.EffectiveState())
	}
	// gastrolog-778iv: StateSince anchors to lastSeen, not now, so the
	// inspector's "unreachable Xm" duration matches the moment the
	// peer actually went silent.
	if !n.StateSince.Equal(time.Unix(1000, 0)) {
		t.Fatalf("expected StateSince=lastSeen=1000, got %v", n.StateSince)
	}
}

func TestUnreachableSweep_LiveToUnreachable_NeverSeen(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, _, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)

	// No PeerState entry for peer → zero LastSeen. Sweep must NOT transition.
	sweep.tick(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("expected Live (never-seen, no transition), got %s", n.EffectiveState())
	}
}

func TestUnreachableSweep_LiveToUnreachable_WithinThreshold(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, peerState, _, peerID := newSweepTest(t, threshold, 10*time.Second)

	// Last-seen is recent (within threshold) → no transition.
	sweep.now = func() time.Time { return time.Unix(2000, 0) }
	setPeerLastSeen(t, peerState, peerID.String(), time.Unix(1990, 0))

	sweep.tick(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("expected Live (fresh heartbeat), got %s", n.EffectiveState())
	}
}

func TestUnreachableSweep_UnreachableToLive_AutoClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, peerState, _, peerID := newSweepTest(t, threshold, 10*time.Second)

	// Peer was previously Unreachable.
	if err := store.SetNodeState(ctx, peerID, system.NodeStateUnreachable, time.Unix(1500, 0)); err != nil {
		t.Fatalf("SetNodeState Unreachable: %v", err)
	}

	// Heartbeat resumes → expect Unreachable → Live.
	sweep.now = func() time.Time { return time.Unix(2000, 0) }
	setPeerLastSeen(t, peerState, peerID.String(), time.Unix(1990, 0))

	sweep.tick(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("expected Live (auto-clear), got %s", n.EffectiveState())
	}
	if !n.StateSince.Equal(time.Unix(2000, 0)) {
		t.Fatalf("expected StateSince=2000, got %v", n.StateSince)
	}
}

func TestUnreachableSweep_UnreachableToLive_StillStale(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, peerState, _, peerID := newSweepTest(t, threshold, 10*time.Second)

	if err := store.SetNodeState(ctx, peerID, system.NodeStateUnreachable, time.Unix(1500, 0)); err != nil {
		t.Fatalf("SetNodeState Unreachable: %v", err)
	}

	// Heartbeat is still stale → must remain Unreachable.
	sweep.now = func() time.Time { return time.Unix(2000, 0) }
	setPeerLastSeen(t, peerState, peerID.String(), time.Unix(1000, 0))

	sweep.tick(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("expected Unreachable (still stale), got %s", n.EffectiveState())
	}
}

func TestUnreachableSweep_OperatorStatesUntouched(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute

	// Each operator-sticky state must survive a heartbeat lapse AND a
	// heartbeat resume without being flipped by the sweep.
	cases := []struct {
		name  string
		state system.NodeState
	}{
		{"Maintenance", system.NodeStateMaintenance},
		{"Draining", system.NodeStateDraining},
		{"Decommissioning", system.NodeStateDecommissioning},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/stale_heartbeat", func(t *testing.T) {
			sweep, store, peerState, _, peerID := newSweepTest(t, threshold, 10*time.Second)

			// Force state to the operator-sticky value via the legal-transition
			// chain (Live → Draining or Live → Maintenance, etc.).
			now := time.Unix(1500, 0)
			switch tc.state {
			case system.NodeStateMaintenance, system.NodeStateDraining:
				if err := store.SetNodeState(ctx, peerID, tc.state, now); err != nil {
					t.Fatalf("SetNodeState %s: %v", tc.state, err)
				}
			case system.NodeStateDecommissioning:
				// Must go through Draining first.
				if err := store.SetNodeState(ctx, peerID, system.NodeStateDraining, now); err != nil {
					t.Fatalf("SetNodeState Draining: %v", err)
				}
				if err := store.SetNodeState(ctx, peerID, system.NodeStateDecommissioning, now); err != nil {
					t.Fatalf("SetNodeState Decommissioning: %v", err)
				}
			}

			sweep.now = func() time.Time { return time.Unix(3000, 0) }
			setPeerLastSeen(t, peerState, peerID.String(), time.Unix(1000, 0))

			sweep.tick(ctx)

			n, _ := store.GetNode(ctx, peerID)
			if n.EffectiveState() != tc.state {
				t.Fatalf("operator state %s was changed to %s by sweep", tc.state, n.EffectiveState())
			}
		})
		t.Run(tc.name+"/fresh_heartbeat", func(t *testing.T) {
			sweep, store, peerState, _, peerID := newSweepTest(t, threshold, 10*time.Second)

			now := time.Unix(1500, 0)
			switch tc.state {
			case system.NodeStateMaintenance, system.NodeStateDraining:
				if err := store.SetNodeState(ctx, peerID, tc.state, now); err != nil {
					t.Fatalf("SetNodeState %s: %v", tc.state, err)
				}
			case system.NodeStateDecommissioning:
				if err := store.SetNodeState(ctx, peerID, system.NodeStateDraining, now); err != nil {
					t.Fatalf("SetNodeState Draining: %v", err)
				}
				if err := store.SetNodeState(ctx, peerID, system.NodeStateDecommissioning, now); err != nil {
					t.Fatalf("SetNodeState Decommissioning: %v", err)
				}
			}

			sweep.now = func() time.Time { return time.Unix(3000, 0) }
			setPeerLastSeen(t, peerState, peerID.String(), time.Unix(2990, 0))

			sweep.tick(ctx)

			n, _ := store.GetNode(ctx, peerID)
			if n.EffectiveState() != tc.state {
				t.Fatalf("operator state %s was changed to %s by sweep", tc.state, n.EffectiveState())
			}
		})
	}
}

func TestUnreachableSweep_SkipsLocalNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, peerState, localID, _ := newSweepTest(t, threshold, 10*time.Second)

	// Even if PeerState somehow had a stale entry for the local row, the
	// sweep must not transition itself.
	sweep.now = func() time.Time { return time.Unix(2000, 0) }
	setPeerLastSeen(t, peerState, localID.String(), time.Unix(1000, 0))

	sweep.tick(ctx)

	n, _ := store.GetNode(ctx, localID)
	if n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("local node was flipped to %s by its own sweep", n.EffectiveState())
	}
}

func TestUnreachableSweep_EnvVarThresholdOverride(t *testing.T) {
	// Not parallel: mutates the process environment.
	t.Setenv("GLOG_UNREACHABLE_THRESHOLD", "2s")
	store := sysmem.NewStore()
	peerState := cluster.NewPeerState(10*time.Second, 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sweep := newUnreachableSweep(store, nil, peerState, "local", nil, logger)
	if sweep.threshold != 2*time.Second {
		t.Fatalf("expected threshold=2s from env var, got %v", sweep.threshold)
	}
}

func TestUnreachableSweep_EnvVarInvalidFallsBack(t *testing.T) {
	// Not parallel: mutates the process environment.
	t.Setenv("GLOG_UNREACHABLE_THRESHOLD", "not-a-duration")
	store := sysmem.NewStore()
	peerState := cluster.NewPeerState(10*time.Second, 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sweep := newUnreachableSweep(store, nil, peerState, "local", nil, logger)
	if sweep.threshold != defaultUnreachableThreshold {
		t.Fatalf("expected default threshold on invalid env, got %v", sweep.threshold)
	}
}

// newAlertSweepTest stands up an unreachableSweep wired to an
// alert.Collector and a peer node that the test can directly mutate
// via the store. The clusterSrv field stays nil — alertTick does not
// touch it (the alert phase is not leader-gated).
func newAlertSweepTest(t *testing.T, alertThreshold time.Duration) (*unreachableSweep, *sysmem.Store, *alert.Collector, glid.GLID) {
	t.Helper()
	store := sysmem.NewStore()
	peerState := cluster.NewPeerState(time.Hour, 0)
	collector := alert.New()
	peerID := glid.New()
	now := time.Now()
	if err := store.PutNode(context.Background(), system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateLive, StateSince: now,
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}
	sweep := &unreachableSweep{
		cfgStore:       store,
		peerState:      peerState,
		localNodeID:    glid.New().String(),
		threshold:      time.Minute,
		alertThreshold: alertThreshold,
		alerts:         collector,
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:            time.Now,
	}
	return sweep, store, collector, peerID
}

func findAlert(collector *alert.Collector, id string) *alert.Alarm {
	for _, a := range collector.Standing() {
		if a.ID == id {
			return a
		}
	}
	return nil
}

func TestUnreachableSweep_AlertFiresAfterThreshold(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, collector, peerID := newAlertSweepTest(t, threshold)

	// Mark peer Unreachable with StateSince 2 minutes in the past.
	stateSince := time.Unix(1000, 0)
	sweep.now = func() time.Time { return stateSince.Add(2 * time.Minute) }
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateUnreachable, StateSince: stateSince,
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}

	sweep.alertTick(ctx)

	a := findAlert(collector, unreachableAlarmType+":"+peerID.String())
	if a == nil {
		t.Fatal("expected unreachable alert to fire after sustained duration")
	}
	if a.Priority != alert.High {
		t.Fatalf("expected High priority (from the catalog), got %v", a.Priority)
	}
	if a.Response == "" || a.Cause == "" {
		t.Fatal("catalog cause/response must be stamped on the raised alarm")
	}
}

func TestUnreachableSweep_AlertSuppressedWithinThreshold(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := 5 * time.Minute
	sweep, store, collector, peerID := newAlertSweepTest(t, threshold)

	// Mark peer Unreachable just now — well below the alert threshold.
	stateSince := time.Unix(1000, 0)
	sweep.now = func() time.Time { return stateSince.Add(30 * time.Second) }
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateUnreachable, StateSince: stateSince,
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}

	sweep.alertTick(ctx)

	if a := findAlert(collector, unreachableAlarmType+":"+peerID.String()); a != nil {
		t.Fatalf("expected no alert within threshold window, got %+v", a)
	}
}

func TestUnreachableSweep_AlertClearsOnRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, collector, peerID := newAlertSweepTest(t, threshold)

	// First, fire the alert: peer Unreachable for 2× the threshold.
	stateSince := time.Unix(1000, 0)
	sweep.now = func() time.Time { return stateSince.Add(2 * time.Minute) }
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateUnreachable, StateSince: stateSince,
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}
	sweep.alertTick(ctx)
	if a := findAlert(collector, unreachableAlarmType+":"+peerID.String()); a == nil {
		t.Fatal("preconditions: alert should have fired")
	}

	// Peer recovers to Live; alert must clear on next tick.
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateLive, StateSince: sweep.now(),
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}
	sweep.alertTick(ctx)

	if a := findAlert(collector, unreachableAlarmType+":"+peerID.String()); a != nil {
		t.Fatalf("expected alert cleared after recovery, got %+v", a)
	}
}

func TestUnreachableSweep_AlertSilentInMaintenance(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := time.Minute
	sweep, store, collector, peerID := newAlertSweepTest(t, threshold)

	// Operator set node to Maintenance — even with old StateSince, no alert.
	stateSince := time.Unix(1000, 0)
	sweep.now = func() time.Time { return stateSince.Add(time.Hour) }
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateMaintenance, StateSince: stateSince,
	}); err != nil {
		t.Fatalf("PutNode peer: %v", err)
	}

	sweep.alertTick(ctx)

	if a := findAlert(collector, unreachableAlarmType+":"+peerID.String()); a != nil {
		t.Fatalf("expected no alert for Maintenance state, got %+v", a)
	}
}

// TestUnreachableSweep_PlacementGuardChain demonstrates the closed loop
// the sweep is wired for: peer heartbeat lapses → sweep transitions the
// peer to Unreachable → placement reconciler reads the Unreachable
// state and retains the existing leader placement instead of rotating.
// Heartbeat resumes → sweep auto-clears back to Live → placement now
// permits rotation again. This is the cluster behavior described in
// the gastrolog-39m2k acceptance criteria.
//
// Uses real wall-clock timestamps (rather than mocked sweep.now)
// because the placement manager's PeerState liveness check reads the
// system clock directly — both clocks must agree on whether the peer
// is currently reachable for the chained assertion to be meaningful.
func TestUnreachableSweep_PlacementGuardChain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	threshold := 100 * time.Millisecond
	// PeerState TTL must outlive the sweep threshold; otherwise the
	// PeerState entry expires from LivePeers() and placement sees the
	// peer as not-alive regardless of NodeConfig.State.
	sweep, store, peerState, localID, peerID := newSweepTest(t, threshold, 10*time.Second)
	pm, _, alerts := newTestPlacement(t, localID.String(), nil)
	pm.cfgStore = store
	pm.peerState = peerState

	vaultID := seedVaultWithLeader(t, store, peerID.String())

	// --- Stage 1: peer's heartbeat is older than threshold. ---
	setPeerLastSeen(t, peerState, peerID.String(), time.Now().Add(-5*threshold))

	sweep.tick(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("stage 1: expected peer Unreachable, got %s", n.EffectiveState())
	}

	pm.reconcile(ctx)
	if got := vaultNode(t, store, vaultID); got != peerID.String() {
		t.Fatalf("stage 1: expected vault to remain on Unreachable peer %s, got %q",
			peerID, got)
	}
	if !hasAlert(alerts, "vault-soft-offline-leader") {
		t.Fatal("stage 1: expected soft-offline alert to be raised")
	}

	// --- Stage 2: peer's heartbeat resumes (current wall-clock). ---
	setPeerLastSeen(t, peerState, peerID.String(), time.Now())

	sweep.tick(ctx)

	n, _ = store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("stage 2: expected peer auto-cleared to Live, got %s", n.EffectiveState())
	}

	pm.reconcile(ctx)
	if got := vaultNode(t, store, vaultID); got != peerID.String() {
		t.Fatalf("stage 2: expected vault to remain on healthy peer %s, got %q",
			peerID, got)
	}
	if hasAlert(alerts, "vault-soft-offline-leader") {
		t.Fatal("stage 2: expected soft-offline alert to be cleared")
	}
}

// TestStartUnreachableSweep_RegistersOperatorVisibleJob verifies the
// sweep gets registered as a proper scheduled job (visible in the
// inspector's Scheduled view) rather than a raw goroutine. Asserts
// the AddJob name + cron + non-empty Describe text, then runs the
// captured task to confirm it drives a tick end-to-end.
func TestStartUnreachableSweep_RegistersOperatorVisibleJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, _, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	// Mark peer Live with stale heartbeat so the leader-only tick
	// path would propose a transition if it ran. We deliberately
	// leave clusterSrv=nil so tickOnce skips the transition phase
	// (the test isolates registration, not transition logic).
	sweep.now = func() time.Time { return time.Unix(3000, 0) }
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateLive, StateSince: time.Unix(1000, 0),
	}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	sched := &fakeScheduler{}
	if err := startUnreachableSweep(ctx, sched, sweep); err != nil {
		t.Fatalf("startUnreachableSweep: %v", err)
	}

	if sched.addJobName != unreachableSweepJobName {
		t.Errorf("AddJob name: got %q, want %q", sched.addJobName, unreachableSweepJobName)
	}
	if sched.addJobCron != unreachableSweepSchedule {
		t.Errorf("AddJob cron: got %q, want %q", sched.addJobCron, unreachableSweepSchedule)
	}
	if sched.describeName != unreachableSweepJobName {
		t.Errorf("Describe name: got %q, want %q", sched.describeName, unreachableSweepJobName)
	}
	if sched.describeMessage == "" {
		t.Error("Describe message empty — operator inspector will show no context")
	}

	// Run the captured task as the scheduler would. The transition
	// phase short-circuits on nil clusterSrv (test setup), so this
	// just confirms the closure resolves and tickOnce runs without
	// panicking.
	if task, ok := sched.addJobTaskFn.(func()); ok {
		task()
	} else {
		t.Fatalf("expected captured task of type func(), got %T", sched.addJobTaskFn)
	}
}

// TestStartUnreachableSweep_PropagatesAddJobError verifies the caller
// sees the AddJob failure (e.g. duplicate name) instead of silently
// running a broken-registration sweep.
func TestStartUnreachableSweep_PropagatesAddJobError(t *testing.T) {
	t.Parallel()
	sweep, _, _, _, _ := newSweepTest(t, time.Minute, 10*time.Second)
	sched := &fakeScheduler{addJobErr: errFakeMember}

	err := startUnreachableSweep(context.Background(), sched, sweep)
	if err == nil {
		t.Fatal("expected AddJob error to propagate")
	}
}

// TestTickOnce_LeaderRunsBothPhases verifies the integrated tick
// runs the transition phase only when clusterSrv reports leadership,
// and the alert phase unconditionally.
func TestTickOnce_NonLeaderRunsAlertPhaseOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, _, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	// nil clusterSrv stands in for "not leader" — IsLeader can't be
	// called, but tickOnce's `s.clusterSrv != nil` guard makes the
	// transition phase a no-op in that case.
	sweep.clusterSrv = nil
	sweep.alerts = alert.New()
	sweep.alertThreshold = time.Minute
	stateSince := time.Unix(1000, 0)
	sweep.now = func() time.Time { return stateSince.Add(2 * time.Minute) }
	if err := store.PutNode(ctx, system.NodeConfig{
		ID: peerID, Name: "peer", State: system.NodeStateUnreachable, StateSince: stateSince,
	}); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	sweep.tickOnce(ctx)

	// Alert phase ran → alert fired for the Unreachable peer.
	if a := findAlert(sweep.alerts, unreachableAlarmType+":"+peerID.String()); a == nil {
		t.Fatal("expected alert phase to fire on non-leader; got no alert")
	}
	// Transition phase did NOT run → store still shows Unreachable
	// (never flipped to Live or any other state).
	n, err := store.GetNode(ctx, peerID)
	if err != nil || n == nil {
		t.Fatalf("GetNode: %v / %v", n, err)
	}
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Errorf("transition phase ran on non-leader: state %s, want Unreachable", n.EffectiveState())
	}
}
