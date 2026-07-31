package app

import (
	"context"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/system"
)

// The two directions are separated by whether an upstream event exists, so the
// split itself needs pinning: a sweep that still auto-cleared would mask a
// broken wake path, and the wake path is the only thing keeping recovery off
// the cron interval.

func TestNodeLiveness_SweepDoesNotAutoClear(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)

	if err := store.SetNodeState(ctx, peerID, system.NodeStateUnreachable, time.Unix(1000, 0)); err != nil {
		t.Fatalf("SetNodeState: %v", err)
	}
	// Contact is current, so the OLD combined tick would have cleared it here.
	setPeerLastSeen(t, peerState, peerID.String(), time.Now())

	sweep.sweepUnreachable(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("state = %s, want unreachable — recovery belongs to autoClear on the wake signal, "+
			"and a sweep that also clears would hide a dead wake path", n.EffectiveState())
	}
}

func TestNodeLiveness_AutoClearLeavesLiveNodesAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	setPeerLastSeen(t, peerState, peerID.String(), time.Now())

	before, _ := store.GetNode(ctx, peerID)
	sweep.autoClear(ctx)
	after, _ := store.GetNode(ctx, peerID)

	if !after.StateSince.Equal(before.StateSince) {
		t.Errorf("StateSince moved on an already-Live node (%v -> %v); auto-clear must propose nothing",
			before.StateSince, after.StateSince)
	}
}

func TestNodeLiveness_AutoClearIgnoresNeverSeenNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, _, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	if err := store.SetNodeState(ctx, peerID, system.NodeStateUnreachable, time.Unix(1000, 0)); err != nil {
		t.Fatalf("SetNodeState: %v", err)
	}
	// No PeerState entry at all: LastSeen is zero, which is "no positive
	// evidence", not "seen long ago".
	sweep.autoClear(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("state = %s, want unreachable — a node never observed must stay operator territory", n.EffectiveState())
	}
}

func TestNodeLiveness_AutoClearSkipsLocalNode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, _, localID, _ := newSweepTest(t, time.Minute, 10*time.Second)
	if err := store.SetNodeState(ctx, localID, system.NodeStateUnreachable, time.Unix(1000, 0)); err != nil {
		t.Fatalf("SetNodeState: %v", err)
	}
	// PeerState holds no entry for self, so the local row's LastSeen is always
	// zero. Skipping it explicitly keeps that from reading as evidence.
	sweep.autoClear(ctx)

	n, _ := store.GetNode(ctx, localID)
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("state = %s: auto-clear acted on the local row", n.EffectiveState())
	}
}

// --- wake plumbing ---

// wakeFired reports whether the signal held in ch has been notified. Signal is
// close-and-recreate, so the channel captured before the action is the one that
// closes.
func wakeFired(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestNodeLiveness_BroadcastTriggersOnlyOnNodeStats(t *testing.T) {
	t.Parallel()
	sweep, _, _, _, _ := newSweepTest(t, time.Minute, 10*time.Second)

	ch := sweep.wake.C()
	sweep.onBroadcast(&gastrologv1.BroadcastMessage{})
	if wakeFired(ch) {
		t.Error("a payload-free broadcast triggered a pass; only NodeStats can move LastSeen")
	}

	ch = sweep.wake.C()
	sweep.onBroadcast(&gastrologv1.BroadcastMessage{
		Payload: &gastrologv1.BroadcastMessage_NodeStats{NodeStats: &gastrologv1.NodeStats{}},
	})
	if !wakeFired(ch) {
		t.Error("a NodeStats broadcast did not trigger a pass — this is the recovery path")
	}
}

func TestNodeLiveness_TriggerCoalesces(t *testing.T) {
	t.Parallel()
	sweep, _, _, _, _ := newSweepTest(t, time.Minute, 10*time.Second)

	ch := sweep.wake.C()
	for range 100 {
		sweep.trigger()
	}
	if !wakeFired(ch) {
		t.Fatal("a burst of triggers left the wake channel unnotified")
	}
	// Re-arming yields a fresh channel: the burst collapsed into one pass
	// rather than queueing 100.
	if wakeFired(sweep.wake.C()) {
		t.Error("the re-armed channel is already closed; triggers are queueing instead of coalescing")
	}
}

func TestNodeLiveness_RunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	sweep, _, _, _, _ := newSweepTest(t, time.Minute, 10*time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		sweep.Run(ctx)
		close(done)
	}()

	// Deliver a trigger first, so cancellation is observed from inside the
	// select rather than only on the initial pass.
	sweep.trigger()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after ctx cancellation")
	}
}

// The composition, not the parts: LastSeen is the MAX of broadcast arrival and
// Raft contact, so a wake driven by broadcasts alone covers less than the
// decision reads. A peer whose Raft lane recovers while its broadcast path
// stays down — with no third node to hear from — would then stay Unreachable
// forever, because recovery no longer has a periodic backstop.
//
// This pins the pairing app.go makes, which is where that gap lives.
func TestNodeLiveness_RaftContactResumeTriggersWithoutAnyBroadcast(t *testing.T) {
	t.Parallel()
	sweep, _, peerState, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	// raftTTL must be live for contact to be admissible evidence at all.
	peerState = cluster.NewPeerState(10*time.Second, 100*time.Millisecond)
	sweep.peerState = peerState
	peerState.SetContactResumedHook(func(string) { sweep.trigger() })

	base := time.Now()
	peerState.RecordRaftContact(peerID.String(), "cluster-ctl", base)

	ch := sweep.wake.C()
	// A lapse, then contact again: the peer's Raft lane came back. No
	// broadcast is delivered anywhere in this test.
	peerState.RecordRaftContact(peerID.String(), "cluster-ctl", base.Add(time.Second))

	if !wakeFired(ch) {
		t.Fatal("a Raft-contact resumption did not trigger an auto-clear pass; " +
			"with no broadcast to hear, this node is the only evidence there is")
	}
}

// autoClearIfLeader is the gate Run applies. A follower must propose nothing:
// the transition is a Raft command and every follower would duplicate it.
func TestNodeLiveness_AutoClearIfLeaderNoOpsWithoutLeadership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	if err := store.SetNodeState(ctx, peerID, system.NodeStateUnreachable, time.Unix(1000, 0)); err != nil {
		t.Fatalf("SetNodeState: %v", err)
	}
	setPeerLastSeen(t, peerState, peerID.String(), time.Now())

	// clusterSrv is nil, which the gate reads as "not the leader".
	sweep.autoClearIfLeader(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("state = %s: a non-leader proposed a transition", n.EffectiveState())
	}

	// The phase itself still works when called directly — proving the guard
	// above is the gate, not a broken auto-clear.
	sweep.autoClear(ctx)
	n, _ = store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("state = %s, want live: autoClear is broken independently of the gate", n.EffectiveState())
	}
}

// The leader counterpart to TestTickOnce_NonLeaderRunsAlertPhaseOnly. Before
// leadership was injectable this direction could not be exercised at all: the
// gate read a concrete *cluster.Server, so a unit test could only ever be a
// follower, and "the leader does transition" went unpinned.
func TestTickOnce_LeaderRunsTheTransitionPhase(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	sweep.isLeader = func() bool { return true }
	setPeerLastSeen(t, peerState, peerID.String(), time.Now().Add(-time.Hour))

	sweep.tickOnce(ctx)

	n, _ := store.GetNode(ctx, peerID)
	if n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("state = %s, want unreachable: the leader did not run the transition phase", n.EffectiveState())
	}
}

// Leadership can change after Run has started, so the gate must be consulted
// per pass rather than captured once.
func TestNodeLiveness_AutoClearGateIsReEvaluatedPerPass(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, _, peerID := newSweepTest(t, time.Minute, 10*time.Second)
	if err := store.SetNodeState(ctx, peerID, system.NodeStateUnreachable, time.Unix(1000, 0)); err != nil {
		t.Fatalf("SetNodeState: %v", err)
	}
	setPeerLastSeen(t, peerState, peerID.String(), time.Now())

	leader := false
	sweep.isLeader = func() bool { return leader }

	sweep.autoClearIfLeader(ctx)
	if n, _ := store.GetNode(ctx, peerID); n.EffectiveState() != system.NodeStateUnreachable {
		t.Fatalf("state = %s: acted while not leader", n.EffectiveState())
	}

	// Leadership arrives — the node inherited an Unreachable row that is
	// already recovered, which is exactly what the leadership-gain trigger
	// exists to catch.
	leader = true
	sweep.autoClearIfLeader(ctx)
	if n, _ := store.GetNode(ctx, peerID); n.EffectiveState() != system.NodeStateLive {
		t.Fatalf("state = %s, want live: a new leader did not re-evaluate inherited state", n.EffectiveState())
	}
}
