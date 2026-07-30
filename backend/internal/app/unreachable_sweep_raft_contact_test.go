package app

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// Coverage at the sweep boundary: Live<->Unreachable must follow real peer
// contact — Raft last-contact — and must NOT follow the stats broadcast on its
// own. The stats broadcast is observability payload; a node whose broadcast
// stalls while Raft keeps replicating to it is not unreachable, and flipping it
// would rotate placement off a perfectly healthy node.

// newRaftSweepTest is newSweepTest with the Raft input enabled, so the sweep
// sees a PeerState shaped the way production wires it.
func newRaftSweepTest(t *testing.T, threshold, statsTTL, raftTTL time.Duration) (*unreachableSweep, *sysmem.Store, *cluster.PeerState, glid.GLID) {
	t.Helper()
	store := sysmem.NewStore()
	peerState := cluster.NewPeerState(statsTTL, raftTTL)
	localID := glid.New()
	peerID := glid.New()

	now := time.Now()
	for _, n := range []system.NodeConfig{
		{ID: localID, Name: "local", State: system.NodeStateLive, StateSince: now},
		{ID: peerID, Name: "peer", State: system.NodeStateLive, StateSince: now},
	} {
		if err := store.PutNode(context.Background(), n); err != nil {
			t.Fatalf("PutNode %s: %v", n.Name, err)
		}
	}

	sweep := &unreachableSweep{
		cfgStore:    store,
		peerState:   peerState,
		localNodeID: localID.String(),
		threshold:   threshold,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		now:         time.Now,
	}
	return sweep, store, peerState, peerID
}

func nodeState(t *testing.T, store *sysmem.Store, id glid.GLID) system.NodeState {
	t.Helper()
	n, err := store.GetNode(context.Background(), id)
	if err != nil || n == nil {
		t.Fatalf("GetNode: %v / %v", n, err)
	}
	return n.EffectiveState()
}

// Raft contact alone holds a node Live. A peer whose stats broadcast has been
// silent for far longer than the unreachable threshold — a wedged collector, a
// starved scheduler, a broadcast RPC that keeps timing out — must stay Live as
// long as Raft is still reaching it, because it demonstrably is.
func TestUnreachableSweep_RaftContactAloneKeepsNodeLive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, peerID := newRaftSweepTest(t, time.Minute, 15*time.Second, 4*time.Second)

	now := time.Unix(2000, 0)
	sweep.now = func() time.Time { return now }
	// Broadcast stopped ten minutes ago; Raft contact is current.
	peerState.Update(peerID.String(), &gastrologv1.NodeStats{}, now.Add(-10*time.Minute))
	peerState.RecordRaftContact(peerID.String(), "cluster-ctl", now.Add(-time.Second))

	sweep.tick(ctx)

	if got := nodeState(t, store, peerID); got != system.NodeStateLive {
		t.Fatalf("state = %s, want Live — a stalled stats broadcast is not unreachability", got)
	}
}

// The mirror image: the broadcast alone holds a node Live when there is no
// Raft edge to it. This is the bootstrap and sparse-placement window, and it is
// what stops a node being declared unreachable merely because no shared Raft
// group exists yet.
func TestUnreachableSweep_BroadcastAloneKeepsNodeLiveWithoutRaftEdge(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, peerID := newRaftSweepTest(t, time.Minute, 15*time.Second, 4*time.Second)

	now := time.Unix(2000, 0)
	sweep.now = func() time.Time { return now }
	peerState.Update(peerID.String(), &gastrologv1.NodeStats{}, now.Add(-2*time.Second))

	sweep.tick(ctx)

	if got := nodeState(t, store, peerID); got != system.NodeStateLive {
		t.Fatalf("state = %s, want Live", got)
	}
}

// Both signals lapse — the node really is gone — and only then does the sweep
// transition, anchoring StateSince to the newest evidence of either kind so the
// inspector's "unreachable for X" starts when the node actually went silent.
func TestUnreachableSweep_TransitionsWhenBothSignalsLapse(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, peerID := newRaftSweepTest(t, time.Minute, 15*time.Second, 4*time.Second)

	now := time.Unix(2000, 0)
	sweep.now = func() time.Time { return now }
	lastBroadcast := now.Add(-10 * time.Minute)
	lastContact := now.Add(-5 * time.Minute)
	peerState.Update(peerID.String(), &gastrologv1.NodeStats{}, lastBroadcast)
	peerState.RecordRaftContact(peerID.String(), "cluster-ctl", lastContact)
	// We have kept probing the whole time and heard nothing.
	peerState.RecordRaftProbe(peerID.String(), "cluster-ctl", now)

	sweep.tick(ctx)

	if got := nodeState(t, store, peerID); got != system.NodeStateUnreachable {
		t.Fatalf("state = %s, want Unreachable", got)
	}
	n, _ := store.GetNode(ctx, peerID)
	if !n.StateSince.Equal(lastContact) {
		t.Fatalf("StateSince = %v, want the newest evidence %v (Raft contact, not the older broadcast)",
			n.StateSince, lastContact)
	}
}

// Auto-clear on Raft contact resume alone. A node coming back is reachable over
// Raft long before its next stats broadcast lands, and the sweep must not make
// the operator wait for the slower signal.
func TestUnreachableSweep_AutoClearsOnRaftContactResume(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, peerID := newRaftSweepTest(t, time.Minute, 15*time.Second, 4*time.Second)

	if err := store.SetNodeState(ctx, peerID, system.NodeStateUnreachable, time.Unix(1500, 0)); err != nil {
		t.Fatalf("SetNodeState Unreachable: %v", err)
	}

	now := time.Unix(2000, 0)
	sweep.now = func() time.Time { return now }
	// No broadcast has arrived since it went away; Raft contact just resumed.
	peerState.Update(peerID.String(), &gastrologv1.NodeStats{}, now.Add(-10*time.Minute))
	peerState.RecordRaftContact(peerID.String(), "vault/one/ctl", now.Add(-500*time.Millisecond))

	sweep.tick(ctx)

	if got := nodeState(t, store, peerID); got != system.NodeStateLive {
		t.Fatalf("state = %s, want Live (auto-clear on Raft contact resume)", got)
	}
}

// A node we have never had any evidence about stays Live: zero LastSeen is
// "no positive evidence", never "dead". A cold-start node that has not yet
// joined any group must not be auto-transitioned on the strength of silence.
func TestUnreachableSweep_NeverContactedNodeIsNotTransitioned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sweep, store, peerState, peerID := newRaftSweepTest(t, time.Minute, 15*time.Second, 4*time.Second)

	now := time.Unix(2000, 0)
	sweep.now = func() time.Time { return now }
	// Probes are going out and failing — the node is in the configuration
	// but has never once answered.
	peerState.RecordRaftProbe(peerID.String(), "cluster-ctl", now)

	sweep.tick(ctx)

	if got := nodeState(t, store, peerID); got != system.NodeStateLive {
		t.Fatalf("state = %s, want Live — never-seen nodes are operator territory", got)
	}
}
