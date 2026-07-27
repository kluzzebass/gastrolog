package cluster

import (
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

// Coverage for gastrolog-1lbifx: PeerState liveness derived from Raft
// last-contact instead of a dedicated 1s liveness broadcast.
//
// Every assertion here drives time by passing explicit timestamps into the
// recorders rather than by sleeping. That is not just for speed: a test that
// slept would be measuring the scheduler, and the thing under test is a
// comparison between two timestamps.

const (
	testRaftTTL  = 4 * time.Second
	testStatsTTL = 15 * time.Second
)

func newRaftPeerState() *PeerState {
	return NewPeerState(testStatsTTL, testRaftTTL)
}

// ---------- The aggregation rule ----------

// ANY-GROUP, MOST-RECENT-WINS: contact on one group keeps a peer live even
// while every other group on this node has been silent for far longer than
// the TTL. Silence in a group is not evidence — it usually just means no Raft
// edge exists there (this node does not lead it, or the peer is not in it).
func TestPeerState_RaftContact_AnyGroupMostRecentWins(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()

	// Two stale groups and one fresh one, recorded out of order so the fold
	// cannot be an accident of arrival sequence.
	ps.RecordRaftContact("node-b", "vault/one/ctl", now.Add(-90*time.Second))
	ps.RecordRaftContact("node-b", "cluster-ctl", now.Add(-1*time.Second))
	ps.RecordRaftContact("node-b", "vault/two/ctl", now.Add(-60*time.Second))

	if !ps.IsLive("node-b") {
		t.Fatal("contact on one group within the TTL must keep the peer live regardless of silence elsewhere")
	}
	if got, want := ps.LastSeen("node-b"), now.Add(-1*time.Second); !got.Equal(want) {
		t.Fatalf("LastSeen = %v, want the maximum across groups %v", got, want)
	}
}

// The fold is monotone: a late callback from a slow group carrying an older
// timestamp must not walk the peer's contact backwards. Group callbacks race
// with each other by construction — one per group, on whatever goroutine
// hraft's replication loop happens to be using.
func TestPeerState_RaftContact_MonotoneUnderOutOfOrderRecords(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()

	ps.RecordRaftContact("node-b", "cluster-ctl", now.Add(-1*time.Second))
	ps.RecordRaftContact("node-b", "vault/one/ctl", now.Add(-30*time.Second))

	if got, want := ps.LastSeen("node-b"), now.Add(-1*time.Second); !got.Equal(want) {
		t.Fatalf("LastSeen = %v, want %v — an older record must not regress the fold", got, want)
	}
	if !ps.IsLive("node-b") {
		t.Fatal("a stale late record must not make a live peer read dead")
	}

	// Same for probes.
	ps.RecordRaftProbe("node-b", "cluster-ctl", now.Add(-1*time.Second))
	ps.RecordRaftProbe("node-b", "vault/one/ctl", now.Add(-30*time.Second))
	if !ps.IsLive("node-b") {
		t.Fatal("a stale late probe must not retract a fresh contact")
	}
}

// ---------- Probe authority: when absence means something ----------

// The fast-detection path. We are actively probing the peer (fresh probes,
// which is what a leader replicating to a follower produces every ~200ms) and
// nothing is coming back. That is authoritative: not live, even though the
// peer's last NodeStats broadcast is still well within the stats TTL.
//
// This is the behaviour that replaces the deleted heartbeat broadcast, and it
// is strictly faster: the verdict flips one raftTTL after the last answer
// instead of one broadcast-TTL after the last heartbeat.
func TestPeerState_RaftProbeWithoutContact_IsAuthoritativelyNotLive(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()

	// A NodeStats broadcast landed 2s ago — comfortably fresh.
	ps.Update("node-b", &gastrologv1.NodeStats{NodeName: "beta"}, now.Add(-2*time.Second))
	// Last answer was raftTTL+1s ago; we are still asking, right now.
	ps.RecordRaftContact("node-b", "cluster-ctl", now.Add(-testRaftTTL-time.Second))
	ps.RecordRaftProbe("node-b", "cluster-ctl", now)

	if ps.IsLive("node-b") {
		t.Fatal("actively probed with no answer past the TTL must read not live, even with a fresh broadcast")
	}
	// The cached stats are a separate question and must stay queryable:
	// liveness expiring does not make the last known payload wrong.
	if ps.Get("node-b") == nil {
		t.Fatal("an unreachable verdict must not expire the peer's cached NodeStats")
	}
	// And the long-horizon accessor must keep the broadcast evidence: the
	// five-minute unreachable sweep asks "silent for ages?", and a failing
	// probe must not erase the fact that the peer was broadcasting 2s ago.
	if got, want := ps.LastSeen("node-b"), now.Add(-2*time.Second); !got.Equal(want) {
		t.Fatalf("LastSeen = %v, want the broadcast at %v", got, want)
	}
}

// Positive evidence outranks a failing probe. Under an asymmetric partition —
// our raft lane to the peer is broken but the peer still reaches us — the peer
// is demonstrably alive and must read that way.
func TestPeerState_InboundContactBeatsFailingProbe(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()

	ps.RecordRaftProbe("node-b", "cluster-ctl", now)                         // we are asking
	ps.RecordRaftContact("node-b", "vault/one/ctl", now.Add(-1*time.Second)) // it spoke to us

	if !ps.IsLive("node-b") {
		t.Fatal("a peer we can hear is alive, whatever our own probes are doing")
	}
}

// Probe authority retracts itself. When this node stops leading a group it
// stops probing that group's members — no leadership bookkeeping is mirrored
// into PeerState, so the authority has to age out on the probe timestamp
// alone. Once it does, the verdict falls back to broadcast freshness rather
// than getting stuck on a stale negative.
func TestPeerState_StaleProbeReleasesAuthorityToBroadcast(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()

	// We led this group until raftTTL+1s ago, then stepped down.
	ps.RecordRaftContact("node-b", "cluster-ctl", now.Add(-testRaftTTL-2*time.Second))
	ps.RecordRaftProbe("node-b", "cluster-ctl", now.Add(-testRaftTTL-time.Second))
	// The peer keeps broadcasting, as a healthy peer does.
	ps.Update("node-b", &gastrologv1.NodeStats{}, now.Add(-3*time.Second))

	if !ps.IsLive("node-b") {
		t.Fatal("once we stop probing, a broadcasting peer must read live again — a stepped-down leader's stale negative must not stick")
	}
}

// The handoff between the two windows must not leave a gap. At the instant a
// leader steps down, its last contact and last probe are both ~one heartbeat
// old, so they expire together — the peer stays continuously live through the
// transition on broadcast evidence, never blinking out in between.
func TestPeerState_LeadershipHandoffDoesNotBlink(t *testing.T) {
	now := time.Now()

	// Walk the whole window in 200ms steps from "just stepped down" to well
	// past both TTLs; the peer must be live at every single point.
	for age := time.Duration(0); age <= testRaftTTL+5*time.Second; age += 200 * time.Millisecond {
		ps := newRaftPeerState()
		ps.RecordRaftContact("node-b", "cluster-ctl", now.Add(-age))
		ps.RecordRaftProbe("node-b", "cluster-ctl", now.Add(-age))
		ps.Update("node-b", &gastrologv1.NodeStats{}, now.Add(-4*time.Second))
		if !ps.IsLive("node-b") {
			t.Fatalf("peer blinked out at contact/probe age %v during leadership handoff", age)
		}
	}
}

// ---------- No Raft edge: bootstrap and sparse placement ----------

// Before any group exists — the boot window, and any pair of nodes that
// neither leads a group containing the other — there is no Raft evidence at
// all. A node must NOT be declared unreachable merely because no shared group
// exists yet; the verdict falls through to NodeStats broadcast freshness,
// which is full-mesh by construction.
func TestPeerState_NoRaftEdgeFallsBackToBroadcast(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()

	ps.Update("node-b", &gastrologv1.NodeStats{}, now.Add(-1*time.Second))
	if !ps.IsLive("node-b") {
		t.Fatal("a peer with no Raft edge and a fresh broadcast must read live")
	}

	// Same peer, broadcast now stale: nothing keeps it alive.
	ps.Update("node-b", &gastrologv1.NodeStats{}, now.Add(-testStatsTTL-time.Second))
	if ps.IsLive("node-b") {
		t.Fatal("no Raft edge and a stale broadcast must read not live")
	}
}

// Two co-followers is the structural case the fallback exists for: Raft's
// per-group topology is a star, so they exchange no traffic at all and never
// will while neither leads. Their mutual silence must not read as death.
func TestPeerState_CoFollowersNeverProbeEachOther(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()

	// node-c is a co-follower: we have contact with the leader but nothing
	// whatsoever with node-c, ever.
	ps.RecordRaftContact("node-leader", "cluster-ctl", now)
	ps.Update("node-c", &gastrologv1.NodeStats{}, now.Add(-2*time.Second))

	if !ps.IsLive("node-c") {
		t.Fatal("a co-follower we share no Raft edge with must read live from its broadcasts")
	}
	live := map[string]bool{}
	for _, id := range ps.LivePeers() {
		live[id] = true
	}
	if !live["node-c"] || !live["node-leader"] {
		t.Fatalf("LivePeers = %v, want both node-c and node-leader", ps.LivePeers())
	}
}

// A peer known ONLY through Raft — it has never broadcast NodeStats, e.g. a
// node that just joined and whose first 5s broadcast has not landed — must
// still be live. Liveness and stats freshness are separate questions, and
// LivePeers has to see peers that exist only in the Raft map.
func TestPeerState_RaftOnlyPeerIsLiveAndListed(t *testing.T) {
	ps := newRaftPeerState()
	ps.RecordRaftContact("node-fresh", "cluster-ctl", time.Now())

	if !ps.IsLive("node-fresh") {
		t.Fatal("a peer seen only over Raft must be live")
	}
	if !livePeerContains(ps, "node-fresh") {
		t.Fatalf("LivePeers = %v, want node-fresh", ps.LivePeers())
	}
	if ps.Get("node-fresh") != nil {
		t.Fatal("no NodeStats broadcast has arrived, so Get must still be nil")
	}
}

// A raftTTL of zero disables the Raft input entirely, leaving the pre-existing
// broadcast-only behaviour intact. Every test and every caller that has no
// Raft transport relies on this.
func TestPeerState_ZeroRaftTTLDisablesRaftInput(t *testing.T) {
	ps := NewPeerState(testStatsTTL, 0)
	now := time.Now()

	ps.RecordRaftProbe("node-b", "cluster-ctl", now)
	ps.Update("node-b", &gastrologv1.NodeStats{}, now.Add(-1*time.Second))
	if !ps.IsLive("node-b") {
		t.Fatal("with the Raft input disabled a fresh broadcast must decide, probes notwithstanding")
	}
}

// ---------- Parity with the heartbeat broadcast it replaces ----------

// Detection-latency parity, stated as the two edges of the window rather than
// as a measured duration. A peer answering Raft probes is live right up to the
// TTL and not live immediately past it — the same shape the 1s heartbeat
// broadcast gave with its own TTL, but anchored on the Raft heartbeat timeout
// and therefore faster in production (raftTTL is 2x the 2s detector timeout;
// the deleted broadcast needed 8x its 1s cadence).
func TestPeerState_PausedPeerDetectionWindowEdges(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name       string
		sincePause time.Duration
		wantLive   bool
	}{
		{"just paused", 100 * time.Millisecond, true},
		{"one timed-out probe in", 2 * time.Second, true},
		{"just inside the TTL", testRaftTTL - 500*time.Millisecond, true},
		{"just past the TTL", testRaftTTL + 500*time.Millisecond, false},
		{"long gone", 60 * time.Second, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ps := newRaftPeerState()
			// The peer answered until it paused; we have kept probing since,
			// and its last broadcast is still inside the stats TTL — which
			// must not rescue it.
			ps.RecordRaftContact("node-b", "cluster-ctl", now.Add(-tc.sincePause))
			ps.RecordRaftProbe("node-b", "cluster-ctl", now)
			ps.Update("node-b", &gastrologv1.NodeStats{}, now.Add(-tc.sincePause))

			if got := ps.IsLive("node-b"); got != tc.wantLive {
				t.Fatalf("IsLive %v after pause = %v, want %v", tc.sincePause, got, tc.wantLive)
			}
		})
	}
}

// No flicker under jitter: a healthy peer whose probe round-trips vary wildly
// — GC pauses, scheduler hiccups, a slow disk on the follower — must stay
// continuously live as long as every gap stays inside the TTL. This is the
// regression the heartbeat TTL multiplier had to be raised for
// (gastrolog-4iacg); the Raft-derived signal must not reintroduce it.
func TestPeerState_NoFlickerUnderJitteredContact(t *testing.T) {
	ps := newRaftPeerState()

	// Deterministic pseudo-jitter: gaps between successive contacts sweep
	// from ~200ms (the nominal heartbeat cadence) up to 3.5s (a probe that
	// nearly burned its whole RPC deadline), then back down.
	gaps := []time.Duration{
		200 * time.Millisecond, 250 * time.Millisecond, 1200 * time.Millisecond,
		3500 * time.Millisecond, 210 * time.Millisecond, 2900 * time.Millisecond,
		180 * time.Millisecond, 3400 * time.Millisecond, 300 * time.Millisecond,
	}

	// Walk a virtual timeline backwards: at every step the peer's most recent
	// contact is `elapsed` old, and it must read live at each one.
	base := time.Now()
	elapsed := time.Duration(0)
	for i, gap := range gaps {
		elapsed += gap
		ps.RecordRaftContact("node-b", "cluster-ctl", base.Add(-elapsed))
		ps.RecordRaftProbe("node-b", "cluster-ctl", base)
		// LastSeen keeps the newest of the folded contacts, which is the
		// first one recorded here; every subsequent one is older.
		if !ps.IsLive("node-b") {
			t.Fatalf("step %d (gap %v): healthy peer flickered out", i, gap)
		}
	}
}

// ---------- Eviction paths must cover the Raft map too ----------

// MarkUnreachable is the forwarder's first-hand "this stream is dead" signal.
// Leaving Raft contact standing would let it out-vote that knowledge and keep
// the peer live for another whole TTL.
func TestPeerState_MarkUnreachableClearsRaftContact(t *testing.T) {
	ps := newRaftPeerState()
	now := time.Now()
	ps.Update("node-b", &gastrologv1.NodeStats{}, now)
	ps.RecordRaftContact("node-b", "cluster-ctl", now)

	if !ps.IsLive("node-b") {
		t.Fatal("precondition: node-b live")
	}
	ps.MarkUnreachable("node-b")
	if ps.IsLive("node-b") {
		t.Fatal("MarkUnreachable must expire Raft contact as well as the broadcast")
	}
}

// Delete is permanent removal (node dropped from the Raft configuration); the
// Raft map is one more per-peer cache that would otherwise grow forever across
// cluster scale-downs.
func TestPeerState_DeleteAndReconcilePurgeRaftContact(t *testing.T) {
	now := time.Now()

	ps := newRaftPeerState()
	ps.RecordRaftContact("node-gone", "cluster-ctl", now)
	ps.RecordRaftProbe("node-gone", "cluster-ctl", now)
	ps.Delete("node-gone")
	if ps.IsLive("node-gone") {
		t.Fatal("Delete must purge Raft contact")
	}
	if !ps.LastSeen("node-gone").IsZero() {
		t.Fatal("Delete must leave no trace in LastSeen")
	}

	ps = newRaftPeerState()
	ps.RecordRaftContact("node-gone", "cluster-ctl", now)
	ps.RecordRaftContact("node-kept", "cluster-ctl", now)
	ps.ReconcilePeers(map[string]struct{}{"node-kept": {}})
	if ps.IsLive("node-gone") {
		t.Fatal("ReconcilePeers must purge Raft contact for departed members")
	}
	if !ps.IsLive("node-kept") {
		t.Fatal("ReconcilePeers must keep Raft contact for retained members")
	}
}

// An empty peer ID is what a malformed or unauthenticated RPC header yields.
// It must be dropped rather than accumulating a phantom "" peer that LivePeers
// would then hand to the placement manager.
func TestPeerState_EmptyPeerIDIsIgnored(t *testing.T) {
	ps := newRaftPeerState()
	ps.RecordRaftContact("", "cluster-ctl", time.Now())
	ps.RecordRaftProbe("", "cluster-ctl", time.Now())

	if len(ps.LivePeers()) != 0 {
		t.Fatalf("LivePeers = %v, want empty — an unidentified sender is not a peer", ps.LivePeers())
	}
}
