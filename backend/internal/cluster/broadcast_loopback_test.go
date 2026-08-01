package cluster

import (
	"context"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"

	hraft "github.com/hashicorp/raft"
)

// A broadcast is one event, so it has one publication path: Send reaches this
// node's subscribers as well as its peers. Without that, every producer wires
// its own local notification beside the broadcast and each new payload repeats
// the split.
//
// The peer CACHES stay peer-only. That distinction is the whole design: what is
// unified is who gets woken, not where state is read from — and getting it
// wrong breaks placement quietly, which the sentinel test below exists to stop.

type noPeers struct{}

func (noPeers) Peers() ([]hraft.Server, error) { return nil, nil }
func (noPeers) InvokeService(context.Context, string, string, string, any, any) error {
	return nil
}

func statsMsg(sender string) *gastrologv1.BroadcastMessage {
	return &gastrologv1.BroadcastMessage{
		SenderId: []byte(sender),
		Payload:  &gastrologv1.BroadcastMessage_NodeStats{NodeStats: &gastrologv1.NodeStats{}},
	}
}

func TestSendDeliversToLocalSubscribers(t *testing.T) {
	t.Parallel()
	b := newBroadcaster(noPeers{}, quietLogger(), time.Second)

	var mu sync.Mutex
	var got []string
	b.SetLocalDelivery(func(m *gastrologv1.BroadcastMessage) {
		mu.Lock()
		got = append(got, string(m.GetSenderId()))
		mu.Unlock()
	})

	b.Send(context.Background(), statsMsg("n1"))

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "n1" {
		t.Fatalf("local delivery = %v, want the message once — a producer publishes once", got)
	}
}

// Single-node is the case that would silently regress: Peers() is empty, so the
// peer fan-out returns early. Local delivery must happen before that.
func TestSendDeliversLocallyWithNoPeers(t *testing.T) {
	t.Parallel()
	b := newBroadcaster(noPeers{}, quietLogger(), time.Second)
	delivered := false
	b.SetLocalDelivery(func(*gastrologv1.BroadcastMessage) { delivered = true })

	b.Send(context.Background(), statsMsg("solo"))

	if !delivered {
		t.Fatal("no peers meant no local delivery; a single node would never see its own events")
	}
}

func TestSendWithoutLocalDeliveryIsPeerOnly(t *testing.T) {
	t.Parallel()
	// A Broadcaster with no server attached must not panic on Send.
	b := newBroadcaster(noPeers{}, quietLogger(), time.Second)
	b.Send(context.Background(), statsMsg("n1"))
}

// --- the peer caches stay peer-only ---

func TestPeerStateIgnoresItsOwnBroadcast(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute, time.Minute)
	ps.SetLocalNodeID("self")

	ps.HandleBroadcast(statsMsg("self"))
	if ps.Get("self") != nil {
		t.Error("PeerState recorded this node as a peer")
	}
	if !ps.LastSeen("self").IsZero() {
		t.Error("PeerState gave itself a LastSeen; consumers treat that as peer evidence")
	}

	ps.HandleBroadcast(statsMsg("other"))
	if ps.Get("other") == nil {
		t.Error("PeerState dropped a real peer's broadcast")
	}
}

// The reason the filter above is load-bearing. placement reads an empty
// LivePeers as "PeerState has not warmed up" and falls back to treating every
// Raft member alive, which is what stops it reassigning vaults away from nodes
// it has merely not heard from yet. Self in that list makes it never empty.
func TestPeerStateSelfDoesNotDefeatThePlacementWarmupSentinel(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute, time.Minute)
	ps.SetLocalNodeID("self")

	// A cold node broadcasting only its own stats: still no live peers.
	for range 5 {
		ps.HandleBroadcast(statsMsg("self"))
	}
	if got := ps.LivePeers(); len(got) != 0 {
		t.Fatalf("LivePeers = %v on a node that has heard from nobody — placement's startup "+
			"fallback is gated on this being empty, and vaults get reassigned away from "+
			"nodes that simply have not reported yet", got)
	}
}

func TestPeerStateWithoutLocalNodeIDKeepsPreLoopbackBehaviour(t *testing.T) {
	t.Parallel()
	// No local ID configured means nothing is filtered, which is what callers
	// that never deliver locally should see.
	ps := NewPeerState(time.Minute, time.Minute)
	ps.HandleBroadcast(statsMsg("whoever"))
	if ps.Get("whoever") == nil {
		t.Error("an unconfigured PeerState dropped a broadcast")
	}
}

func TestPeerJobStateIgnoresItsOwnBroadcast(t *testing.T) {
	t.Parallel()
	pjs := NewPeerJobState(time.Minute)
	pjs.SetLocalNodeID("self")

	jobs := &gastrologv1.BroadcastMessage{
		SenderId: []byte("self"),
		Payload: &gastrologv1.BroadcastMessage_NodeJobs{
			NodeJobs: &gastrologv1.NodeJobs{Jobs: []*gastrologv1.Job{{Id: []byte("j1")}}},
		},
	}
	pjs.HandleBroadcast(jobs)

	if len(pjs.GetAll()) != 0 {
		t.Error("PeerJobState folded this node's own jobs into the peer half; the job list " +
			"reads local jobs live from the scheduler, so these would duplicate and lag")
	}
}
