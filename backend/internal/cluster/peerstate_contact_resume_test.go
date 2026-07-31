package cluster

import (
	"sync/atomic"
	"testing"
	"time"
)

// The contact-resumed hook exists so a consumer reacting to a peer RETURNING
// sees the Raft half of the evidence. Broadcast arrival cannot cover it: a
// broadcast is delivered only to its receivers, so a node observes no event of
// its own, and with no third node to hear from, a peer whose Raft lane
// recovers ahead of its broadcast path produces no event at all.
//
// The edge must be rare. RecordRaftContact runs on the Raft receive path
// several times a second per peer per group, so a hook that fired on every
// contact would be a firehose, and a consumer that proposes Raft commands
// would propose on every heartbeat.

func recordingHook() (*atomic.Int64, func(string)) {
	var n atomic.Int64
	return &n, func(string) { n.Add(1) }
}

func TestContactResumedFiresOnStaleToFreshEdge(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute, 100*time.Millisecond)
	n, hook := recordingHook()
	ps.SetContactResumedHook(hook)

	base := time.Now()
	ps.RecordRaftContact("peer", "g1", base)
	if got := n.Load(); got != 0 {
		t.Fatalf("first-ever contact fired %d times, want 0 — there is no lapse to resume from", got)
	}

	// A gap wider than raftTTL is the peer having been not-live by Raft
	// evidence, so coming back is the edge.
	ps.RecordRaftContact("peer", "g1", base.Add(time.Second))
	if got := n.Load(); got != 1 {
		t.Fatalf("resumption after a 1s lapse (TTL 100ms) fired %d times, want 1", got)
	}
}

func TestContactResumedSilentWhileContactIsContinuous(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute, time.Second)
	n, hook := recordingHook()
	ps.SetContactResumedHook(hook)

	// 50 contacts at 10ms spacing under a 1s TTL: the steady state, which is
	// exactly what must stay silent.
	base := time.Now()
	for i := range 50 {
		ps.RecordRaftContact("peer", "g1", base.Add(time.Duration(i)*10*time.Millisecond))
	}
	if got := n.Load(); got != 0 {
		t.Fatalf("continuous contact fired %d times, want 0 — the hook is on the Raft receive path", got)
	}
}

func TestContactResumedIgnoresOutOfOrderRecords(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute, 100*time.Millisecond)
	n, hook := recordingHook()
	ps.SetContactResumedHook(hook)

	base := time.Now()
	ps.RecordRaftContact("peer", "g1", base)
	ps.RecordRaftContact("peer", "g1", base.Add(time.Second)) // the edge
	if got := n.Load(); got != 1 {
		t.Fatalf("setup: want 1 edge, got %d", got)
	}

	// A late callback from a slow group carries an older timestamp. It must
	// not re-fire: the stored contact is already fresher, and RecordRaftContact
	// is documented as monotone.
	ps.RecordRaftContact("peer", "g1", base.Add(500*time.Millisecond))
	if got := n.Load(); got != 1 {
		t.Fatalf("a late out-of-order record fired the hook again (%d) — a slow group must not manufacture edges", got)
	}
}

func TestContactResumedDisabledWithoutRaftTTL(t *testing.T) {
	t.Parallel()
	// raftTTL 0 disables the Raft input entirely, so there is no window
	// against which "stale" means anything.
	ps := NewPeerState(time.Minute, 0)
	n, hook := recordingHook()
	ps.SetContactResumedHook(hook)

	base := time.Now()
	ps.RecordRaftContact("peer", "g1", base)
	ps.RecordRaftContact("peer", "g1", base.Add(time.Hour))
	if got := n.Load(); got != 0 {
		t.Fatalf("hook fired %d times with the Raft input disabled, want 0", got)
	}
}

func TestContactResumedNoHookDoesNotPanic(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute, 100*time.Millisecond)
	base := time.Now()
	ps.RecordRaftContact("peer", "g1", base)
	ps.RecordRaftContact("peer", "g1", base.Add(time.Second))
	if ls := ps.LastSeen("peer"); !ls.Equal(base.Add(time.Second)) {
		t.Fatalf("LastSeen = %v, want the fresher contact — recording must work with no hook installed", ls)
	}
}

// The hook must not run under p.mu: it signals a consumer, and holding the
// lock across it would put that consumer between every Raft RPC and its
// record. A hook that reads PeerState would deadlock if the lock were held.
func TestContactResumedHookRunsOutsideTheLock(t *testing.T) {
	t.Parallel()
	ps := NewPeerState(time.Minute, 100*time.Millisecond)
	var observed time.Time
	ps.SetContactResumedHook(func(peerID string) {
		observed = ps.LastSeen(peerID) // takes p.mu.RLock
	})

	base := time.Now()
	ps.RecordRaftContact("peer", "g1", base)
	ps.RecordRaftContact("peer", "g1", base.Add(time.Second))

	if observed.IsZero() {
		t.Fatal("hook never observed LastSeen; a reentrant read would have deadlocked instead")
	}
}
