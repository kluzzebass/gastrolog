package app

import (
	"testing"
	"time"

	"gastrolog/internal/cluster"
	"gastrolog/internal/multiraft"
	"gastrolog/internal/raftgroup"

	hraft "github.com/hashicorp/raft"
)

func newTestTransport() *multiraft.Transport[string] {
	return multiraft.New[string](hraft.ServerAddress("127.0.0.1:0"),
		func(s string) []byte { return []byte(s) },
		func(b []byte) string { return string(b) },
	)
}

// The wiring is the whole feature. An unattached recorder is silent by
// design — no error, no log, just a cluster that quietly falls back to
// broadcast freshness for every peer forever. So this asserts the attachment
// itself, not merely that the types line up.
func TestWireRaftContactRecorder_AttachesPeerState(t *testing.T) {
	t.Parallel()
	tm := newTestTransport()
	ps := cluster.NewPeerState(15*time.Second, 4*time.Second)

	wireRaftContactRecorder(tm, ps)

	got := tm.ContactRecorder()
	if got == nil {
		t.Fatal("transport has no contact recorder after wiring")
	}
	if got != multiraft.ContactRecorder(ps) {
		t.Fatalf("contact recorder = %T, want the PeerState we wired", got)
	}
}

// Single-node and test configurations have no cluster stack, so there is no
// transport and no peer to be live. Skipping must be a no-op, not a panic on
// a nil receiver during startup.
func TestWireRaftContactRecorder_NilInputsAreNoOps(t *testing.T) {
	t.Parallel()
	wireRaftContactRecorder(nil, cluster.NewPeerState(time.Second, time.Second))

	tm := newTestTransport()
	wireRaftContactRecorder(tm, nil)
	if tm.ContactRecorder() != nil {
		t.Fatal("a nil PeerState must not be installed as the recorder")
	}
}

// The Raft evidence window is derived from the failure detector rather than
// hardcoded, so widening the detector widens liveness with it. A liveness
// window stricter than the consensus timeout it serves is the gastrolog-1io54g
// inversion, and it must stay unrepresentable here too.
func TestRaftContactTTL_TracksTheFailureDetector(t *testing.T) {
	// Not parallel: ConfigureTimeouts mutates package-level raftgroup state.
	base, _, baseLease := raftgroup.RaftTimeouts(raftgroup.GroupConfig{})
	t.Cleanup(func() {
		if err := raftgroup.ConfigureTimeouts(base, baseLease); err != nil {
			t.Fatalf("restore timeouts: %v", err)
		}
	})

	if got, want := raftContactTTL(), raftContactTTLMultiplier*base; got != want {
		t.Fatalf("raftContactTTL = %v, want %v", got, want)
	}
	if raftContactTTL() <= base {
		t.Fatalf("raftContactTTL %v must exceed the heartbeat timeout %v it is measured against",
			raftContactTTL(), base)
	}

	// Widen the detector: the liveness window must follow.
	widened := 6 * time.Second
	if err := raftgroup.ConfigureTimeouts(widened, baseLease); err != nil {
		t.Fatalf("ConfigureTimeouts: %v", err)
	}
	if got, want := raftContactTTL(), raftContactTTLMultiplier*widened; got != want {
		t.Fatalf("after widening the detector: raftContactTTL = %v, want %v", got, want)
	}
}
