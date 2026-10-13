package raftgroup

// Coverage for liveness counter classification of Raft observer events.

import (
	"testing"

	hraft "github.com/hashicorp/raft"
)

func TestLivenessCountersObserve(t *testing.T) {
	t.Parallel()
	var c LivenessCounters

	c.observe(hraft.Observation{Data: hraft.RaftState(hraft.Candidate)})
	c.observe(hraft.Observation{Data: hraft.RaftState(hraft.Follower)}) // not an election
	c.observe(hraft.Observation{Data: hraft.LeaderObservation{LeaderID: ""}})
	c.observe(hraft.Observation{Data: hraft.LeaderObservation{LeaderID: "node-2"}}) // elected, not a loss
	c.observe(hraft.Observation{Data: hraft.FailedHeartbeatObservation{PeerID: "node-3"}})
	c.observe(hraft.Observation{Data: hraft.ResumedHeartbeatObservation{PeerID: "node-3"}}) // not counted

	elections, losses, failedHB := c.Snapshot()
	if elections != 1 || losses != 1 || failedHB != 1 {
		t.Fatalf("counters = %d/%d/%d, want 1/1/1", elections, losses, failedHB)
	}

	// Nil receiver is a no-op (cluster-ctl callers may pass nil).
	var nilC *LivenessCounters
	nilC.observe(hraft.Observation{Data: hraft.RaftState(hraft.Candidate)})
	e, l, f := nilC.Snapshot()
	if e != 0 || l != 0 || f != 0 {
		t.Fatal("nil counters must read zero")
	}
}

// A Raft instance's candidacies before it first sees a leader are its
// initial leader election — on group creation or node start — and are not
// counted. Once the instance has seen a leader, every candidacy counts.
// Each instance is classified on its own: one group being established does
// not make another group's initial election count.
func TestGroupLivenessHoldsBackInitialElection(t *testing.T) {
	t.Parallel()
	var c LivenessCounters
	fresh := &groupLiveness{counters: &c}
	other := &groupLiveness{counters: &c}

	// Split votes during the initial election: several candidacies.
	fresh.observe(hraft.Observation{Data: hraft.RaftState(hraft.Candidate)})
	fresh.observe(hraft.Observation{Data: hraft.RaftState(hraft.Follower)})
	fresh.observe(hraft.Observation{Data: hraft.RaftState(hraft.Candidate)})
	fresh.observe(hraft.Observation{Data: hraft.RaftState(hraft.Leader)})
	fresh.observe(hraft.Observation{Data: hraft.LeaderObservation{LeaderID: "node-1"}})
	if e, _, _ := c.Snapshot(); e != 0 {
		t.Fatalf("elections = %d after an initial election, want 0", e)
	}

	other.observe(hraft.Observation{Data: hraft.RaftState(hraft.Candidate)})
	if e, _, _ := c.Snapshot(); e != 0 {
		t.Fatalf("elections = %d after another group's initial candidacy, want 0", e)
	}

	// The established group loses its leader and re-elects.
	fresh.observe(hraft.Observation{Data: hraft.LeaderObservation{LeaderID: ""}})
	fresh.observe(hraft.Observation{Data: hraft.RaftState(hraft.Candidate)})
	fresh.observe(hraft.Observation{Data: hraft.RaftState(hraft.Candidate)})
	elections, losses, _ := c.Snapshot()
	if elections != 2 {
		t.Fatalf("elections = %d after two candidacies in an established group, want 2", elections)
	}
	if losses != 1 {
		t.Fatalf("leader losses = %d, want 1", losses)
	}
}
