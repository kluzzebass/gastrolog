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
