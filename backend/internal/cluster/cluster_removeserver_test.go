package cluster_test

// hashicorp/raft treats removing an unknown server ID as success: it commits
// a no-op configuration entry and reports nil, leaving the voter set
// unchanged. Callers logging "removed node X" on that nil are lying to
// whoever acts on the report — an operator with a typo, a preStop hook, two
// concurrent removals. RemoveServer must distinguish "removed" from "was
// never there".

import (
	"errors"
	"testing"
	"time"

	"gastrolog/internal/cluster"

	hraft "github.com/hashicorp/raft"
)

func raftServerCount(t *testing.T, r *hraft.Raft) int {
	t.Helper()
	fut := r.GetConfiguration()
	if err := fut.Error(); err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	return len(fut.Configuration().Servers)
}

func TestRemoveServerUnknownIDIsAnError(t *testing.T) {
	t.Parallel()
	node := newTestNode(t, "node-a", true)
	defer node.close()
	waitLeader(t, node.raft, 5*time.Second)

	err := node.srv.RemoveServer("node-never-joined", 5*time.Second)
	if !errors.Is(err, cluster.ErrNodeNotInCluster) {
		t.Fatalf("removing an unknown ID returned %v, want ErrNodeNotInCluster", err)
	}
	if got := raftServerCount(t, node.raft); got != 1 {
		t.Fatalf("configuration has %d servers after the refused removal, want 1", got)
	}
}

func TestRemoveServerRemovesAMemberOnceAndOnlyOnce(t *testing.T) {
	t.Parallel()
	a := newTestNode(t, "node-a", true)
	defer a.close()
	b := newTestNode(t, "node-b", false)
	defer b.close()
	waitLeader(t, a.raft, 5*time.Second)
	addVoter(t, a.srv, "node-b", string(b.srv.Transport().LocalAddr()))
	if got := raftServerCount(t, a.raft); got != 2 {
		t.Fatalf("configuration has %d servers after join, want 2", got)
	}

	if err := a.srv.RemoveServer("node-b", 5*time.Second); err != nil {
		t.Fatalf("removing a real member: %v", err)
	}
	if got := raftServerCount(t, a.raft); got != 1 {
		t.Fatalf("configuration has %d servers after removal, want 1", got)
	}

	// The second removal of the same ID — the lost race between two
	// concurrent operators — must say so rather than report success again.
	err := a.srv.RemoveServer("node-b", 5*time.Second)
	if !errors.Is(err, cluster.ErrNodeNotInCluster) {
		t.Fatalf("second removal of the same ID returned %v, want ErrNodeNotInCluster", err)
	}
}
