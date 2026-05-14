package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/cluster"
)

// fakeMembership feeds the reaper a fixed leader-state + server list.
type fakeMembership struct {
	leader  bool
	servers []cluster.RaftServer
	err     error
}

func (f *fakeMembership) IsLeader() bool                      { return f.leader }
func (f *fakeMembership) Servers() ([]cluster.RaftServer, error) { return f.servers, f.err }

// fakePeerState returns a constant LastSeen per nodeID.
type fakePeerState struct {
	lastSeen map[string]time.Time
}

func (f *fakePeerState) LastSeen(nodeID string) time.Time {
	return f.lastSeen[nodeID]
}

// captureRemove records every removeNode call so tests can assert
// exactly which nodes were evicted (and only those).
type captureRemove struct {
	mu     sync.Mutex
	called []string
	err    error
}

func (c *captureRemove) fn(_ context.Context, nodeID string) error {
	c.mu.Lock()
	c.called = append(c.called, nodeID)
	c.mu.Unlock()
	return c.err
}

func voter(id string) cluster.RaftServer {
	return cluster.RaftServer{ID: id, Address: id + ":4566", Suffrage: "Voter"}
}

// gastrolog-6bfwk: the reaper evicts voters whose last contact has
// aged past the threshold AND whose lastSeen is non-zero (positive
// evidence of past liveness). The local node is never evicted.
func TestStaleVoterReaperEvictsStaleVoters(t *testing.T) {
	t.Parallel()

	now := time.Now()
	threshold := 5 * time.Minute

	cs := &fakeMembership{
		leader: true,
		servers: []cluster.RaftServer{
			voter("local"),
			voter("fresh"),   // recently seen; keep
			voter("stale-1"), // past threshold; evict
			voter("stale-2"), // past threshold; evict
			voter("zero"),    // never seen; keep (no positive evidence)
		},
	}
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"fresh":   now.Add(-30 * time.Second),
		"stale-1": now.Add(-10 * time.Minute),
		"stale-2": now.Add(-6 * time.Minute),
		// zero: not in map → LastSeen returns zero time
	}}
	cap := &captureRemove{}
	r := &staleVoterReaper{
		clusterSrv:  cs,
		peerState:   ps,
		removeNode:  cap.fn,
		localNodeID: "local",
		threshold:   threshold,
		interval:    time.Hour, // not used in direct tick
		logger:      slog.Default(),
	}

	r.tick(context.Background())

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.called) != 2 {
		t.Fatalf("expected 2 evictions, got %d (%v)", len(cap.called), cap.called)
	}
	got := map[string]bool{}
	for _, id := range cap.called {
		got[id] = true
	}
	if !got["stale-1"] || !got["stale-2"] {
		t.Errorf("expected stale-1 + stale-2 to be evicted, got %v", cap.called)
	}
	if got["local"] {
		t.Errorf("local node MUST never be evicted, got %v", cap.called)
	}
	if got["fresh"] {
		t.Errorf("fresh peer (within threshold) MUST NOT be evicted, got %v", cap.called)
	}
	if got["zero"] {
		t.Errorf("never-seen peer MUST NOT be evicted (no positive evidence), got %v", cap.called)
	}
}

// Followers ignore the tick entirely — leader-only convergence point.
// Without this gate every node would race to evict, producing spurious
// duplicate-eviction Raft applies.
func TestStaleVoterReaperFollowersAreNoOp(t *testing.T) {
	t.Parallel()

	cs := &fakeMembership{
		leader: false, // not leader
		servers: []cluster.RaftServer{
			voter("local"),
			voter("ancient"), // would be evicted on a leader
		},
	}
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"ancient": time.Now().Add(-time.Hour),
	}}
	cap := &captureRemove{}
	r := &staleVoterReaper{
		clusterSrv:  cs,
		peerState:   ps,
		removeNode:  cap.fn,
		localNodeID: "local",
		threshold:   5 * time.Minute,
		logger:      slog.Default(),
	}

	r.tick(context.Background())

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.called) != 0 {
		t.Errorf("follower must not evict, got %v", cap.called)
	}
}

// Non-voters (staging, nonvoter) are out of scope. The reaper only
// touches Voter suffrage.
func TestStaleVoterReaperSkipsNonVoters(t *testing.T) {
	t.Parallel()

	cs := &fakeMembership{
		leader: true,
		servers: []cluster.RaftServer{
			voter("local"),
			{ID: "nonvoter-stale", Address: "x:4566", Suffrage: "Nonvoter"},
			{ID: "staging-stale", Address: "y:4566", Suffrage: "Staging"},
		},
	}
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"nonvoter-stale": time.Now().Add(-time.Hour),
		"staging-stale":  time.Now().Add(-time.Hour),
	}}
	cap := &captureRemove{}
	r := &staleVoterReaper{
		clusterSrv:  cs,
		peerState:   ps,
		removeNode:  cap.fn,
		localNodeID: "local",
		threshold:   5 * time.Minute,
		logger:      slog.Default(),
	}

	r.tick(context.Background())

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.called) != 0 {
		t.Errorf("reaper must skip non-voter suffrage, got %v", cap.called)
	}
}

// An error from removeNode for one voter must not block the rest of
// the tick from proceeding with the remaining stale candidates.
func TestStaleVoterReaperContinuesPastEvictionError(t *testing.T) {
	t.Parallel()

	cs := &fakeMembership{
		leader: true,
		servers: []cluster.RaftServer{
			voter("local"),
			voter("stale-1"),
			voter("stale-2"),
		},
	}
	now := time.Now()
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"stale-1": now.Add(-10 * time.Minute),
		"stale-2": now.Add(-10 * time.Minute),
	}}
	cap := &captureRemove{err: errors.New("transient")}
	r := &staleVoterReaper{
		clusterSrv:  cs,
		peerState:   ps,
		removeNode:  cap.fn,
		localNodeID: "local",
		threshold:   5 * time.Minute,
		logger:      slog.Default(),
	}

	r.tick(context.Background())

	cap.mu.Lock()
	defer cap.mu.Unlock()
	if len(cap.called) != 2 {
		t.Errorf("error on first eviction must not abort the tick; got %v", cap.called)
	}
}
