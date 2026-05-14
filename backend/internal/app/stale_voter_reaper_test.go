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

	// Five-voter cluster so the quorum-preservation gate
	// (gastrolog-24iv4) allows both evictions. With N=5,
	// canSafelyEvict permits stepping down to 4 (failure tolerance 1);
	// the second tick would step to 3 (still failure tolerance 1).
	// The error path under test is "one transient eviction failure
	// does not abort the rest of the tick" — unrelated to the gate.
	cs := &fakeMembership{
		leader: true,
		servers: []cluster.RaftServer{
			voter("local"),
			voter("healthy-a"),
			voter("healthy-b"),
			voter("stale-1"),
			voter("stale-2"),
		},
	}
	now := time.Now()
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"healthy-a": now.Add(-30 * time.Second),
		"healthy-b": now.Add(-30 * time.Second),
		"stale-1":   now.Add(-10 * time.Minute),
		"stale-2":   now.Add(-10 * time.Minute),
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

// gastrolog-24iv4: 3-voter cluster (the bare-metal trio case). Even
// when a voter has been silent past the threshold, the reaper must NOT
// evict — doing so reduces the cluster to N=2 / quorum=2 / zero
// failure tolerance, which strictly worsens availability for a node
// that may be in maintenance. The dead voter sticks around as a ghost
// until an operator explicitly removes it via `cluster remove-node`.
func TestStaleVoterReaperSkipsEvictionAtThreeVoters(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cs := &fakeMembership{
		leader: true,
		servers: []cluster.RaftServer{
			voter("local"),
			voter("healthy"),
			voter("stale"),
		},
	}
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"healthy": now.Add(-30 * time.Second),
		"stale":   now.Add(-10 * time.Minute),
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
		t.Errorf("3-voter cluster: reaper must skip eviction to preserve failure tolerance; got %v", cap.called)
	}
}

// gastrolog-24iv4: same gate logic, 2-voter edge case. Refuse
// eviction even though the cluster is already at zero failure
// tolerance — making it worse (1 voter, can't form quorum at all) is
// strictly bad. Operator must explicitly intervene.
func TestStaleVoterReaperSkipsEvictionAtTwoVoters(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cs := &fakeMembership{
		leader: true,
		servers: []cluster.RaftServer{
			voter("local"),
			voter("stale"),
		},
	}
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"stale": now.Add(-10 * time.Minute),
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
		t.Errorf("2-voter cluster: reaper must skip eviction; got %v", cap.called)
	}
}

// gastrolog-24iv4: 4-voter cluster IS eligible for eviction — post
// eviction leaves 3 voters with failure tolerance 1, which is the
// boundary the gate allows. Verifies the gate doesn't over-block
// larger clusters.
func TestStaleVoterReaperEvictsAtFourVoters(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cs := &fakeMembership{
		leader: true,
		servers: []cluster.RaftServer{
			voter("local"),
			voter("healthy-a"),
			voter("healthy-b"),
			voter("stale"),
		},
	}
	ps := &fakePeerState{lastSeen: map[string]time.Time{
		"healthy-a": now.Add(-30 * time.Second),
		"healthy-b": now.Add(-30 * time.Second),
		"stale":     now.Add(-10 * time.Minute),
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
	if len(cap.called) != 1 || cap.called[0] != "stale" {
		t.Errorf("4-voter cluster: expected single eviction of 'stale'; got %v", cap.called)
	}
}

// gastrolog-24iv4: pure-function unit test for the gate predicate.
// Pins the boundary between "safe to evict" and "refuse to evict"
// against future intent changes.
func TestCanSafelyEvict(t *testing.T) {
	t.Parallel()
	cases := []struct {
		voters int
		want   bool
	}{
		{0, false}, // empty cluster (defensive)
		{1, false}, // single voter
		{2, false}, // pair, evicting leaves a useless 1-voter cluster
		{3, false}, // trio, evicting leaves N=2/quorum=2/failure=0
		{4, true},  // N=4 → N=3, quorum=2, failure=1 — boundary
		{5, true},  // N=5 → N=4, quorum=3, failure=1
		{10, true}, // large clusters comfortably allow eviction
		{100, true},
	}
	for _, c := range cases {
		got := canSafelyEvict(c.voters)
		if got != c.want {
			t.Errorf("canSafelyEvict(%d) = %v, want %v", c.voters, got, c.want)
		}
	}
}
