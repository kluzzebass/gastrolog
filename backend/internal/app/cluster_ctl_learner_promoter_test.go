package app

import (
	"strconv"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
)

// mockRaftMembership implements raftMembership with configurable
// returns. Tracks AddVoter calls so tests can assert promotion shape.
type mockRaftMembership struct {
	mu            sync.Mutex
	isLeader      bool
	servers       []cluster.RaftServer
	serversErr    error
	addVoterErr   error
	appliedIndex  uint64
	statsMissing  bool
	addVoterCalls []addVoterCall
}

type addVoterCall struct {
	id   string
	addr string
}

func (m *mockRaftMembership) IsLeader() bool { return m.isLeader }
func (m *mockRaftMembership) Servers() ([]cluster.RaftServer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]cluster.RaftServer(nil), m.servers...), m.serversErr
}

func (m *mockRaftMembership) AddVoter(id, addr string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addVoterCalls = append(m.addVoterCalls, addVoterCall{id: id, addr: addr})
	if m.addVoterErr != nil {
		return m.addVoterErr
	}
	// Reflect successful promotion in the configuration a later pass reads.
	for i, s := range m.servers {
		if s.ID == id {
			m.servers[i].Suffrage = "Voter"
			break
		}
	}
	return nil
}

func (m *mockRaftMembership) LocalStats() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.statsMissing {
		return nil
	}
	return map[string]string{"applied_index": strconv.FormatUint(m.appliedIndex, 10)}
}

func (m *mockRaftMembership) voterCalls() []addVoterCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]addVoterCall(nil), m.addVoterCalls...)
}

// mockPeerStats implements peerStatsReader.
type mockPeerStats struct {
	byNode map[string]uint64
}

func (m *mockPeerStats) Get(senderID string) *gastrologv1.NodeStats {
	if v, ok := m.byNode[senderID]; ok {
		return &gastrologv1.NodeStats{RaftAppliedIndex: v}
	}
	return nil
}

// TestClusterCtlPromoter_NoLearners_NoOp: a configuration of all voters
// yields no promotion.
func TestClusterCtlPromoter_NoLearners_NoOp(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "n1", Address: "addr-1", Suffrage: "Voter"},
			{ID: "n2", Address: "addr-2", Suffrage: "Voter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"n1": 100, "n2": 100}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	if got := srv.voterCalls(); len(got) != 0 {
		t.Fatalf("expected no AddVoter when no learners, got %v", got)
	}
}

// TestClusterCtlPromoter_CaughtUpLearnerPromoted: a Nonvoter that has
// reached the leader's applied index is promoted on the first pass.
func TestClusterCtlPromoter_CaughtUpLearnerPromoted(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "learner", Address: "addr-x", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"learner": 100}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	got := srv.voterCalls()
	if len(got) != 1 || got[0].id != "learner" || got[0].addr != "addr-x" {
		t.Fatalf("expected one AddVoter(learner, addr-x), got %v", got)
	}
}

// TestClusterCtlPromoter_LaggingLearnerHeldOff: a Nonvoter behind the
// leader (tolerance 0) is not promoted.
func TestClusterCtlPromoter_LaggingLearnerHeldOff(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "lagger", Address: "addr-y", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"lagger": 50}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	if got := srv.voterCalls(); len(got) != 0 {
		t.Fatalf("expected no AddVoter for lagging learner, got %v", got)
	}
}

// TestClusterCtlPromoter_NoPeerStatsBlocksPromotion: a learner with no
// broadcast evidence is never promoted.
func TestClusterCtlPromoter_NoPeerStatsBlocksPromotion(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "ghost", Address: "addr-g", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	if got := srv.voterCalls(); len(got) != 0 {
		t.Fatalf("expected no AddVoter for unobserved learner, got %v", got)
	}
}

// TestClusterCtlPromoter_StagingTreatedAsLearner: hraft's transient
// "Staging" suffrage (between AddNonvoter and commit) counts as a learner.
func TestClusterCtlPromoter_StagingTreatedAsLearner(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "staging", Address: "addr-s", Suffrage: "Staging"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"staging": 100}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	got := srv.voterCalls()
	if len(got) != 1 || got[0].id != "staging" {
		t.Fatalf("expected Staging member promoted, got %v", got)
	}
}

// TestClusterCtlPromoter_NonLeaderNoOp: a follower never proposes AddVoter,
// even with a caught-up learner in the configuration.
func TestClusterCtlPromoter_NonLeaderNoOp(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: false, appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "n1", Address: "addr-1", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"n1": 100}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	if got := srv.voterCalls(); len(got) != 0 {
		t.Fatalf("non-leader: AddVoter called %v", got)
	}
}

// TestClusterCtlPromoter_LeaderAppliedZeroSkips: a leader with no applied
// entries yet has nothing to compare against.
func TestClusterCtlPromoter_LeaderAppliedZeroSkips(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 0,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "learner", Address: "addr-x", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"learner": 0}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	if got := srv.voterCalls(); len(got) != 0 {
		t.Fatalf("leaderApplied==0 must skip, got %v", got)
	}
}

// TestClusterCtlPromoter_AddVoterFailRetries: a transient AddVoter failure
// is retried on the next pass while the learner is still caught up.
func TestClusterCtlPromoter_AddVoterFailRetries(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 100,
		addVoterErr: errFakeMember,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "stuck", Address: "addr-z", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"stuck": 100}}
	p := newClusterCtlLearnerPromoter(srv, ps, quietLogger())

	p.evaluate()
	if got := srv.voterCalls(); len(got) != 1 {
		t.Fatalf("expected one (failed) AddVoter attempt, got %v", got)
	}

	srv.mu.Lock()
	srv.addVoterErr = nil
	srv.mu.Unlock()
	p.evaluate()
	if got := srv.voterCalls(); len(got) != 2 {
		t.Fatalf("expected retry to fire, got %v", got)
	}
}

// TestClusterCtlPromoter_ServersErrorNoOp: a Servers() error yields no
// learners and no promotion (the error is logged, not fatal).
func TestClusterCtlPromoter_ServersErrorNoOp(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader: true, appliedIndex: 100, serversErr: errFakeMember,
	}
	ps := &mockPeerStats{byNode: map[string]uint64{}}
	newClusterCtlLearnerPromoter(srv, ps, quietLogger()).evaluate()

	if got := srv.voterCalls(); len(got) != 0 {
		t.Fatalf("expected no AddVoter on Servers() error, got %v", got)
	}
}

// TestLocalAppliedIndex_ParsesStats verifies the helper's parsing path
// and its nil-safety.
func TestLocalAppliedIndex_ParsesStats(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{appliedIndex: 42}
	if got := localAppliedIndex(srv); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}
	if got := localAppliedIndex(&mockRaftMembership{statsMissing: true}); got != 0 {
		t.Fatalf("expected 0 for missing stats, got %d", got)
	}
	if got := localAppliedIndex(nil); got != 0 {
		t.Fatalf("expected 0 for nil membership, got %d", got)
	}
}
