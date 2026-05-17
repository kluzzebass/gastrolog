package app

import (
	"context"
	"io"
	"log/slog"
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
	mu             sync.Mutex
	isLeader       bool
	servers        []cluster.RaftServer
	serversErr     error
	addVoterErr    error
	appliedIndex   uint64
	statsMissing   bool
	addVoterCalls  []addVoterCall
	statsCallCount int
}

type addVoterCall struct {
	id   string
	addr string
}

func (m *mockRaftMembership) IsLeader() bool { return m.isLeader }
func (m *mockRaftMembership) Servers() ([]cluster.RaftServer, error) {
	return append([]cluster.RaftServer(nil), m.servers...), m.serversErr
}

func (m *mockRaftMembership) AddVoter(id, addr string, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.addVoterCalls = append(m.addVoterCalls, addVoterCall{id: id, addr: addr})
	if m.addVoterErr != nil {
		return m.addVoterErr
	}
	// Reflect successful promotion in the configuration the next tick
	// will read — so the test can verify the learner is gone from the
	// catchup-ticks map after promotion completes.
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
	m.statsCallCount++
	if m.statsMissing {
		return nil
	}
	return map[string]string{"applied_index": strconv.FormatUint(m.appliedIndex, 10)}
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

func newPromoterForTest(srv raftMembership, ps peerStatsReader) *systemLearnerPromoter {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return newSystemLearnerPromoter(srv, ps, logger)
}

func TestSystemLearnerPromoter_NoLearners_NoOp(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "n1", Address: "addr-1", Suffrage: "Voter"},
			{ID: "n2", Address: "addr-2", Suffrage: "Voter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"n1": 100, "n2": 100}}
	p := newPromoterForTest(srv, ps)

	p.tick(context.Background())

	if len(srv.addVoterCalls) != 0 {
		t.Fatalf("expected no AddVoter when no learners, got %v", srv.addVoterCalls)
	}
}

func TestSystemLearnerPromoter_CaughtUpLearnerPromotedAfterStabilityWindow(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "learner", Address: "addr-x", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"learner": 100}}
	p := newPromoterForTest(srv, ps)

	// Tick 1: learner is caught up, but stability requires 2 — no promotion.
	p.tick(context.Background())
	if len(srv.addVoterCalls) != 0 {
		t.Fatalf("tick 1: expected no AddVoter (need %d ticks), got %v",
			p.stabilityRequired, srv.addVoterCalls)
	}
	if p.catchupTicks["learner"] != 1 {
		t.Fatalf("tick 1: expected catchupTicks[learner]=1, got %d", p.catchupTicks["learner"])
	}

	// Tick 2: stability window satisfied — promote.
	p.tick(context.Background())
	if len(srv.addVoterCalls) != 1 || srv.addVoterCalls[0].id != "learner" {
		t.Fatalf("tick 2: expected one AddVoter for learner, got %v", srv.addVoterCalls)
	}
	if srv.addVoterCalls[0].addr != "addr-x" {
		t.Fatalf("AddVoter passed wrong address: %q", srv.addVoterCalls[0].addr)
	}
	// Counter is cleared post-promotion.
	if _, present := p.catchupTicks["learner"]; present {
		t.Fatalf("expected catchupTicks[learner] cleared after promotion, present=%v", p.catchupTicks["learner"])
	}
}

func TestSystemLearnerPromoter_LaggingLearnerHeldOff(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "lagger", Address: "addr-y", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"lagger": 50}} // behind
	p := newPromoterForTest(srv, ps)

	for range 5 {
		p.tick(context.Background())
	}

	if len(srv.addVoterCalls) != 0 {
		t.Fatalf("expected no AddVoter for lagging learner, got %v", srv.addVoterCalls)
	}
	if p.catchupTicks["lagger"] != 0 {
		t.Fatalf("expected catchupTicks[lagger]=0, got %d", p.catchupTicks["lagger"])
	}
}

// TestSystemLearnerPromoter_TransientLagResets verifies the stability
// counter resets to zero on a single non-caught-up tick. A flaky
// learner that flickers between caught-up and lagging must complete
// a CONTIGUOUS stability window — never sneaking promotion via
// intermittent observations.
func TestSystemLearnerPromoter_TransientLagResets(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "flaky", Address: "addr-z", Suffrage: "Nonvoter"},
		},
	}
	// Tick 1: caught up.
	ps := &mockPeerStats{byNode: map[string]uint64{"flaky": 100}}
	p := newPromoterForTest(srv, ps)
	p.tick(context.Background())
	if p.catchupTicks["flaky"] != 1 {
		t.Fatalf("tick 1: expected catchupTicks=1, got %d", p.catchupTicks["flaky"])
	}

	// Tick 2: lagging (transient). Counter must reset.
	ps.byNode["flaky"] = 80
	p.tick(context.Background())
	if p.catchupTicks["flaky"] != 0 {
		t.Fatalf("tick 2 (lag): expected catchupTicks=0 after lag, got %d", p.catchupTicks["flaky"])
	}
	if len(srv.addVoterCalls) != 0 {
		t.Fatalf("expected no AddVoter mid-flake, got %v", srv.addVoterCalls)
	}

	// Tick 3+4: caught up again. Must complete a fresh window.
	ps.byNode["flaky"] = 100
	p.tick(context.Background())
	if len(srv.addVoterCalls) != 0 {
		t.Fatalf("tick 3: expected no AddVoter (fresh window not done), got %v", srv.addVoterCalls)
	}
	p.tick(context.Background())
	if len(srv.addVoterCalls) != 1 {
		t.Fatalf("tick 4: expected promotion, got %v", srv.addVoterCalls)
	}
}

// TestSystemLearnerPromoter_NoPeerStatsBlocksPromotion verifies that a
// learner with no recent NodeStats broadcast (no PeerState entry) is
// never promoted — even if the leader's own applied_index is 0 (which
// the early-out for "no log applied yet" guards), the absent stats
// imply the leader has no evidence to act on.
func TestSystemLearnerPromoter_NoPeerStatsBlocksPromotion(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "ghost", Address: "addr-g", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{}} // no entry for ghost
	p := newPromoterForTest(srv, ps)

	for range 5 {
		p.tick(context.Background())
	}

	if len(srv.addVoterCalls) != 0 {
		t.Fatalf("expected no AddVoter for unobserved learner, got %v", srv.addVoterCalls)
	}
}

// TestSystemLearnerPromoter_StagingTreatedAsLearner verifies the
// "Staging" suffrage (hraft's transient state during AddVoter) is
// treated as a learner — Staging is what hashicorp/raft reports
// during the brief window between AddNonvoter and the membership
// commit landing. The promoter must not race itself by skipping
// these and then re-issuing AddVoter.
func TestSystemLearnerPromoter_StagingTreatedAsLearner(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "staging", Address: "addr-s", Suffrage: "Staging"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"staging": 100}}
	p := newPromoterForTest(srv, ps)

	p.tick(context.Background())
	if p.catchupTicks["staging"] != 1 {
		t.Fatalf("expected Staging member counted, got catchupTicks=%d", p.catchupTicks["staging"])
	}
}

// TestSystemLearnerPromoter_GoneLearnerCounterCleaned verifies that
// when a learner leaves the configuration (promoted via another path,
// or removed entirely) its tick-counter entry is cleaned up — the
// map must not grow unboundedly across cluster scale-ups.
func TestSystemLearnerPromoter_GoneLearnerCounterCleaned(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "ephemeral", Address: "addr-e", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"ephemeral": 100}}
	p := newPromoterForTest(srv, ps)

	// Tick once — counter goes to 1.
	p.tick(context.Background())
	if p.catchupTicks["ephemeral"] != 1 {
		t.Fatalf("setup: expected catchupTicks=1, got %d", p.catchupTicks["ephemeral"])
	}

	// Simulate the learner disappearing from the Raft configuration.
	srv.servers = srv.servers[:1] // only the leader remains

	p.tick(context.Background())
	if _, present := p.catchupTicks["ephemeral"]; present {
		t.Fatalf("expected catchupTicks[ephemeral] cleaned after departure")
	}
}

// TestSystemLearnerPromoter_AddVoterFailDoesNotResetCounter verifies
// that a transient AddVoter failure (Raft quorum hiccup, slow commit)
// leaves the catchup-tick counter intact so the next tick can retry
// without forcing the operator through another full stability window.
func TestSystemLearnerPromoter_AddVoterFailDoesNotResetCounter(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		addVoterErr:  context.DeadlineExceeded,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "stuck", Address: "addr-z", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"stuck": 100}}
	p := newPromoterForTest(srv, ps)

	p.tick(context.Background()) // counter=1, no AddVoter
	p.tick(context.Background()) // counter=2, AddVoter fails
	if len(srv.addVoterCalls) != 1 {
		t.Fatalf("expected one AddVoter attempt, got %v", srv.addVoterCalls)
	}
	if p.catchupTicks["stuck"] < 2 {
		t.Fatalf("expected counter preserved across AddVoter failure, got %d", p.catchupTicks["stuck"])
	}

	// Next tick: AddVoter succeeds now.
	srv.addVoterErr = nil
	p.tick(context.Background())
	if len(srv.addVoterCalls) != 2 {
		t.Fatalf("expected retry to fire, got %v", srv.addVoterCalls)
	}
}

// TestLocalAppliedIndex_ParsesStats verifies the helper's parsing path
// and its nil-safety. The leader's applied_index comes from
// hraft.Stats() which returns string-typed values.
func TestLocalAppliedIndex_ParsesStats(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{appliedIndex: 42}
	if got := localAppliedIndex(srv); got != 42 {
		t.Fatalf("expected 42, got %d", got)
	}

	srv2 := &mockRaftMembership{statsMissing: true}
	if got := localAppliedIndex(srv2); got != 0 {
		t.Fatalf("expected 0 for missing stats, got %d", got)
	}

	if got := localAppliedIndex(nil); got != 0 {
		t.Fatalf("expected 0 for nil membership, got %d", got)
	}
}

// TestTickOnce_NonLeaderIsNoOp verifies the scheduler can fire the
// promoter task on every node — only the system-Raft leader runs the
// actual promotion logic. Followers must short-circuit before any
// catchup bookkeeping or AddVoter call.
func TestTickOnce_NonLeaderIsNoOp(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     false,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "n1", Address: "addr-1", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"n1": 100}}
	p := newPromoterForTest(srv, ps)

	p.tickOnce(context.Background())

	if len(srv.addVoterCalls) != 0 {
		t.Errorf("non-leader: AddVoter called %v", srv.addVoterCalls)
	}
	if got := p.catchupTicks["n1"]; got != 0 {
		t.Errorf("non-leader: catchup advanced (got %d) — leader gate failed", got)
	}
}

// TestStartSystemLearnerPromoter_RegistersOperatorVisibleJob verifies
// the promoter ships as a proper scheduled job: name + cron set, and
// a non-empty Describe text so the inspector shows context to the
// operator.
func TestStartSystemLearnerPromoter_RegistersOperatorVisibleJob(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{
		isLeader:     true,
		appliedIndex: 100,
		servers: []cluster.RaftServer{
			{ID: "leader", Address: "addr-l", Suffrage: "Voter"},
			{ID: "n1", Address: "addr-1", Suffrage: "Nonvoter"},
		},
	}
	ps := &mockPeerStats{byNode: map[string]uint64{"n1": 100}}
	p := newPromoterForTest(srv, ps)
	sched := &fakeScheduler{}

	if err := startSystemLearnerPromoter(context.Background(), sched, p); err != nil {
		t.Fatalf("startSystemLearnerPromoter: %v", err)
	}
	if sched.addJobName != systemLearnerPromoterJobName {
		t.Errorf("AddJob name: got %q, want %q", sched.addJobName, systemLearnerPromoterJobName)
	}
	if sched.addJobCron != systemLearnerPromoterSchedule {
		t.Errorf("AddJob cron: got %q, want %q", sched.addJobCron, systemLearnerPromoterSchedule)
	}
	if sched.describeMessage == "" {
		t.Error("Describe message empty — operator inspector will show no context")
	}

	// Run the captured task as the scheduler would. A single tick on
	// a caught-up learner advances catchupTicks to 1 (stability=2);
	// no promotion yet, but proof the leader-side tick ran.
	if task, ok := sched.addJobTaskFn.(func()); ok {
		task()
	} else {
		t.Fatalf("expected captured task of type func(), got %T", sched.addJobTaskFn)
	}
	if got := p.catchupTicks["n1"]; got != 1 {
		t.Errorf("after task run: expected catchupTicks[n1]=1, got %d", got)
	}
}

// TestStartSystemLearnerPromoter_PropagatesAddJobError verifies the
// caller sees an AddJob failure (e.g. duplicate name).
func TestStartSystemLearnerPromoter_PropagatesAddJobError(t *testing.T) {
	t.Parallel()
	srv := &mockRaftMembership{isLeader: true, appliedIndex: 1}
	ps := &mockPeerStats{byNode: map[string]uint64{}}
	p := newPromoterForTest(srv, ps)
	sched := &fakeScheduler{addJobErr: errFakeMember}

	if err := startSystemLearnerPromoter(context.Background(), sched, p); err == nil {
		t.Fatal("expected AddJob error to propagate")
	}
}
