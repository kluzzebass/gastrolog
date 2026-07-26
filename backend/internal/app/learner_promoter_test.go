package app

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakePromotionGroup is a scriptable promotionGroup that records promote
// calls. It models one Raft group's observable state so the engine can be
// driven without a real Raft node.
type fakePromotionGroup struct {
	mu            sync.Mutex
	name          string
	leader        bool
	applied       uint64
	tol           uint64
	members       []learnerMember
	observed      map[string]uint64 // nodeID -> broadcast applied; absent = no evidence
	promoteErr    error
	promoted      []string
	promoteRemove bool // when true, a promoted member leaves the learner set
}

func (g *fakePromotionGroup) label() string { return g.name }

func (g *fakePromotionGroup) isLeader() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.leader
}

func (g *fakePromotionGroup) leaderApplied() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.applied
}

func (g *fakePromotionGroup) tolerance() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.tol
}

func (g *fakePromotionGroup) learners() []learnerMember {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]learnerMember(nil), g.members...)
}

func (g *fakePromotionGroup) observedApplied(nodeID string) (uint64, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	v, ok := g.observed[nodeID]
	return v, ok
}

func (g *fakePromotionGroup) promote(m learnerMember) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.promoted = append(g.promoted, m.nodeID)
	if g.promoteErr != nil {
		return g.promoteErr
	}
	if g.promoteRemove {
		kept := g.members[:0]
		for _, mem := range g.members {
			if mem.nodeID != m.nodeID {
				kept = append(kept, mem)
			}
		}
		g.members = kept
	}
	return nil
}

func (g *fakePromotionGroup) promotedIDs() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]string(nil), g.promoted...)
}

func promoterOver(groups ...promotionGroup) *learnerPromoter {
	gs := groups
	return newLearnerPromoter("test", func() []promotionGroup { return gs }, quietLogger())
}

// TestEvaluate_PromotesCaughtUpLearner: a single caught-up learner is
// promoted on one evaluation pass — no stability window, no waiting for a
// second observation. The learner's own broadcast of a real applied index
// is trustworthy evidence.
func TestEvaluate_PromotesCaughtUpLearner(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: true, applied: 100, tol: 0,
		members:  []learnerMember{{nodeID: "learner", addr: "addr-x"}},
		observed: map[string]uint64{"learner": 100},
	}
	promoterOver(g).evaluate()

	if got := g.promotedIDs(); len(got) != 1 || got[0] != "learner" {
		t.Fatalf("expected learner promoted once, got %v", got)
	}
}

// TestEvaluate_LaggingLearnerHeldOff: a learner behind the leader by more
// than the tolerance is not promoted. This is the "NOT promoted before
// threshold" guarantee.
func TestEvaluate_LaggingLearnerHeldOff(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: true, applied: 100, tol: 10,
		members:  []learnerMember{{nodeID: "lagger", addr: "a"}},
		observed: map[string]uint64{"lagger": 80}, // 80+10 < 100
	}
	promoterOver(g).evaluate()

	if got := g.promotedIDs(); len(got) != 0 {
		t.Fatalf("expected no promotion for lagging learner, got %v", got)
	}
}

// TestEvaluate_ToleranceBoundary: applied+tolerance == leaderApplied counts
// as caught up (>= boundary, inclusive).
func TestEvaluate_ToleranceBoundary(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: true, applied: 100, tol: 20,
		members:  []learnerMember{{nodeID: "edge", addr: "a"}},
		observed: map[string]uint64{"edge": 80}, // 80+20 == 100
	}
	promoterOver(g).evaluate()

	if got := g.promotedIDs(); len(got) != 1 {
		t.Fatalf("expected promotion at exact tolerance boundary, got %v", got)
	}
}

// TestEvaluate_NoEvidenceBlocksPromotion: a learner with no broadcast
// evidence (never reported an applied index for this group) is never
// promoted, even if the leader is ready.
func TestEvaluate_NoEvidenceBlocksPromotion(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: true, applied: 100, tol: 0,
		members:  []learnerMember{{nodeID: "ghost", addr: "a"}},
		observed: map[string]uint64{}, // no evidence
	}
	promoterOver(g).evaluate()

	if got := g.promotedIDs(); len(got) != 0 {
		t.Fatalf("expected no promotion without evidence, got %v", got)
	}
}

// TestEvaluate_NonLeaderNoOp: a group this node does not lead is skipped
// entirely — no promote attempt.
func TestEvaluate_NonLeaderNoOp(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: false, applied: 100, tol: 0,
		members:  []learnerMember{{nodeID: "learner", addr: "a"}},
		observed: map[string]uint64{"learner": 100},
	}
	promoterOver(g).evaluate()

	if got := g.promotedIDs(); len(got) != 0 {
		t.Fatalf("non-leader must not promote, got %v", got)
	}
}

// TestEvaluate_LeaderAppliedZeroSkips: a fresh leader with no applied
// entries has nothing to compare against and skips the group.
func TestEvaluate_LeaderAppliedZeroSkips(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: true, applied: 0, tol: 0,
		members:  []learnerMember{{nodeID: "learner", addr: "a"}},
		observed: map[string]uint64{"learner": 0},
	}
	promoterOver(g).evaluate()

	if got := g.promotedIDs(); len(got) != 0 {
		t.Fatalf("leaderApplied==0 must skip, got %v", got)
	}
}

// TestEvaluate_OnePromotionPerGroupPerPass: with several caught-up learners
// in one group, only one is promoted per pass (Raft commits configuration
// changes one at a time); the next pass promotes the next.
func TestEvaluate_OnePromotionPerGroupPerPass(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: true, applied: 100, tol: 0, promoteRemove: true,
		members: []learnerMember{
			{nodeID: "a", addr: "a"},
			{nodeID: "b", addr: "b"},
			{nodeID: "c", addr: "c"},
		},
		observed: map[string]uint64{"a": 100, "b": 100, "c": 100},
	}
	p := promoterOver(g)

	p.evaluate()
	if got := g.promotedIDs(); len(got) != 1 {
		t.Fatalf("pass 1: expected exactly one promotion, got %v", got)
	}
	p.evaluate()
	if got := g.promotedIDs(); len(got) != 2 {
		t.Fatalf("pass 2: expected two promotions total, got %v", got)
	}
	p.evaluate()
	if got := g.promotedIDs(); len(got) != 3 {
		t.Fatalf("pass 3: expected three promotions total, got %v", got)
	}
}

// TestEvaluate_IndependentGroupsEachPromoteInOnePass: different groups are
// independent Raft configurations, so one promotion in each is safe in the
// same pass.
func TestEvaluate_IndependentGroupsEachPromoteInOnePass(t *testing.T) {
	t.Parallel()
	g1 := &fakePromotionGroup{
		name: "g1", leader: true, applied: 50, tol: 0,
		members:  []learnerMember{{nodeID: "x", addr: "x"}},
		observed: map[string]uint64{"x": 50},
	}
	g2 := &fakePromotionGroup{
		name: "g2", leader: true, applied: 70, tol: 0,
		members:  []learnerMember{{nodeID: "y", addr: "y"}},
		observed: map[string]uint64{"y": 70},
	}
	promoterOver(g1, g2).evaluate()

	if got := g1.promotedIDs(); len(got) != 1 {
		t.Fatalf("g1: expected one promotion, got %v", got)
	}
	if got := g2.promotedIDs(); len(got) != 1 {
		t.Fatalf("g2: expected one promotion, got %v", got)
	}
}

// TestEvaluate_PromoteFailureRetries: a failed AddVoter leaves the learner
// in place; a subsequent pass retries while it is still caught up.
func TestEvaluate_PromoteFailureRetries(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: true, applied: 100, tol: 0,
		members:    []learnerMember{{nodeID: "stuck", addr: "a"}},
		observed:   map[string]uint64{"stuck": 100},
		promoteErr: context.DeadlineExceeded,
	}
	p := promoterOver(g)

	p.evaluate()
	if got := g.promotedIDs(); len(got) != 1 {
		t.Fatalf("expected one (failed) attempt, got %v", got)
	}
	// Failure cleared — next pass retries and succeeds.
	g.mu.Lock()
	g.promoteErr = nil
	g.mu.Unlock()
	p.evaluate()
	if got := g.promotedIDs(); len(got) != 2 {
		t.Fatalf("expected retry attempt, got %v", got)
	}
}

// TestOnBroadcast_TriggersOnlyOnNodeStats: the broadcast subscriber wakes
// an evaluation on NodeStats (which carries applied index) but ignores
// heartbeats (which cannot change a promotion decision).
func TestOnBroadcast_TriggersOnlyOnNodeStats(t *testing.T) {
	t.Parallel()
	p := newLearnerPromoter("test", func() []promotionGroup { return nil }, quietLogger())

	// NodeStats → trigger (channel closes).
	before := p.wake.C()
	p.onBroadcast(&gastrologv1.BroadcastMessage{
		Payload: &gastrologv1.BroadcastMessage_NodeStats{NodeStats: &gastrologv1.NodeStats{}},
	})
	select {
	case <-before:
	default:
		t.Fatal("NodeStats broadcast should have triggered the promoter")
	}

	// Heartbeat → no trigger (channel stays open).
	after := p.wake.C()
	p.onBroadcast(&gastrologv1.BroadcastMessage{
		Payload: &gastrologv1.BroadcastMessage_Heartbeat{Heartbeat: &gastrologv1.Heartbeat{}},
	})
	select {
	case <-after:
		t.Fatal("heartbeat broadcast must not trigger the promoter")
	default:
	}
}

// TestRun_PromotesOnTrigger drives the engine through its Run loop: a
// learner that is lagging when Run starts is promoted only after it catches
// up and a trigger fires — proving the event path, with no sleeps (we wait
// on the promote signal, not the clock).
func TestRun_PromotesOnTrigger(t *testing.T) {
	t.Parallel()
	promoted := make(chan string, 4)
	g := &signalGroup{
		fakePromotionGroup: fakePromotionGroup{
			name: "g", leader: true, applied: 100, tol: 0,
			members:  []learnerMember{{nodeID: "learner", addr: "a"}},
			observed: map[string]uint64{"learner": 50}, // lagging initially
		},
		promotedCh: promoted,
	}
	p := promoterOver(g)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)

	// Initial evaluate (inside Run) sees a lagging learner → no promotion.
	// Bring it up to date, then fire the event.
	g.mu.Lock()
	g.observed["learner"] = 100
	g.mu.Unlock()
	p.trigger()

	select {
	case id := <-promoted:
		if id != "learner" {
			t.Fatalf("promoted wrong node: %q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("learner not promoted after catch-up trigger")
	}
}

// TestEvaluate_LeadershipChangeMidCatchup: a node that is not (yet) the
// leader promotes nothing even with a caught-up learner; once it holds
// leadership the next pass promotes it. Deterministic — both phases are
// synchronous evaluate() calls, no clock. Models the placement-driven
// leader transfer case.
func TestEvaluate_LeadershipChangeMidCatchup(t *testing.T) {
	t.Parallel()
	g := &fakePromotionGroup{
		name: "g", leader: false, applied: 100, tol: 0, // not leader yet
		members:  []learnerMember{{nodeID: "learner", addr: "a"}},
		observed: map[string]uint64{"learner": 100}, // already caught up
	}
	p := promoterOver(g)

	p.evaluate() // not leader → nothing
	if got := g.promotedIDs(); len(got) != 0 {
		t.Fatalf("non-leader must not promote, got %v", got)
	}

	g.mu.Lock()
	g.leader = true
	g.mu.Unlock()

	p.evaluate() // now leader → promote
	if got := g.promotedIDs(); len(got) != 1 || got[0] != "learner" {
		t.Fatalf("expected promotion after gaining leadership, got %v", got)
	}
}

// signalGroup is a fakePromotionGroup that reports each promotion on a
// channel so Run-driven tests can await the event without sleeping.
type signalGroup struct {
	fakePromotionGroup
	promotedCh chan string
}

func (g *signalGroup) promote(m learnerMember) error {
	err := g.fakePromotionGroup.promote(m)
	if err == nil {
		g.promotedCh <- m.nodeID
	}
	return err
}
