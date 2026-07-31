package app

import (
	"context"
	"log/slog"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/notify"

	hraft "github.com/hashicorp/raft"
)

// learnerMember is one Nonvoter / Staging member the evaluator may
// promote to Voter.
type learnerMember struct {
	nodeID string
	addr   string
}

// promotionGroup is one Raft group the local node might lead, expressed
// in the narrow terms the promotion evaluator needs. The cluster-ctl
// group (a single cluster.Server) and each per-vault vault-ctl
// raftgroup.Group implement it, so a single evaluator drives both group
// families.
type promotionGroup interface {
	// label identifies the group in logs ("cluster-ctl" or a vault ID).
	label() string
	// isLeader reports whether the local node currently leads this group.
	// Only the leader may propose a membership change, so a non-leader
	// group is skipped entirely.
	isLeader() bool
	// leaderApplied is the local (leader) applied index. 0 means "no log
	// applied yet — nothing to compare against", and the group is skipped.
	leaderApplied() uint64
	// learners lists the group's current Nonvoter / Staging members.
	learners() []learnerMember
	// observedApplied returns the learner's last broadcast applied index
	// for this group and whether any evidence exists yet. False means the
	// leader has never seen the learner report progress for this group, so
	// it must not promote (no evidence of catch-up).
	observedApplied(nodeID string) (uint64, bool)
	// tolerance is the catch-up slack, in log entries, subtracted from the
	// gap between the leader's live applied index and the learner's last
	// broadcast one. An active group commits faster than the broadcast
	// interval, so a healthy learner's last report always lags the leader
	// slightly; the tolerance absorbs exactly that lag. Zero for the
	// low-churn cluster-ctl group.
	tolerance() uint64
	// promote issues the AddVoter membership change for the learner.
	promote(m learnerMember) error
}

// learnerPromoter promotes caught-up Raft learners (Nonvoter / Staging
// members) to voters for one group family. It is event-driven: an
// evaluation pass runs only when the promotion decision could have
// changed — never on a wall-clock cron.
//
// The catch-up signal is the peer NodeStats broadcast. A learner's
// applied index reaches the leader ONLY via that broadcast:
// hashicorp/raft exposes no per-peer match index and emits no
// replication-progress observation (v1.7.3 observations are
// RequestVote / RaftState / PeerObservation / LeaderObservation only).
// So the arrival of a fresh broadcast carrying a higher applied index IS
// the "learner made progress" event, and evaluating on it is what makes
// this event-driven rather than a poll.
//
// Discrete triggers complement the broadcast:
//   - leadership gained: a new leader must (re-)evaluate learners that
//     may already be caught up from a previous epoch;
//   - learner added: reflected by the next broadcast, and — for
//     vault-ctl — by the same leader epoch that added it.
//
// Every trigger coalesces into one signal, so a burst of broadcasts
// collapses to a single evaluation pass. No per-group timer exists even
// though vault-ctl has many groups: one subscription and one signal
// cover the whole family, and evaluate() fans out over the currently-led
// groups on demand.
type learnerPromoter struct {
	family string
	groups func() []promotionGroup
	wake   *notify.Signal
	logger *slog.Logger
}

// newLearnerPromoter constructs an evaluator for one group family.
// groups enumerates the currently-relevant groups on each pass (a
// single element for cluster-ctl; the vaults this node hosts for
// vault-ctl). family labels log lines.
func newLearnerPromoter(family string, groups func() []promotionGroup, logger *slog.Logger) *learnerPromoter {
	return &learnerPromoter{
		family: family,
		groups: groups,
		wake:   notify.NewSignal(),
		logger: logger,
	}
}

// Run evaluates once immediately (so a learner already caught up at
// wiring time isn't stranded until the first broadcast), then blocks
// evaluating on every trigger until ctx is cancelled. Intended to run in
// its own goroutine.
//
// The wake channel is re-armed BEFORE each evaluation so a trigger that
// fires while evaluate() runs is not lost — it wakes the very next select
// instead of waiting for a subsequent broadcast.
func (p *learnerPromoter) Run(ctx context.Context) {
	wake := p.wake.C()
	p.evaluate()
	for {
		select {
		case <-ctx.Done():
			return
		case <-wake:
			wake = p.wake.C()
			p.evaluate()
		}
	}
}

// trigger requests an evaluation pass. Non-blocking; coalesces with any
// other pending trigger.
func (p *learnerPromoter) trigger() { p.wake.Notify() }

// onBroadcast is a cluster-broadcast subscriber. Only the heavy NodeStats
// payload carries a peer's applied index, so heartbeats (which only
// refresh liveness) are ignored — they can't change a promotion decision.
func (p *learnerPromoter) onBroadcast(msg *gastrologv1.BroadcastMessage) {
	if msg.GetNodeStats() != nil {
		p.trigger()
	}
}

// evaluate runs one promotion pass over every currently-led group. Each
// group promotes at most one learner per pass because hashicorp/raft
// commits configuration changes one at a time; a second caught-up learner
// in the same group is promoted on the next trigger. Different groups are
// independent Raft configurations, so one promotion each in the same pass
// is safe.
func (p *learnerPromoter) evaluate() {
	for _, g := range p.groups() {
		if !g.isLeader() {
			continue
		}
		leaderApplied := g.leaderApplied()
		if leaderApplied == 0 {
			continue
		}
		p.evaluateGroup(g, leaderApplied)
	}
}

// evaluateGroup promotes the first caught-up learner in one group, if any.
func (p *learnerPromoter) evaluateGroup(g promotionGroup, leaderApplied uint64) {
	for _, m := range g.learners() {
		applied, ok := g.observedApplied(m.nodeID)
		if !ok {
			// No broadcast evidence yet — the leader can't prove this
			// learner has replicated anything, so it can't promote.
			continue
		}
		if applied+g.tolerance() < leaderApplied {
			p.logger.Debug("learner_promoter: learner lagging",
				"family", p.family, "group", g.label(), "node", m.nodeID,
				"learner_applied", applied, "leader_applied", leaderApplied,
				"tolerance", g.tolerance())
			continue
		}
		if err := g.promote(m); err != nil {
			p.logger.Warn("learner_promoter: AddVoter failed",
				"family", p.family, "group", g.label(), "node", m.nodeID,
				"addr", m.addr, "error", err)
			// Leave the learner in place; the next trigger retries while
			// it is still caught up.
			continue
		}
		p.logger.Info("learner_promoter: promoted learner to voter",
			"family", p.family, "group", g.label(), "node", m.nodeID,
			"addr", m.addr, "leader_applied", leaderApplied)
		return
	}
}

// observeLeadershipGain triggers an evaluation pass whenever the
// cluster-ctl node's Raft leadership changes. A node that gains leadership
// inherits learners that may already have caught up under the previous
// leader; without this it would wait for the next broadcast to notice.
// Runs until ctx is cancelled. (vault-ctl leadership gain is wired
// separately via the per-vault leader epoch — see app.go.)
func observeLeadershipGain(ctx context.Context, clusterSrv *cluster.Server, p interface{ trigger() }) {
	ch := make(chan hraft.Observation, 4)
	clusterSrv.RegisterLeaderObserver(ch)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ch:
				p.trigger()
			}
		}
	}()
}
