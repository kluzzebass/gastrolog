package app

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
)

const (
	// systemLearnerPromoterInterval is the leader-side tick cadence.
	// Mirrors the unreachable sweep (30s); both are slow, leader-only
	// scans that consult Raft membership state.
	systemLearnerPromoterInterval = 30 * time.Second

	// systemLearnerStabilityTicks is the number of consecutive ticks a
	// learner must be observed at caught-up state before promotion.
	// Guards against transient apply-index parity caused by gossip lag
	// or a brief stall in the leader's own apply pipeline. Two ticks at
	// 30s gives ~60s of sustained-catchup confirmation.
	systemLearnerStabilityTicks = 2

	// systemLearnerPromoteTimeout bounds each AddVoter membership-change
	// commit. The change is small (one config entry) so this is mostly
	// a quorum-loss guard; a healthy cluster commits in milliseconds.
	systemLearnerPromoteTimeout = 5 * time.Second
)

// raftMembership is the subset of cluster.Server methods the
// learner promoter needs. Defined at the consumer site so tests
// can supply a mock without standing up a real Raft node.
type raftMembership interface {
	IsLeader() bool
	Servers() ([]cluster.RaftServer, error)
	AddVoter(id, addr string, timeout time.Duration) error
	LocalStats() map[string]string
}

// peerStatsReader is the subset of cluster.PeerState used by the
// promoter — same consumer-site narrowing for testability.
type peerStatsReader interface {
	Get(senderID string) *gastrologv1.NodeStats
}

// systemLearnerPromoter promotes system-Raft learners (Nonvoter /
// Staging members) to voters once they have caught up to the leader's
// applied index and held that state across the stability window.
// Runs on the system-Raft leader only.
//
// Companion to the per-vault-ctl learner promoter (gastrolog-gcbx7)
// and the JoinCluster-as-learner change (gastrolog-41sut). The trio
// implements the "new nodes join as non-voters, get promoted once
// they have replicated the existing log" semantic — protecting the
// cluster from quorum-loss windows where a fresh joiner has been
// counted as a voter but cannot yet vote on Raft entries because
// its WAL is still catching up.
//
// The "caught up" signal comes from the existing NodeStats broadcast
// (cluster.PeerState): every node publishes its own
// RaftAppliedIndex once per stats interval, and the leader compares
// each learner's last-reported index against its own. This avoids
// needing a new RPC purely for catchup probing; the heartbeat fabric
// we already maintain answers the question.
type systemLearnerPromoter struct {
	cluster           raftMembership
	peerState         peerStatsReader
	logger            *slog.Logger
	interval          time.Duration
	stabilityRequired int

	// catchupTicks tracks consecutive caught-up observations per
	// learner. Cleared on a single non-caught-up tick so a flaky
	// learner never sneaks past the stability window.
	catchupTicks map[string]int
	now          func() time.Time
}

func newSystemLearnerPromoter(c raftMembership, ps peerStatsReader, logger *slog.Logger) *systemLearnerPromoter {
	return &systemLearnerPromoter{
		cluster:           c,
		peerState:         ps,
		logger:            logger,
		interval:          systemLearnerPromoterInterval,
		stabilityRequired: systemLearnerStabilityTicks,
		catchupTicks:      make(map[string]int),
		now:               time.Now,
	}
}

// Run blocks until ctx is cancelled. Non-leader ticks are no-ops so
// the same goroutine runs harmlessly on every node — only the current
// system-Raft leader proposes AddVoter, preventing concurrent
// membership change attempts that would fight Raft's single-mutation
// invariant.
func (p *systemLearnerPromoter) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if p.cluster.IsLeader() {
				p.tick(ctx)
			}
		}
	}
}

// tick scans the current Raft configuration once. For each learner it
// either advances or resets the per-learner catchup-tick counter, and
// promotes to voter when the counter reaches stabilityRequired.
//
// A learner with no recent NodeStats broadcast (no PeerState entry)
// resets to zero — the leader has no evidence it's caught up, so it
// can't promote. This also covers the bootstrap window where the new
// node has joined Raft but hasn't yet been observed gossiping back.
func (p *systemLearnerPromoter) tick(ctx context.Context) {
	servers, err := p.cluster.Servers()
	if err != nil {
		p.logger.Error("system_learner_promoter: list servers", "error", err)
		return
	}
	leaderApplied := localAppliedIndex(p.cluster)
	if leaderApplied == 0 {
		// No applied entries yet — nothing to compare against; skip.
		return
	}

	seen := make(map[string]bool, len(servers))
	for _, srv := range servers {
		if srv.Suffrage != "Nonvoter" && srv.Suffrage != "Staging" {
			continue
		}
		seen[srv.ID] = true
		p.evaluateLearner(ctx, srv, leaderApplied)
	}

	// Drop tick counters for nodes that have left the configuration
	// (either removed entirely or already promoted). Without this the
	// map would slowly accumulate stale entries across cluster
	// scale-ups and downs.
	for id := range p.catchupTicks {
		if !seen[id] {
			delete(p.catchupTicks, id)
		}
	}
}

// evaluateLearner inspects one learner's catchup state and either
// advances the stability counter or promotes the learner. Split out
// of tick() so the inner block is readable without a deeply nested
// switch.
func (p *systemLearnerPromoter) evaluateLearner(ctx context.Context, srv cluster.RaftServer, leaderApplied uint64) {
	stats := p.peerState.Get(srv.ID)
	if stats == nil || stats.RaftAppliedIndex < leaderApplied {
		p.catchupTicks[srv.ID] = 0
		return
	}
	p.catchupTicks[srv.ID]++
	if p.catchupTicks[srv.ID] < p.stabilityRequired {
		p.logger.Debug("system_learner_promoter: learner caught up, awaiting stability",
			"node", srv.ID, "ticks", p.catchupTicks[srv.ID], "needed", p.stabilityRequired)
		return
	}

	promoteCtx, cancel := context.WithTimeout(ctx, systemLearnerPromoteTimeout)
	defer cancel()
	_ = promoteCtx // hraft.AddVoter doesn't accept a context; the timeout governs the future.

	if err := p.cluster.AddVoter(srv.ID, srv.Address, systemLearnerPromoteTimeout); err != nil {
		p.logger.Warn("system_learner_promoter: AddVoter failed",
			"node", srv.ID, "addr", srv.Address, "error", err)
		// Leave the tick counter intact — the next tick will retry
		// if the learner is still caught up. Resetting would force
		// the operator to wait another stability window after a
		// transient Raft hiccup.
		return
	}
	p.logger.Info("system_learner_promoter: promoted learner to voter",
		"node", srv.ID, "addr", srv.Address, "leader_applied", leaderApplied)
	delete(p.catchupTicks, srv.ID)
}

// localAppliedIndex reads the local node's Raft applied_index from
// the raftMembership stats map. Returns 0 if Raft is uninitialised
// or the field is missing/unparseable. The map-of-strings interface
// is what hraft.Stats returns; parsing it here keeps the promoter
// self-contained without adding a new typed accessor to the Server.
func localAppliedIndex(s raftMembership) uint64 {
	if s == nil {
		return 0
	}
	m := s.LocalStats()
	if m == nil {
		return 0
	}
	v, _ := strconv.ParseUint(m["applied_index"], 10, 64)
	return v
}
