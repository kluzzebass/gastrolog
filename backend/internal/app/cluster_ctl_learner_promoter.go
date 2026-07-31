package app

import (
	"log/slog"
	"strconv"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
)

const (
	// clusterCtlLearnerPromoteTimeout bounds each AddVoter membership-change
	// commit. The change is small (one config entry) so this is mostly
	// a quorum-loss guard; a healthy cluster commits in milliseconds.
	clusterCtlLearnerPromoteTimeout = 5 * time.Second
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

// clusterCtlPromotionGroup adapts the single cluster-ctl Raft group to
// the promotionGroup surface. cluster-ctl is a low-churn group (config
// mutations only), so its catch-up tolerance is zero: a learner is
// promotable once its broadcast RaftAppliedIndex reaches the leader's
// applied index exactly.
type clusterCtlPromotionGroup struct {
	cluster   raftMembership
	peerState peerStatsReader
	logger    *slog.Logger
}

func (g *clusterCtlPromotionGroup) label() string         { return "cluster-ctl" }
func (g *clusterCtlPromotionGroup) isLeader() bool        { return g.cluster.IsLeader() }
func (g *clusterCtlPromotionGroup) leaderApplied() uint64 { return localAppliedIndex(g.cluster) }
func (g *clusterCtlPromotionGroup) tolerance() uint64     { return 0 }

func (g *clusterCtlPromotionGroup) learners() []learnerMember {
	servers, err := g.cluster.Servers()
	if err != nil {
		g.logger.Error("cluster_ctl_learner_promoter: list servers", "error", err)
		return nil
	}
	var out []learnerMember
	for _, srv := range servers {
		// "Staging" is hashicorp/raft's transient suffrage between
		// AddNonvoter and the membership commit landing — treat it as a
		// learner so the promoter doesn't race itself by skipping it and
		// then re-issuing AddVoter.
		if srv.Suffrage != "Nonvoter" && srv.Suffrage != "Staging" {
			continue
		}
		out = append(out, learnerMember{nodeID: srv.ID, addr: srv.Address})
	}
	return out
}

func (g *clusterCtlPromotionGroup) observedApplied(nodeID string) (uint64, bool) {
	stats := g.peerState.Get(nodeID)
	if stats == nil {
		return 0, false
	}
	return stats.RaftAppliedIndex, true
}

func (g *clusterCtlPromotionGroup) promote(m learnerMember) error {
	return g.cluster.AddVoter(m.nodeID, m.addr, clusterCtlLearnerPromoteTimeout)
}

// newClusterCtlLearnerPromoter builds the event-driven promoter for the
// cluster-ctl Raft group. Fresh nodes join as Nonvoter / Staging learners
// and are promoted to Voter once they have replicated the existing log —
// protecting the cluster from the quorum-loss window where a fresh joiner
// counts as a voter but cannot yet vote because its WAL is still catching
// up. The single-element group provider means evaluate() consults the one
// cluster-ctl group; the isLeader() gate inside makes non-leader nodes
// no-ops.
func newClusterCtlLearnerPromoter(c raftMembership, ps peerStatsReader, logger *slog.Logger) *learnerPromoter {
	g := &clusterCtlPromotionGroup{cluster: c, peerState: ps, logger: logger}
	return newLearnerPromoter("cluster-ctl", func() []promotionGroup {
		return []promotionGroup{g}
	}, logger)
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
