package app

import (
	"context"
	"log/slog"
	"time"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/cluster"
	"gastrolog/internal/glid"
	"gastrolog/internal/raftgroup"
	"gastrolog/internal/system"

	hraft "github.com/hashicorp/raft"
)

const (
	// vaultCtlLearnerPromoteTimeout bounds each AddVoter membership
	// commit on a vault-ctl group.
	vaultCtlLearnerPromoteTimeout = 5 * time.Second

	// vaultCtlLearnerCatchupTolerance accommodates the staleness between
	// the follower's last NodeStats broadcast and the leader's live
	// applied index. A healthy follower lags the leader by typically <10
	// entries; the broadcast itself adds at most one broadcast-interval's
	// worth of new commits to the gap (~50-100 entries at typical rates).
	// Sized small on purpose: genuinely-behind followers should NOT be
	// promoted, and a larger budget here would silently mask plumbing
	// bugs rather than surface them.
	vaultCtlLearnerCatchupTolerance uint64 = 100
)

// vaultCtlRaftGroupAccess is the subset of raftgroup.GroupManager the
// promoter calls — narrowed for testability.
type vaultCtlRaftGroupAccess interface {
	GetGroup(groupID string) *raftgroup.Group
}

// vaultCtlPromotionGroup adapts one per-vault vault-ctl Raft group to the
// promotionGroup surface. Unlike cluster-ctl (single group, single
// leader), vault-ctl groups are per-vault: any node may lead some vaults
// and follow others, so isLeader() checks this specific group's Raft
// state. The catch-up signal is the per-vault VaultStats.RaftAppliedIndex
// carried in the same NodeStats broadcast.
type vaultCtlPromotionGroup struct {
	vaultID   glid.GLID
	group     *raftgroup.Group
	peerState peerStatsReader
	logger    *slog.Logger
}

func (g *vaultCtlPromotionGroup) label() string { return g.vaultID.String() }

func (g *vaultCtlPromotionGroup) isLeader() bool {
	return g.group.Raft != nil && g.group.Raft.State() == hraft.Leader
}

func (g *vaultCtlPromotionGroup) leaderApplied() uint64 { return g.group.Raft.AppliedIndex() }
func (g *vaultCtlPromotionGroup) tolerance() uint64     { return vaultCtlLearnerCatchupTolerance }

func (g *vaultCtlPromotionGroup) learners() []learnerMember {
	cfg := g.group.Raft.GetConfiguration()
	if err := cfg.Error(); err != nil {
		g.logger.Warn("vault_ctl_learner_promoter: get configuration",
			"vault", g.vaultID, "error", err)
		return nil
	}
	var out []learnerMember
	for _, srv := range cfg.Configuration().Servers {
		if srv.Suffrage != hraft.Nonvoter && srv.Suffrage != hraft.Staging {
			continue
		}
		out = append(out, learnerMember{nodeID: string(srv.ID), addr: string(srv.Address)})
	}
	return out
}

func (g *vaultCtlPromotionGroup) observedApplied(nodeID string) (uint64, bool) {
	obs := observePeerVault(g.peerState, nodeID, g.vaultID)
	if !obs.hasVaultEntry {
		g.logger.Debug("vault_ctl_learner_promoter: peer not yet reporting this vault",
			"vault", g.vaultID, "node", nodeID,
			"has_peer_stats", obs.hasPeerStats,
			"vaults_in_peer_broadcast", obs.totalVaults)
		return 0, false
	}
	return obs.appliedIndex, true
}

func (g *vaultCtlPromotionGroup) promote(m learnerMember) error {
	fut := g.group.Raft.AddVoter(hraft.ServerID(m.nodeID), hraft.ServerAddress(m.addr), 0, vaultCtlLearnerPromoteTimeout)
	return fut.Error()
}

// newVaultCtlLearnerPromoter builds the event-driven promoter for the
// per-vault vault-ctl Raft groups. The group provider enumerates every
// vault configured on this node and yields a promotionGroup for the ones
// whose Raft group exists locally; evaluate()'s per-group isLeader() gate
// then restricts action to the groups this node currently leads. New
// members are added to vault-ctl groups as Nonvoter learners by the
// vault-ctl leader manager's membership reconcile, which explicitly
// leaves promotion to this promoter. See gastrolog-4vg17.
func newVaultCtlLearnerPromoter(ctx context.Context, cfgStore system.Store, groupMgr vaultCtlRaftGroupAccess, ps peerStatsReader, logger *slog.Logger) *learnerPromoter {
	return newLearnerPromoter("vault-ctl", func() []promotionGroup {
		vaults, err := cfgStore.ListVaults(ctx)
		if err != nil {
			logger.Error("vault_ctl_learner_promoter: list vaults", "error", err)
			return nil
		}
		groups := make([]promotionGroup, 0, len(vaults))
		for _, v := range vaults {
			g := groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(v.ID))
			if g == nil || g.Raft == nil {
				continue
			}
			groups = append(groups, &vaultCtlPromotionGroup{
				vaultID:   v.ID,
				group:     g,
				peerState: ps,
				logger:    logger,
			})
		}
		return groups
	}, logger)
}

// peerVaultObservation describes what the leader knows about a peer's
// vault-ctl state from broadcasts. Used to choose between "wait" (no
// evidence) and "evaluate catchup" (have evidence).
type peerVaultObservation struct {
	hasPeerStats  bool   // PeerState had an entry at all
	hasVaultEntry bool   // peer's NodeStats included this vault
	totalVaults   int    // number of vault entries the peer broadcast (diagnostic)
	appliedIndex  uint64 // raft applied index for this vault (only valid when hasVaultEntry)
}

// observePeerVault looks up the named peer's broadcast vault-ctl
// applied index for a specific vault. The observation distinguishes
// "peer hasn't broadcast at all" from "peer broadcast but didn't
// include this vault" — the latter usually means the peer's
// orchestrator hasn't registered the vault yet, which is a real
// catchup-side condition rather than a stats-lag.
func observePeerVault(ps peerStatsReader, nodeID string, vaultID glid.GLID) peerVaultObservation {
	stats := ps.Get(nodeID)
	if stats == nil {
		return peerVaultObservation{}
	}
	obs := peerVaultObservation{hasPeerStats: true, totalVaults: len(stats.Vaults)}
	idStr := vaultID.String()
	for _, vs := range stats.Vaults {
		if vs == nil {
			continue
		}
		if glid.FromBytes(vs.Id).String() == idStr {
			obs.hasVaultEntry = true
			obs.appliedIndex = vs.GetRaftAppliedIndex()
			return obs
		}
	}
	return obs
}

// peerVaultAppliedIndex is the flat shape, retained for the unit tests.
// Callers that need to distinguish the absence reasons should use
// observePeerVault directly.
func peerVaultAppliedIndex(ps peerStatsReader, nodeID string, vaultID glid.GLID) (uint64, bool) {
	obs := observePeerVault(ps, nodeID, vaultID)
	return obs.appliedIndex, obs.hasVaultEntry
}

// Compile-time check that *cluster.PeerState satisfies peerStatsReader.
var _ peerStatsReader = (*cluster.PeerState)(nil)

// vaultStatsByID is exported so tests can construct a NodeStats with
// per-vault entries without reaching into proto internals. Returns a
// freshly-allocated VaultStats with just the fields the promoter reads.
func vaultStatsByID(vaultID glid.GLID, appliedIndex uint64) *gastrologv1.VaultStats {
	return &gastrologv1.VaultStats{
		Id:               vaultID[:],
		RaftAppliedIndex: appliedIndex,
	}
}
