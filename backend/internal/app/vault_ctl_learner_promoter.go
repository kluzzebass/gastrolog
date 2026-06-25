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
	// vaultCtlLearnerPromoterJobName is the operator-visible name
	// shown in the inspector's Scheduled view. Keep stable across
	// releases.
	vaultCtlLearnerPromoterJobName = "vault-ctl-learner-promoter"

	// vaultCtlLearnerPromoterSchedule runs every 30 seconds. Mirrors
	// the cluster-ctl learner promoter (gastrolog-2czh9): slow,
	// per-vault-leader-only, low-churn. 6-field cron (with-seconds).
	vaultCtlLearnerPromoterSchedule = "*/30 * * * * *"

	// vaultCtlLearnerStabilityTicks is the number of consecutive ticks
	// a learner must be observed at caught-up state before promotion.
	// Same rationale as the cluster-ctl promoter: guards against
	// transient apply-index parity caused by gossip lag or a brief
	// stall in the local apply pipeline.
	vaultCtlLearnerStabilityTicks = 2

	// vaultCtlLearnerPromoteTimeout bounds each AddVoter membership
	// commit on a vault-ctl group.
	vaultCtlLearnerPromoteTimeout = 5 * time.Second

	// vaultCtlLearnerCatchupTolerance accommodates the ~half-tick
	// staleness between the follower's last NodeStats broadcast and
	// the leader's live applied index. A healthy follower lags the
	// leader by typically <10 entries; the broadcast itself adds at
	// most one broadcast-interval's worth of new commits to the gap
	// (~50-100 entries at typical rates). Sized small on purpose:
	// genuinely-behind followers should NOT be promoted, and a larger
	// budget here would silently mask plumbing bugs rather than
	// surface them.
	vaultCtlLearnerCatchupTolerance uint64 = 100
)

// vaultCtlRaftGroupAccess is the subset of raftgroup.GroupManager the
// promoter calls — narrowed for testability.
type vaultCtlRaftGroupAccess interface {
	GetGroup(groupID string) *raftgroup.Group
}

// vaultCtlLearnerPromoter promotes vault-ctl Raft learners (Nonvoter /
// Staging members) to voters once their broadcast
// VaultStats.RaftAppliedIndex has matched the local node's applied
// index for a stability window. Runs on every node; only proposes
// AddVoter when this node is the leader of the per-vault group.
//
// Companion to the cluster-ctl learner promoter (gastrolog-2czh9)
// and the JoinCluster-as-learner change (gastrolog-41sut). Unlike the
// cluster-ctl promoter (single group, single leader), vault-ctl groups are
// per-vault — each vault has its own leader, and any node might be
// the leader for some vaults and a follower for others. The promoter
// iterates every vault on each tick and only acts on groups it
// currently leads.
//
// The catchup signal reuses the existing per-vault VaultStats
// broadcast in NodeStats: every node publishes its vault-ctl
// RaftAppliedIndex per vault, and the leader looks up each
// learner's last-reported value via PeerState. No new RPC needed.
type vaultCtlLearnerPromoter struct {
	cfgStore          system.Store
	groupMgr          vaultCtlRaftGroupAccess
	peerState         peerStatsReader
	localNodeID       string
	logger            *slog.Logger
	stabilityRequired int

	// catchupTicks tracks consecutive caught-up observations keyed by
	// (vaultID, nodeID). A flicker on either dimension resets the
	// count to zero — same contract as the cluster-ctl promoter.
	catchupTicks map[catchupKey]int
}

type catchupKey struct {
	vaultID glid.GLID
	nodeID  string
}

func newVaultCtlLearnerPromoter(cfgStore system.Store, groupMgr vaultCtlRaftGroupAccess, peerState peerStatsReader, localNodeID string, logger *slog.Logger) *vaultCtlLearnerPromoter {
	return &vaultCtlLearnerPromoter{
		cfgStore:          cfgStore,
		groupMgr:          groupMgr,
		peerState:         peerState,
		localNodeID:       localNodeID,
		logger:            logger,
		stabilityRequired: vaultCtlLearnerStabilityTicks,
		catchupTicks:      make(map[catchupKey]int),
	}
}

// tickOnce is the scheduled task body. Unlike the cluster-ctl
// promoter, there's no top-level leader gate: every node may lead
// some vault-ctl groups and follow others, so the iteration runs
// everywhere and the per-group leader check sits inside
// evaluateVault (line ~155).
func (p *vaultCtlLearnerPromoter) tickOnce(ctx context.Context) {
	p.tick(ctx)
}

// startVaultCtlLearnerPromoter registers the promoter with the
// supplied scheduler as a recurring job. Returns the AddJob error if
// any. Describes the per-group-leader semantics so the operator
// understands why the job fires on every node (not just the system-
// Raft leader).
func startVaultCtlLearnerPromoter(ctx context.Context, scheduler scheduledJobRegistry, promoter *vaultCtlLearnerPromoter) error {
	task := func() { promoter.tickOnce(ctx) }
	if err := scheduler.AddJob(vaultCtlLearnerPromoterJobName, vaultCtlLearnerPromoterSchedule, task); err != nil {
		return err
	}
	scheduler.Describe(vaultCtlLearnerPromoterJobName,
		"Per-vault-ctl learner promotion. Runs on every node and iterates every vault-ctl group; the per-group leader gate inside the tick body only proposes AddVoter for groups this node currently leads. Each learner must hold its broadcast RaftAppliedIndex within tolerance of the group leader's applied index for a stability window before promotion. Companion to gastrolog-2czh9 (cluster-ctl promoter) and gastrolog-41sut (JoinCluster-as-learner). Original implementation gastrolog-gcbx7.")
	return nil
}

// tick scans every configured vault. For each one, if the local node
// is the vault-ctl group's Raft leader, it evaluates the group's
// learners against the leader's applied index and advances or resets
// per-learner stability counters. Promotion fires when the counter
// reaches stabilityRequired.
func (p *vaultCtlLearnerPromoter) tick(ctx context.Context) {
	vaults, err := p.cfgStore.ListVaults(ctx)
	if err != nil {
		p.logger.Error("vault_ctl_learner_promoter: list vaults", "error", err)
		return
	}
	seen := make(map[catchupKey]bool)
	for _, v := range vaults {
		p.evaluateVault(v.ID, seen)
	}
	// Drop stale counter entries for vaults / learners that have left
	// the configuration (vault deleted, learner promoted via another
	// path, or removed entirely).
	for k := range p.catchupTicks {
		if !seen[k] {
			delete(p.catchupTicks, k)
		}
	}
}

// evaluateVault evaluates one vault's group for learner promotion.
// Split out of tick() so the inner block reads sequentially without a
// nested switch+for. seen records the (vault, learner) keys observed
// this tick, used by the caller to prune stale counters.
func (p *vaultCtlLearnerPromoter) evaluateVault(vaultID glid.GLID, seen map[catchupKey]bool) {
	g := p.groupMgr.GetGroup(raftgroup.VaultControlPlaneGroupID(vaultID))
	if g == nil || g.Raft == nil {
		return
	}
	if g.Raft.State() != hraft.Leader {
		return
	}

	cfgFuture := g.Raft.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		p.logger.Warn("vault_ctl_learner_promoter: get configuration",
			"vault", vaultID, "error", err)
		return
	}
	leaderApplied := g.Raft.AppliedIndex()
	if leaderApplied == 0 {
		return
	}

	promotionUsed := false
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.Suffrage != hraft.Nonvoter && srv.Suffrage != hraft.Staging {
			continue
		}
		nodeID := string(srv.ID)
		key := catchupKey{vaultID: vaultID, nodeID: nodeID}
		seen[key] = true
		allowPromote := !promotionUsed
		if p.evaluateLearner(g, vaultID, nodeID, string(srv.Address), leaderApplied, key, allowPromote) {
			promotionUsed = true
		}
	}
}

// evaluateLearner advances or resets the per-learner stability counter
// for one vault-ctl group's learner. Promotes via AddVoter when the
// counter reaches stabilityRequired and allowPromote is true.
func (p *vaultCtlLearnerPromoter) evaluateLearner(g *raftgroup.Group, vaultID glid.GLID, nodeID, addr string, leaderApplied uint64, key catchupKey, allowPromote bool) bool {
	obs := observePeerVault(p.peerState, nodeID, vaultID)
	if !obs.hasVaultEntry {
		p.logger.Info("vault_ctl_learner_promoter: peer not yet reporting this vault",
			"vault", vaultID, "node", nodeID,
			"has_peer_stats", obs.hasPeerStats,
			"vaults_in_peer_broadcast", obs.totalVaults)
		p.catchupTicks[key] = 0
		return false
	}
	applied := obs.appliedIndex
	// Tolerance: an active vault-ctl group commits entries faster
	// than the NodeStats broadcast interval, so the follower's last
	// broadcast lags the leader's live applied by some delta.
	// vaultCtlLearnerCatchupTolerance is the budget — applied within
	// this many entries of leaderApplied counts as caught up.
	caughtUp := applied+vaultCtlLearnerCatchupTolerance >= leaderApplied
	if !caughtUp {
		// lag uint64-subtracts safely because !caughtUp guarantees
		// leaderApplied > applied + tolerance (in particular,
		// leaderApplied > applied).
		p.logger.Info("vault_ctl_learner_promoter: learner lagging",
			"vault", vaultID, "node", nodeID,
			"learner_applied", applied, "leader_applied", leaderApplied,
			"lag", leaderApplied-applied, "tolerance", vaultCtlLearnerCatchupTolerance)
		p.catchupTicks[key] = 0
		return false
	}
	p.logger.Info("vault_ctl_learner_promoter: learner caught up",
		"vault", vaultID, "node", nodeID,
		"learner_applied", applied, "leader_applied", leaderApplied,
		"ticks", p.catchupTicks[key]+1, "needed", p.stabilityRequired)
	p.catchupTicks[key]++
	if p.catchupTicks[key] < p.stabilityRequired {
		p.logger.Debug("vault_ctl_learner_promoter: learner caught up, awaiting stability",
			"vault", vaultID, "node", nodeID,
			"ticks", p.catchupTicks[key], "needed", p.stabilityRequired)
		return false
	}
	if !allowPromote {
		return false
	}

	future := g.Raft.AddVoter(hraft.ServerID(nodeID), hraft.ServerAddress(addr), 0, vaultCtlLearnerPromoteTimeout)
	if err := future.Error(); err != nil {
		p.logger.Warn("vault_ctl_learner_promoter: AddVoter failed",
			"vault", vaultID, "node", nodeID, "addr", addr, "error", err)
		// Leave counter intact for retry.
		return true
	}
	p.logger.Info("vault_ctl_learner_promoter: promoted learner to voter",
		"vault", vaultID, "node", nodeID, "addr", addr,
		"leader_applied", leaderApplied)
	delete(p.catchupTicks, key)
	return true
}

// peerVaultObservation describes what the leader knows about a peer's
// vault-ctl state from broadcasts. Used by the promoter to choose
// between "wait" (no evidence) and "evaluate catchup" (have evidence).
type peerVaultObservation struct {
	hasPeerStats   bool   // PeerState had an entry at all
	hasVaultEntry  bool   // peer's NodeStats included this vault
	totalVaults    int    // number of vault entries the peer broadcast (diagnostic)
	appliedIndex   uint64 // raft applied index for this vault (only valid when hasVaultEntry)
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

// peerVaultAppliedIndex is the old shape, retained for the existing
// unit tests. Callers that need to distinguish the absence reasons
// should use observePeerVault directly.
func peerVaultAppliedIndex(ps peerStatsReader, nodeID string, vaultID glid.GLID) (uint64, bool) {
	obs := observePeerVault(ps, nodeID, vaultID)
	return obs.appliedIndex, obs.hasVaultEntry
}

// Compile-time check that *cluster.PeerState satisfies peerStatsReader.
// peerStatsReader is defined in cluster_ctl_learner_promoter.go.
var _ peerStatsReader = (*cluster.PeerState)(nil)

// vaultStatsByID is exported so tests can construct a NodeStats with
// per-vault entries without reaching into proto internals. Returns a
// freshly-allocated VaultStats with just the fields the promoter
// reads.
func vaultStatsByID(vaultID glid.GLID, appliedIndex uint64) *gastrologv1.VaultStats {
	return &gastrologv1.VaultStats{
		Id:               vaultID[:],
		RaftAppliedIndex: appliedIndex,
	}
}
