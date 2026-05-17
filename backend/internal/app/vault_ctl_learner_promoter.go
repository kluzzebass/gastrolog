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
	// vaultCtlLearnerPromoterInterval is the leader-side tick cadence
	// per-vault-ctl group. Mirrors the system-Raft learner promoter
	// (gastrolog-2czh9): slow, leader-only, low-churn.
	vaultCtlLearnerPromoterInterval = 30 * time.Second

	// vaultCtlLearnerStabilityTicks is the number of consecutive ticks
	// a learner must be observed at caught-up state before promotion.
	// Same rationale as the system-Raft promoter: guards against
	// transient apply-index parity caused by gossip lag or a brief
	// stall in the local apply pipeline.
	vaultCtlLearnerStabilityTicks = 2

	// vaultCtlLearnerPromoteTimeout bounds each AddVoter membership
	// commit on a vault-ctl group.
	vaultCtlLearnerPromoteTimeout = 5 * time.Second
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
// Companion to the system-Raft learner promoter (gastrolog-2czh9)
// and the JoinCluster-as-learner change (gastrolog-41sut). Unlike the
// system promoter (single group, single leader), vault-ctl groups are
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
	interval          time.Duration
	stabilityRequired int

	// catchupTicks tracks consecutive caught-up observations keyed by
	// (vaultID, nodeID). A flicker on either dimension resets the
	// count to zero — same contract as the system promoter.
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
		interval:          vaultCtlLearnerPromoterInterval,
		stabilityRequired: vaultCtlLearnerStabilityTicks,
		catchupTicks:      make(map[catchupKey]int),
	}
}

// Run blocks until ctx is cancelled. Ticks every interval; per tick,
// iterates every vault in the config store, checks if this node is
// the leader of that vault's vault-ctl group, and if so evaluates
// learners for promotion.
func (p *vaultCtlLearnerPromoter) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
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

	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.Suffrage != hraft.Nonvoter && srv.Suffrage != hraft.Staging {
			continue
		}
		nodeID := string(srv.ID)
		key := catchupKey{vaultID: vaultID, nodeID: nodeID}
		seen[key] = true
		p.evaluateLearner(g, vaultID, nodeID, string(srv.Address), leaderApplied, key)
	}
}

// evaluateLearner advances or resets the per-learner stability
// counter for one vault-ctl group's learner. Promotes via AddVoter
// when the counter reaches stabilityRequired. AddVoter failure
// preserves the counter so the next tick retries without forcing
// another full window — same rationale as the system promoter.
func (p *vaultCtlLearnerPromoter) evaluateLearner(g *raftgroup.Group, vaultID glid.GLID, nodeID, addr string, leaderApplied uint64, key catchupKey) {
	applied, ok := peerVaultAppliedIndex(p.peerState, nodeID, vaultID)
	if !ok || applied < leaderApplied {
		p.catchupTicks[key] = 0
		return
	}
	p.catchupTicks[key]++
	if p.catchupTicks[key] < p.stabilityRequired {
		p.logger.Debug("vault_ctl_learner_promoter: learner caught up, awaiting stability",
			"vault", vaultID, "node", nodeID,
			"ticks", p.catchupTicks[key], "needed", p.stabilityRequired)
		return
	}

	future := g.Raft.AddVoter(hraft.ServerID(nodeID), hraft.ServerAddress(addr), 0, vaultCtlLearnerPromoteTimeout)
	if err := future.Error(); err != nil {
		p.logger.Warn("vault_ctl_learner_promoter: AddVoter failed",
			"vault", vaultID, "node", nodeID, "addr", addr, "error", err)
		// Leave counter intact for retry.
		return
	}
	p.logger.Info("vault_ctl_learner_promoter: promoted learner to voter",
		"vault", vaultID, "node", nodeID, "addr", addr,
		"leader_applied", leaderApplied)
	delete(p.catchupTicks, key)
}

// peerVaultAppliedIndex looks up the named peer's broadcast
// vault-ctl applied index for a specific vault. Returns (0, false)
// when the peer has no recent NodeStats (no PeerState entry) or
// hasn't reported VaultStats for this vault. The boolean
// distinguishes "no evidence" from a legitimate zero — the promoter
// uses it to skip rather than treat absence as lag.
func peerVaultAppliedIndex(ps peerStatsReader, nodeID string, vaultID glid.GLID) (uint64, bool) {
	stats := ps.Get(nodeID)
	if stats == nil {
		return 0, false
	}
	idStr := vaultID.String()
	for _, vs := range stats.Vaults {
		if vs == nil {
			continue
		}
		// VaultStats.Id is the raw byte form of the GLID. Compare via
		// the canonical string form. FromBytes returns Nil on short
		// input (so the .String() comparison naturally rejects
		// malformed entries).
		if glid.FromBytes(vs.Id).String() == idStr {
			return vs.GetRaftAppliedIndex(), true
		}
	}
	return 0, false
}

// Compile-time check that *cluster.PeerState satisfies peerStatsReader.
// peerStatsReader is defined in system_raft_learner_promoter.go.
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
