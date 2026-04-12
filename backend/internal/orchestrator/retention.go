package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

const (
	defaultRetentionSchedule = "* * * * *" // every minute
	retentionJobName         = "retention"
)

// retentionKey returns a unique map key for a tier instance's retention state.
func retentionKey(tierID uuid.UUID, storageID string) string {
	if storageID == "" {
		return tierID.String()
	}
	return tierID.String() + ":" + storageID
}

// retentionRule is a resolved rule: a compiled policy paired with an action.
type retentionRule struct {
	policy        chunk.RetentionPolicy
	action        config.RetentionAction
	ejectRouteIDs []uuid.UUID // target route IDs, only for eject
}

// retentionRunner holds per-tier-instance state that persists across sweeps.
// Only primaries get runners — secondaries react to the tier Raft manifest
// via the ChunkFSM.OnDelete callback.
type retentionRunner struct {
	mu         sync.Mutex
	vaultID    uuid.UUID
	tierID     uuid.UUID
	cm         chunk.ChunkManager
	im         index.IndexManager
	inflight   map[chunk.ChunkID]bool // chunks currently being processed
	unreadable map[chunk.ChunkID]bool // chunks that failed to read — skipped until restart
	orch       *Orchestrator          // for eject/transition callbacks

	// applyRaftDelete applies CmdDeleteChunk to the tier Raft before local
	// deletion, so secondaries learn about it via the manifest. Nil in
	// single-node / memory mode — local delete proceeds without Raft.
	applyRaftDelete              func(id chunk.ChunkID) error
	applyRaftRetentionPending    func(id chunk.ChunkID) error
	applyRaftTransitionStreamed  func(id chunk.ChunkID) error

	// isLeader returns true if this node is the config leader for this tier.
	// Retention (expiry + transitions) only runs on the leader to prevent
	// all nodes from independently transitioning the same chunks.
	isLeader bool

	// followerTargets are the remote nodes that hold replicas of this tier's
	// chunks. Used to forward chunk deletions after retention expires them.
	followerTargets []config.ReplicationTarget

	now func() time.Time
	logger      *slog.Logger
}

type sweepTarget struct {
	runner *retentionRunner
	rules  []retentionRule
}

// retentionSweepAll is the single scheduled retention job. Each tick:
//   - Leader: evaluates retention rules, applies CmdDeleteChunk to tier Raft, deletes locally.
//   - Followers: reconcile local disk against the tier Raft manifest — delete anything
//     the leader has removed. No independent rule evaluation.
func (o *Orchestrator) retentionSweepAll() {
	cfg, err := o.loadConfig(context.Background())
	if err != nil {
		o.logger.Error("retention: failed to load config", "error", err)
		return
	}
	if cfg == nil {
		return
	}

	var targets []sweepTarget
	var reconcileTiers []*TierInstance
	active := make(map[string]bool)

	o.mu.Lock()
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
			// Only the config leader evaluates retention rules.
			// Followers reconcile against the manifest.
			if !tier.IsLeader() {
				if tier.ListManifest != nil {
					reconcileTiers = append(reconcileTiers, tier)
				}
				continue
			}
			if t := o.retentionTargetForTier(cfg, vaultCfg, tier, active); t != nil {
				targets = append(targets, *t)
			}
		}
	}
	for key := range o.retention {
		if !active[key] {
			delete(o.retention, key)
		}
	}
	o.mu.Unlock()

	// Leader: evaluate retention rules.
	for _, t := range targets {
		t.runner.sweep(t.rules)
	}

	// Leader: confirm streamed transitions. Chunks marked as
	// transitionStreamed have been streamed to the next tier but not yet
	// deleted — we wait for the destination to have adequate replicas
	// before expiring the source. See gastrolog-4913n.
	for _, t := range targets {
		t.runner.confirmStreamedTransitions(cfg)
	}

	// Followers: reconcile against the tier Raft manifest.
	for _, tier := range reconcileTiers {
		o.reconcileFollower(tier)
	}

	// Memory budget enforcement: transition oldest chunks when over budget.
	o.enforceMemoryBudgets(cfg)

	// Cache eviction: collect evictors under lock, run outside.
	// EvictCache does filesystem I/O — holding the lock would block Raft applies.
	var evictors []chunk.ChunkCacheEvictor
	o.mu.RLock()
	for _, vault := range o.vaults {
		for _, tier := range vault.Tiers {
			if evictor, ok := tier.Chunks.(chunk.ChunkCacheEvictor); ok {
				evictors = append(evictors, evictor)
			}
		}
	}
	o.mu.RUnlock()
	for _, evictor := range evictors {
		evictor.EvictCache()
	}
}

// enforceMemoryBudgets checks memory tiers for budget overruns and transitions
// the oldest sealed chunks to the next tier. Only runs on leaders.
func (o *Orchestrator) enforceMemoryBudgets(cfg *config.Config) {
	if cfg == nil {
		return
	}
	type budgetTarget struct {
		vaultID uuid.UUID
		tierID  uuid.UUID
		cm      chunk.ChunkManager
		excess  int64
	}

	var targets []budgetTarget
	o.mu.RLock()
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
			if !tier.IsLeader() {
				continue
			}
			monitor, ok := tier.Chunks.(chunk.ChunkBudgetMonitor)
			if !ok {
				continue
			}
			if excess := monitor.BudgetExceeded(); excess > 0 {
				targets = append(targets, budgetTarget{
					vaultID: vaultCfg.ID,
					tierID:  tier.TierID,
					cm:      tier.Chunks,
					excess:  excess,
				})
			}
		}
	}
	o.mu.RUnlock()

	for _, t := range targets {
		o.drainExcessChunks(t.vaultID, t.tierID, t.cm, t.excess)
	}
}

// drainExcessChunks transitions the oldest sealed chunks from a memory tier
// until the excess bytes are reclaimed (or no more sealed chunks remain).
func (o *Orchestrator) drainExcessChunks(vaultID, tierID uuid.UUID, cm chunk.ChunkManager, excess int64) {
	metas, err := cm.List()
	if err != nil {
		return
	}

	// Sort oldest first (by WriteStart).
	slices.SortFunc(metas, func(a, b chunk.ChunkMeta) int {
		return a.WriteStart.Compare(b.WriteStart)
	})

	// Find the index manager for this tier.
	var im index.IndexManager
	o.mu.RLock()
	if vault := o.vaults[vaultID]; vault != nil {
		for _, tier := range vault.Tiers {
			if tier.TierID == tierID {
				im = tier.Indexes
				break
			}
		}
	}
	o.mu.RUnlock()

	var reclaimed int64
	for _, m := range metas {
		if reclaimed >= excess {
			break
		}
		if !m.Sealed {
			continue
		}

		runner := &retentionRunner{
			isLeader: true,
			vaultID:  vaultID,
			tierID:   tierID,
			cm:       cm,
			im:       im,
			orch:     o,
			now:      o.now,
			logger:   o.logger,
		}
		runner.transitionChunk(m.ID)
		reclaimed += m.Bytes
	}

	if reclaimed > 0 {
		o.logger.Info("memory budget enforcement: transitioned chunks",
			"vault", vaultID, "tier", tierID,
			"excess", excess, "reclaimed", reclaimed)
	}
}

// RetentionPendingChunks returns chunk IDs marked as retention-pending in the
// tier Raft FSM for a vault. Visible to all nodes via Raft replication.
func (o *Orchestrator) RetentionPendingChunks(vaultID uuid.UUID) map[chunk.ChunkID]bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	vault := o.vaults[vaultID]
	if vault == nil {
		return nil
	}
	result := make(map[chunk.ChunkID]bool)
	for _, tier := range vault.Tiers {
		if tier.ListRetentionPending != nil {
			for _, id := range tier.ListRetentionPending() {
				result[id] = true
			}
		}
	}
	return result
}

// retentionTargetForTier resolves a single tier instance into a sweep target.
// Returns nil if the tier should be skipped (no rules, no leader, etc.).
func (o *Orchestrator) retentionTargetForTier(cfg *config.Config, vaultCfg config.VaultConfig, tier *TierInstance, active map[string]bool) *sweepTarget {
	if tier.HasRaftLeader != nil && !tier.HasRaftLeader() {
		return nil
	}
	// IsRaftLeader check removed: the tier apply forwarder transparently
	// routes applies to the tier Raft leader. The config placement leader
	// always runs retention regardless of tier Raft leadership.
	tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
	if tierCfg == nil || len(tierCfg.RetentionRules) == 0 {
		return nil
	}
	rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
	if err != nil {
		o.logger.Warn("retention: failed to resolve rules",
			"vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
		return nil
	}
	if len(rules) == 0 {
		return nil
	}

	key := retentionKey(tier.TierID, tier.StorageID)
	active[key] = true

	runner := o.retention[key]
	if runner == nil {
		runner = &retentionRunner{
			vaultID: vaultCfg.ID,
			tierID:  tier.TierID,
			cm:      tier.Chunks,
			im:      tier.Indexes,
			orch:    o,
			now:     o.now,
			logger:  o.logger,
		}
		o.retention[key] = runner
	}
	runner.cm = tier.Chunks
	runner.im = tier.Indexes
	runner.applyRaftDelete = tier.ApplyRaftDelete
	runner.applyRaftRetentionPending = tier.ApplyRaftRetentionPending
	runner.applyRaftTransitionStreamed = tier.ApplyRaftTransitionStreamed
	runner.isLeader = tier.IsLeader()
	runner.followerTargets = tier.FollowerTargets
	return &sweepTarget{runner: runner, rules: rules}
}

// reconcileFollower compares on-disk chunks against the tier Raft manifest
// and deletes any sealed chunks that the primary has removed. This is the
// fallback for cases where OnDelete didn't fire (snapshot restore, startup,
// Raft connectivity gaps).
func (o *Orchestrator) reconcileFollower(tier *TierInstance) {
	// Don't reconcile until the tier FSM has applied at least one log entry
	// or restored from a snapshot. Before that, the manifest is incomplete —
	// deleting chunks against it causes permanent data loss.
	if tier.IsFSMReady != nil && !tier.IsFSMReady() {
		return
	}
	manifestIDs := tier.ListManifest()
	manifest := make(map[chunk.ChunkID]bool, len(manifestIDs))
	for _, id := range manifestIDs {
		manifest[id] = true
	}

	localMetas, err := tier.Chunks.List()
	if err != nil {
		return
	}

	for _, meta := range localMetas {
		if manifest[meta.ID] || !meta.Sealed {
			continue
		}
		if tier.Indexes != nil {
			if err := tier.Indexes.DeleteIndexes(meta.ID); err != nil {
				o.logger.Warn("reconcile: delete indexes failed", "tier", tier.TierID, "chunk", meta.ID, "error", err)
			}
		}
		// Local cleanup — do not announce. The authoritative delete already
		// happened elsewhere (retention on the leader) and we are just
		// catching up the local store that missed the OnDelete callback.
		if err := chunk.DeleteNoAnnounce(tier.Chunks, meta.ID); err != nil {
			o.logger.Warn("reconcile: failed to delete orphaned chunk",
				"tier", tier.TierID, "chunk", meta.ID, "error", err)
			continue
		}
		o.logger.Info("reconcile: deleted orphaned chunk from follower",
			"tier", tier.TierID, "chunk", meta.ID)
	}
}

// sweep evaluates retention rules on a primary and applies expire/eject/transition.
func (r *retentionRunner) sweep(rules []retentionRule) {
	// Only the config placement leader runs retention. Raft applies are
	// forwarded transparently to the tier Raft leader via TierApplyForwarder.
	// Config followers must not independently evaluate and transition chunks —
	// that causes N× duplication.
	if !r.isLeader {
		return
	}

	r.mu.Lock()
	if r.inflight == nil {
		r.inflight = make(map[chunk.ChunkID]bool)
	}
	r.mu.Unlock()

	if len(rules) == 0 {
		return
	}

	metas, err := r.cm.List()
	if err != nil {
		r.logger.Error("retention: failed to list chunks", "vault", r.vaultID, "error", err)
		return
	}

	r.mu.Lock()
	unreadable := r.unreadable
	tier := r.findTierInstance()
	r.mu.Unlock()

	// Build a set of chunks awaiting destination replication confirmation
	// so we don't re-stream them on every sweep. See gastrolog-4913n.
	streamed := make(map[chunk.ChunkID]bool)
	if tier != nil && tier.ListTransitionStreamed != nil {
		for _, id := range tier.ListTransitionStreamed() {
			streamed[id] = true
		}
	}

	var sealed []chunk.ChunkMeta
	for _, meta := range metas {
		if meta.Sealed && !unreadable[meta.ID] && !streamed[meta.ID] {
			sealed = append(sealed, meta)
		}
	}

	if len(sealed) == 0 {
		return
	}

	state := chunk.VaultState{
		Chunks: sealed,
		Now:    r.now(),
	}

	processed := make(map[chunk.ChunkID]bool)

	for _, b := range rules {
		matched := b.policy.Apply(state)
		for _, id := range matched {
			if processed[id] {
				continue
			}
			processed[id] = true
			r.tryRetainChunk(id, b)
		}
	}
}

// tryRetainChunk attempts to apply a retention action to a single chunk.
// Acquires the inflight lock, marks retention-pending via Raft, and
// dispatches to the action handler.
func (r *retentionRunner) tryRetainChunk(id chunk.ChunkID, b retentionRule) {
	r.mu.Lock()
	if r.inflight[id] {
		r.mu.Unlock()
		return
	}
	r.inflight[id] = true
	r.mu.Unlock()

	// Mark as retention-pending in the tier Raft so all nodes see it.
	if r.applyRaftRetentionPending != nil {
		if err := r.applyRaftRetentionPending(id); err != nil {
			r.logger.Error("retention: failed to apply raft retention-pending",
				"vault", r.vaultID, "chunk", id, "error", err)
			r.clearInflight(id)
			return
		}
		// Notify: retention_pending flag changed — inspector shows this.
		if r.orch != nil {
			r.orch.NotifyChunkChange()
		}
	}

	defer r.clearInflight(id)
	switch b.action {
	case config.RetentionActionExpire:
		r.expireChunk(id)
	case config.RetentionActionEject:
		r.ejectChunk(id, b.ejectRouteIDs)
	case config.RetentionActionTransition:
		r.transitionChunk(id)
	default:
		r.logger.Error("retention: unknown action", "vault", r.vaultID, "action", b.action)
	}
}

// clearInflight removes a chunk from the in-flight set.
func (r *retentionRunner) clearInflight(id chunk.ChunkID) {
	r.mu.Lock()
	delete(r.inflight, id)
	r.mu.Unlock()
}

// markUnreadable flags a chunk as unreadable so retention skips it on future sweeps.
func (r *retentionRunner) markUnreadable(id chunk.ChunkID, reason error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.unreadable == nil {
		r.unreadable = make(map[chunk.ChunkID]bool)
	}
	r.unreadable[id] = true
	if r.orch.alerts != nil {
		r.orch.alerts.Set(
			fmt.Sprintf("chunk-unreadable:%s", id),
			alert.Error, "retention",
			fmt.Sprintf("Chunk %s unreadable: %v", id, reason),
		)
	}
}

// expireChunk commits the deletion to the tier Raft manifest (so followers
// learn about it via OnDelete), then runs the legacy direct-delete paths
// as a belt-and-braces fallback. The Phase 5 FSM OnDelete cascade already
// deletes the chunk on every node; the direct paths here are redundant in
// cluster mode but harmless — they just return ErrChunkNotFound for the
// chunks OnDelete already deleted, which we treat as an expected "nothing
// to do" signal rather than an error.
//
// Single-node mode (applyRaftDelete == nil) still requires the direct
// paths because there's no Raft cascade to rely on.
func (r *retentionRunner) expireChunk(id chunk.ChunkID) {
	clusterMode := r.applyRaftDelete != nil
	if clusterMode {
		if err := r.applyRaftDelete(id); err != nil {
			r.logger.Warn("retention: raft delete failed, will retry",
				"vault", r.vaultID, "chunk", id.String(), "error", err)
			return
		}
	}

	if err := r.im.DeleteIndexes(id); err != nil {
		r.logger.Error("retention: failed to delete indexes",
			"vault", r.vaultID, "chunk", id.String(), "error", err)
		return
	}

	if err := r.cm.Delete(id); err != nil {
		// In cluster mode, the OnDelete cascade may have already deleted
		// the chunk locally — ErrChunkNotFound is expected, not an error.
		if clusterMode && errors.Is(err, chunk.ErrChunkNotFound) {
			r.logger.Debug("retention: chunk already removed by OnDelete cascade",
				"vault", r.vaultID, "chunk", id.String())
		} else {
			r.logger.Error("retention: failed to delete chunk",
				"vault", r.vaultID, "chunk", id.String(), "error", err)
			return
		}
	}

	if r.orch != nil {
		// Record the successful retention deletion for the per-tier rate
		// alerter. Only counted on the leader path (this function only
		// runs on tier leaders) so the rate reflects active expiration
		// decisions, not follower OnDelete cascades. See gastrolog-47qyw.
		r.orch.retentionRates.Record(r.tierID, r.orch.now())
		// Notify WatchChunks subscribers: a chunk has been removed.
		r.orch.NotifyChunkChange()

		// Delete from same-node follower instances.
		r.orch.deleteFromFollowers(r.vaultID, r.tierID, id)
		// Forward deletion to remote follower nodes.
		r.forwardDeletionToFollowers(id, clusterMode)
	}

	r.logger.Debug("retention: deleted chunk",
		"vault", r.vaultID, "chunk", id.String())
}

// forwardDeletionToFollowers sends an explicit delete RPC to each remote
// follower. In cluster mode this is redundant with the FSM OnDelete cascade
// (which already deleted the chunk everywhere), so "chunk not found" responses
// are logged at debug only. In single-node / pre-cascade mode the forward is
// the primary deletion mechanism for followers and any error is a real failure.
func (r *retentionRunner) forwardDeletionToFollowers(id chunk.ChunkID, clusterMode bool) {
	for _, target := range r.followerTargets {
		if target.NodeID == r.orch.localNodeID {
			continue // already handled by deleteFromFollowers
		}
		err := r.sendDeleteToFollower(target.NodeID, id)
		if err == nil {
			continue
		}
		if clusterMode && strings.Contains(err.Error(), "chunk not found") {
			r.logger.Debug("retention: follower already removed chunk via OnDelete cascade",
				"vault", r.vaultID, "chunk", id.String(), "follower", target.NodeID)
			continue
		}
		r.logger.Warn("retention: failed to forward chunk deletion to follower",
			"vault", r.vaultID, "chunk", id.String(),
			"follower", target.NodeID, "error", err)
	}
}

// sendDeleteToFollower issues a single chunk-delete RPC via the tier
// replicator. Returns nil when no replicator is configured (single-node mode).
func (r *retentionRunner) sendDeleteToFollower(followerID string, id chunk.ChunkID) error {
	if r.orch.tierReplicator == nil {
		return nil
	}
	return r.orch.tierReplicator.DeleteChunk(
		context.Background(), followerID, r.vaultID, r.tierID, id,
	)
}
