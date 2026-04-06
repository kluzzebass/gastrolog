package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
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
	applyRaftDelete           func(id chunk.ChunkID) error
	applyRaftRetentionPending func(id chunk.ChunkID) error

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
	runner.isLeader = tier.IsLeader()
	runner.followerTargets = tier.FollowerTargets
	return &sweepTarget{runner: runner, rules: rules}
}

// reconcileFollower compares on-disk chunks against the tier Raft manifest
// and deletes any sealed chunks that the primary has removed. This is the
// fallback for cases where OnDelete didn't fire (snapshot restore, startup,
// Raft connectivity gaps).
func (o *Orchestrator) reconcileFollower(tier *TierInstance) {
	// Don't reconcile until the tier Raft group has a leader. Without a leader,
	// the local FSM may be restored from a stale snapshot and hasn't caught up
	// with recent log entries. Deleting chunks against a stale manifest causes
	// permanent data loss — no mechanism re-transfers them.
	if tier.HasRaftLeader != nil && !tier.HasRaftLeader() {
		return
	}
	manifestIDs := tier.ListManifest()
	if len(manifestIDs) == 0 {
		return // manifest not yet populated — don't delete anything
	}
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
			_ = tier.Indexes.DeleteIndexes(meta.ID)
		}
		if err := tier.Chunks.Delete(meta.ID); err != nil {
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
	// Only the tier Raft leader runs retention. Followers must not
	// independently evaluate and transition chunks — that causes N×
	// duplication (every node transitions the same chunks).
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
	r.mu.Unlock()
	var sealed []chunk.ChunkMeta
	for _, meta := range metas {
		if meta.Sealed && !unreadable[meta.ID] {
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
		if len(matched) == 0 {
			continue
		}

		for _, id := range matched {
			if processed[id] {
				continue
			}
			processed[id] = true

			r.mu.Lock()
			if r.inflight[id] {
				r.mu.Unlock()
				continue
			}
			r.inflight[id] = true
			r.mu.Unlock()

			// Mark as retention-pending in the tier Raft so all nodes see it.
			if r.applyRaftRetentionPending != nil {
				_ = r.applyRaftRetentionPending(id)
			}

			func() {
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
			}()
		}
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

// expireChunk commits the deletion to the tier Raft manifest (so secondaries
// learn about it via OnDelete), then deletes locally. If the Raft apply fails,
// the local delete is skipped and retried next tick.
func (r *retentionRunner) expireChunk(id chunk.ChunkID) {
	if r.applyRaftDelete != nil {
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
		r.logger.Error("retention: failed to delete chunk",
			"vault", r.vaultID, "chunk", id.String(), "error", err)
		return
	}

	// Delete from same-node follower instances.
	if r.orch != nil {
		r.orch.deleteFromFollowers(r.vaultID, r.tierID, id)
	}

	// Forward deletion to remote follower nodes.
	if r.orch != nil {
		for _, target := range r.followerTargets {
			if target.NodeID == r.orch.localNodeID {
				continue // already handled by deleteFromFollowers
			}
			var err error
			if r.orch.tierReplicator != nil {
				err = r.orch.tierReplicator.DeleteChunk(
					context.Background(), target.NodeID, r.vaultID, r.tierID, id,
				)
			} else if r.orch.transferrer != nil {
				err = r.orch.transferrer.ForwardDeleteChunk(
					context.Background(), target.NodeID, r.vaultID, r.tierID, id,
				)
			}
			if err != nil {
				r.logger.Warn("retention: failed to forward chunk deletion to follower",
					"vault", r.vaultID, "chunk", id.String(),
					"follower", target.NodeID, "error", err)
			}
		}
	}

	r.logger.Debug("retention: deleted chunk",
		"vault", r.vaultID, "chunk", id.String())
}
