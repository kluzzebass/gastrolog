package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
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
	applyRaftDelete func(id chunk.ChunkID) error

	isSecondary bool // secondaries force all actions to expire
	now         func() time.Time
	logger      *slog.Logger
}

type sweepTarget struct {
	runner *retentionRunner
	rules  []retentionRule
}

// retentionSweepAll is the single scheduled retention job. Each tick it
// discovers all local tier instances and evaluates retention rules.
//
// Primaries: evaluate rules, apply CmdDeleteChunk to tier Raft, delete locally.
// Secondaries: evaluate the same rules but force all actions to expire.
//
// Additionally, the tier Raft's OnDelete callback provides real-time
// propagation when connected (see wireOnDeleteCallback). The scheduled
// reconciliation (reconcileSecondary) handles gaps when Raft isn't connected.
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
	active := make(map[string]bool)

	o.mu.Lock()
	for _, vaultCfg := range cfg.Vaults {
		vault := o.vaults[vaultCfg.ID]
		if vault == nil {
			continue
		}
		for _, tier := range vault.Tiers {
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

	for _, t := range targets {
		t.runner.sweep(t.rules)
	}
}

// retentionTargetForTier resolves a single tier instance into a sweep target.
// Returns nil if the tier should be skipped (no rules, no leader, etc.).
func (o *Orchestrator) retentionTargetForTier(cfg *config.Config, vaultCfg config.VaultConfig, tier *TierInstance, active map[string]bool) *sweepTarget {
	if tier.HasRaftLeader != nil && !tier.HasRaftLeader() {
		return nil
	}
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
	runner.isSecondary = tier.IsSecondary
	if !tier.IsSecondary {
		runner.applyRaftDelete = tier.ApplyRaftDelete
	}
	return &sweepTarget{runner: runner, rules: rules}
}

// reconcileSecondary compares on-disk chunks against the tier Raft manifest
// and deletes any sealed chunks that the primary has removed. This is the
// fallback for cases where OnDelete didn't fire (snapshot restore, startup,
// Raft connectivity gaps).
func (o *Orchestrator) reconcileSecondary(tier *TierInstance) {
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
		o.logger.Info("reconcile: deleted orphaned chunk from secondary",
			"tier", tier.TierID, "chunk", meta.ID)
	}
}

// sweep evaluates retention rules on a primary and applies expire/eject/transition.
func (r *retentionRunner) sweep(rules []retentionRule) {
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

			func() {
				defer r.clearInflight(id)
				action := b.action
				if r.isSecondary {
					action = config.RetentionActionExpire
				}
				switch action {
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

	// Delete from same-node secondary instances that don't have their own
	// Raft-driven OnDelete (e.g., single-node mode without tier Raft).
	if r.orch != nil {
		r.orch.deleteFromSecondaries(r.vaultID, r.tierID, id)
	}

	r.logger.Info("retention: deleted chunk",
		"vault", r.vaultID, "chunk", id.String())
}
