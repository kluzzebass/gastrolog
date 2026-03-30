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
// StorageID disambiguates same-node primary/secondary instances of the same tier.
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
// Only primaries get runners — the primary decides what expires, then fans
// out deletions to all secondaries (same-node and cross-node).
type retentionRunner struct {
	mu         sync.Mutex
	vaultID    uuid.UUID
	tierID     uuid.UUID
	cm         chunk.ChunkManager
	im         index.IndexManager
	inflight   map[chunk.ChunkID]bool // chunks currently being processed
	unreadable map[chunk.ChunkID]bool // chunks that failed to read — skipped until restart
	orch       *Orchestrator          // for eject action (loadConfig, Append, transferrer)

	secondaryNodeIDs []string         // cross-node secondary targets for deletion fan-out
	now              func() time.Time
	logger           *slog.Logger
}

// retentionSweepAll is the single scheduled retention job. Each tick it
// discovers all local PRIMARY tier instances, resolves their retention
// rules from the current config, and sweeps each one. Secondaries never
// run retention — the primary is the sole authority on chunk lifecycle.
func (o *Orchestrator) retentionSweepAll() {
	cfg, err := o.loadConfig(context.Background())
	if err != nil {
		o.logger.Error("retention: failed to load config", "error", err)
		return
	}
	if cfg == nil {
		return
	}

	type sweepTarget struct {
		runner *retentionRunner
		rules  []retentionRule
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
			if tier.IsSecondary {
				continue // only primaries make retention decisions
			}
			tierCfg := findTierConfig(cfg.Tiers, tier.TierID)
			if tierCfg == nil || len(tierCfg.RetentionRules) == 0 {
				continue
			}
			rules, err := resolveRetentionRulesFromTier(cfg, vaultCfg, tierCfg)
			if err != nil {
				o.logger.Warn("retention: failed to resolve rules",
					"vault", vaultCfg.ID, "tier", tier.TierID, "error", err)
				continue
			}
			if len(rules) == 0 {
				continue
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
			// Refresh secondary targets from config each tick.
			runner.secondaryNodeIDs = tierCfg.SecondaryNodeIDs(cfg.NodeStorageConfigs)
			targets = append(targets, sweepTarget{runner: runner, rules: rules})
		}
	}
	// Prune runners for tier instances that no longer exist.
	for key := range o.retention {
		if !active[key] {
			delete(o.retention, key)
		}
	}
	o.mu.Unlock()

	// Execute sweeps without holding o.mu — sweep callbacks (expireChunk,
	// deleteFromSecondaries, ejectChunk) need to acquire it.
	for _, t := range targets {
		t.runner.sweep(t.rules)
	}
}

// sweep evaluates retention rules and applies expire/eject/transition actions.
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

	// Filter to sealed chunks only, skipping unreadable ones.
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

	// Track already-processed chunk IDs to avoid double-processing across rules.
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

			// Skip chunks already being processed by an overlapping sweep.
			r.mu.Lock()
			if r.inflight[id] {
				r.mu.Unlock()
				continue
			}
			r.inflight[id] = true
			r.mu.Unlock()

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

// markUnreadable flags a chunk as unreadable so retention skips it on future
// sweeps. The chunk remains on disk for manual investigation. Cleared on restart.
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

// expireChunk deletes a chunk locally, then fans out the deletion to all
// secondaries: same-node instances via direct call, cross-node via RPC.
func (r *retentionRunner) expireChunk(id chunk.ChunkID) {
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

	// Delete from same-node secondary instances.
	if r.orch != nil {
		r.orch.deleteFromSecondaries(r.vaultID, r.tierID, id)
	}

	// Fan out deletion to cross-node secondaries.
	r.deleteFromRemoteSecondaries(id)

	r.logger.Info("retention: deleted chunk",
		"vault", r.vaultID, "chunk", id.String())
}

// deleteFromRemoteSecondaries sends ForwardDeleteChunk RPCs to all cross-node
// secondary nodes. Best-effort: failures are logged but don't block the
// primary's retention. The next sweep tick will find the chunk already gone
// on the primary, but any failed secondary will still have it — eventual
// consistency handled by periodic reconciliation (future).
func (r *retentionRunner) deleteFromRemoteSecondaries(id chunk.ChunkID) {
	if r.orch == nil || r.orch.transferrer == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, nodeID := range r.secondaryNodeIDs {
		if nodeID == r.orch.localNodeID {
			continue // same-node secondaries handled by deleteFromSecondaries
		}
		if err := r.orch.transferrer.DeleteRemoteChunk(ctx, nodeID, r.vaultID, r.tierID, id); err != nil {
			r.logger.Warn("retention: failed to delete chunk on secondary",
				"vault", r.vaultID, "tier", r.tierID, "chunk", id.String(),
				"node", nodeID, "error", err)
		}
	}
}
