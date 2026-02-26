package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/config"
	"gastrolog/internal/index"

	"github.com/google/uuid"
)

const defaultRetentionSchedule = "* * * * *" // every minute

// retentionJobName returns the scheduler job name for a vault's retention sweep.
func retentionJobName(vaultID uuid.UUID) string {
	return fmt.Sprintf("retention:%s", vaultID)
}

// retentionRule is a resolved rule: a compiled policy paired with an action.
type retentionRule struct {
	policy      chunk.RetentionPolicy
	action      config.RetentionAction
	destination uuid.UUID // target vault ID, only for migrate
}

// retentionRunner manages the retention sweep for a single vault.
// It is invoked by the shared scheduler on a cron schedule.
type retentionRunner struct {
	mu       sync.Mutex
	vaultID  uuid.UUID
	cm       chunk.ChunkManager
	im       index.IndexManager
	rules []retentionRule
	orch     *Orchestrator // for MoveChunk on migrate action
	now      func() time.Time
	logger   *slog.Logger
}

// setRules hot-swaps the retention rules.
func (r *retentionRunner) setRules(rules []retentionRule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rules = rules
}

// sweep evaluates all retention rules and applies expire/migrate actions.
func (r *retentionRunner) sweep() {
	r.mu.Lock()
	rules := r.rules
	r.mu.Unlock()

	if len(rules) == 0 {
		return
	}

	metas, err := r.cm.List()
	if err != nil {
		r.logger.Error("retention: failed to list chunks", "vault", r.vaultID, "error", err)
		return
	}

	// Filter to sealed chunks only (sorted by StartTS from List).
	var sealed []chunk.ChunkMeta
	for _, meta := range metas {
		if meta.Sealed {
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

			switch b.action {
			case config.RetentionActionExpire:
				r.expireChunk(id)
			case config.RetentionActionMigrate:
				r.migrateChunk(id, b.destination)
			default:
				r.logger.Error("retention: unknown action", "vault", r.vaultID, "action", b.action)
			}
		}
	}
}

// expireChunk deletes a chunk's indexes then data.
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

	r.logger.Info("retention: deleted chunk",
		"vault", r.vaultID, "chunk", id.String())
}

// migrateChunk moves a single chunk to the destination vault.
func (r *retentionRunner) migrateChunk(id chunk.ChunkID, dstID uuid.UUID) {
	ctx := context.Background()
	if err := r.orch.MoveChunk(ctx, id, r.vaultID, dstID); err != nil {
		r.logger.Error("retention: failed to migrate chunk",
			"vault", r.vaultID, "chunk", id.String(), "destination", dstID, "error", err)
		return
	}
	r.logger.Info("retention: migrated chunk",
		"vault", r.vaultID, "chunk", id.String(), "destination", dstID)
}
