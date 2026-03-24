package orchestrator

import (
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

// retentionJobName returns the scheduler job name for a tier's retention sweep.
func retentionJobName(tierID uuid.UUID) string {
	return fmt.Sprintf("retention:%s", tierID)
}

// retentionRule is a resolved rule: a compiled policy paired with an action.
type retentionRule struct {
	policy        chunk.RetentionPolicy
	action        config.RetentionAction
	ejectRouteIDs []uuid.UUID // target route IDs, only for eject
}

// retentionRunner manages the retention sweep for a single tier.
// It is invoked by the shared scheduler on a cron schedule.
type retentionRunner struct {
	mu       sync.Mutex
	vaultID  uuid.UUID
	tierID   uuid.UUID
	cm       chunk.ChunkManager
	im       index.IndexManager
	rules    []retentionRule
	inflight map[chunk.ChunkID]bool // chunks currently being processed
	orch     *Orchestrator          // for eject action (loadConfig, Append, transferrer)
	now      func() time.Time
	logger   *slog.Logger
}

// setRules hot-swaps the retention rules.
func (r *retentionRunner) setRules(rules []retentionRule) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rules = rules
}

// sweep evaluates all retention rules and applies expire/eject actions.
func (r *retentionRunner) sweep() {
	r.mu.Lock()
	rules := r.rules
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

	// Filter to sealed chunks only (sorted by WriteStart from List).
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
