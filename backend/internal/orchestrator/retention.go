package orchestrator

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

const defaultRetentionSchedule = "* * * * *" // every minute

// retentionJobName returns the scheduler job name for a store's retention sweep.
func retentionJobName(storeID string) string {
	return fmt.Sprintf("retention:%s", storeID)
}

// retentionRunner manages the retention sweep for a single store.
// It is invoked by the shared scheduler on a cron schedule.
type retentionRunner struct {
	mu      sync.Mutex
	storeID string
	cm      chunk.ChunkManager
	im      index.IndexManager
	policy  chunk.RetentionPolicy
	now     func() time.Time
	logger  *slog.Logger
}

// setPolicy hot-swaps the retention policy.
func (r *retentionRunner) setPolicy(policy chunk.RetentionPolicy) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.policy = policy
}

// sweep evaluates the retention policy and deletes expired chunks.
func (r *retentionRunner) sweep() {
	r.mu.Lock()
	policy := r.policy
	r.mu.Unlock()

	if policy == nil {
		return
	}

	metas, err := r.cm.List()
	if err != nil {
		r.logger.Error("retention: failed to list chunks", "store", r.storeID, "error", err)
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

	state := chunk.StoreState{
		Chunks: sealed,
		Now:    r.now(),
	}

	toDelete := policy.Apply(state)
	if len(toDelete) == 0 {
		return
	}

	for _, id := range toDelete {
		// Delete indexes first, then chunk data.
		if err := r.im.DeleteIndexes(id); err != nil {
			r.logger.Error("retention: failed to delete indexes",
				"store", r.storeID, "chunk", id.String(), "error", err)
			continue
		}

		if err := r.cm.Delete(id); err != nil {
			r.logger.Error("retention: failed to delete chunk",
				"store", r.storeID, "chunk", id.String(), "error", err)
			continue
		}

		r.logger.Info("retention: deleted chunk",
			"store", r.storeID, "chunk", id.String())
	}
}
