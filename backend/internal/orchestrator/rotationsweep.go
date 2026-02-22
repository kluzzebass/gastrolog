package orchestrator

import (
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

const (
	rotationSweepJobName  = "rotation-sweep"
	rotationSweepSchedule = "*/15 * * * * *" // every 15 seconds
)

// rotationSweep checks all stores for active chunks that need rotation
// based on their current rotation policy (e.g., age exceeded).
// This runs as a scheduled job so time-based policies trigger even when
// no records are being appended to a store.
func (o *Orchestrator) rotationSweep() {
	// Collect seals under the read lock.
	type sealEvent struct {
		storeID uuid.UUID
		chunkID chunk.ChunkID
	}
	var seals []sealEvent

	o.mu.RLock()
	for id, store := range o.stores {
		activeBefore := store.Chunks.Active()
		if trigger := store.Chunks.CheckRotation(); trigger != nil {
			o.logger.Info("background rotation triggered",
				"store", id,
				"trigger", *trigger,
			)
			if activeBefore != nil {
				seals = append(seals, sealEvent{storeID: id, chunkID: activeBefore.ID})
			}
		}
	}
	o.mu.RUnlock()

	// Schedule compression + index builds outside the outer lock.
	// postSealWork acquires its own lock internally.
	for _, s := range seals {
		o.postSealWork(s.storeID, s.chunkID)
	}
}
