package orchestrator

import (
	"fmt"
	"log/slog"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// cronJobName returns the scheduler job name for a store's cron rotation.
func cronJobName(storeID uuid.UUID) string {
	return fmt.Sprintf("cron-rotate:%s", storeID)
}

// cronRotationManager manages cron-based chunk rotation jobs on the shared scheduler.
// It doesn't own a scheduler — it registers/removes named jobs on the orchestrator's
// shared Scheduler so all scheduled tasks are centrally visible.
type cronRotationManager struct {
	scheduler *Scheduler
	onSeal    func(storeID uuid.UUID, chunkID chunk.ChunkID) // called after sealing to trigger compression + indexing
	logger    *slog.Logger
}

func newCronRotationManager(scheduler *Scheduler, logger *slog.Logger) *cronRotationManager {
	return &cronRotationManager{
		scheduler: scheduler,
		logger:    logger,
	}
}

// addJob registers a cron rotation job for a store.
func (m *cronRotationManager) addJob(storeID uuid.UUID, storeName, cronExpr string, cm chunk.ChunkManager) error {
	name := cronJobName(storeID)
	if err := m.scheduler.AddJob(name, cronExpr, m.rotateStore, storeID, cm); err != nil {
		return err
	}
	m.scheduler.Describe(name, fmt.Sprintf("Rotate active chunk in '%s'", storeName))
	return nil
}

// removeJob stops and removes the cron rotation job for a store.
func (m *cronRotationManager) removeJob(storeID uuid.UUID) {
	m.scheduler.RemoveJob(cronJobName(storeID))
}

// updateJob replaces the cron rotation job for a store with a new schedule.
func (m *cronRotationManager) updateJob(storeID uuid.UUID, storeName, cronExpr string, cm chunk.ChunkManager) error {
	name := cronJobName(storeID)
	if err := m.scheduler.UpdateJob(name, cronExpr, m.rotateStore, storeID, cm); err != nil {
		return err
	}
	m.scheduler.Describe(name, fmt.Sprintf("Rotate active chunk in '%s'", storeName))
	return nil
}

// hasJob returns true if a cron rotation job exists for a store.
func (m *cronRotationManager) hasJob(storeID uuid.UUID) bool {
	return m.scheduler.HasJob(cronJobName(storeID))
}

// rotateStore seals the active chunk for a store if it has records.
func (m *cronRotationManager) rotateStore(storeID uuid.UUID, cm chunk.ChunkManager) {
	active := cm.Active()
	if active == nil || active.RecordCount == 0 {
		m.logger.Debug("cron rotation: skipping empty chunk",
			"store", storeID)
		return
	}

	sealedID := active.ID
	if err := cm.Seal(); err != nil {
		m.logger.Error("cron rotation: failed to seal chunk",
			"store", storeID, "chunk", sealedID.String(), "error", err)
		return
	}

	m.logger.Info("rotating chunk",
		"trigger", "cron",
		"store", storeID,
		"chunk", sealedID.String(),
		"bytes", active.Bytes,
		"records", active.RecordCount,
	)

	if m.onSeal != nil {
		m.onSeal(storeID, sealedID)
	}
}
