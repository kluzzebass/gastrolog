package orchestrator

import (
	"fmt"
	"log/slog"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// cronJobName returns the scheduler job name for a vault's cron rotation.
func cronJobName(vaultID uuid.UUID) string {
	return fmt.Sprintf("cron-rotate:%s", vaultID)
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

// addJob registers a cron rotation job for a vault.
func (m *cronRotationManager) addJob(vaultID uuid.UUID, vaultName, cronExpr string, cm chunk.ChunkManager) error {
	name := cronJobName(vaultID)
	if err := m.scheduler.AddJob(name, cronExpr, m.rotateVault, vaultID, cm); err != nil {
		return err
	}
	m.scheduler.Describe(name, fmt.Sprintf("Rotate active chunk in '%s'", vaultName))
	return nil
}

// removeJob stops and removes the cron rotation job for a vault.
func (m *cronRotationManager) removeJob(vaultID uuid.UUID) {
	m.scheduler.RemoveJob(cronJobName(vaultID))
}

// updateJob replaces the cron rotation job for a vault with a new schedule.
func (m *cronRotationManager) updateJob(vaultID uuid.UUID, vaultName, cronExpr string, cm chunk.ChunkManager) error {
	name := cronJobName(vaultID)
	if err := m.scheduler.UpdateJob(name, cronExpr, m.rotateVault, vaultID, cm); err != nil {
		return err
	}
	m.scheduler.Describe(name, fmt.Sprintf("Rotate active chunk in '%s'", vaultName))
	return nil
}

// hasJob returns true if a cron rotation job exists for a vault.
func (m *cronRotationManager) hasJob(vaultID uuid.UUID) bool {
	return m.scheduler.HasJob(cronJobName(vaultID))
}

// rotateVault seals the active chunk for a vault if it has records.
func (m *cronRotationManager) rotateVault(vaultID uuid.UUID, cm chunk.ChunkManager) {
	active := cm.Active()
	if active == nil || active.RecordCount == 0 {
		m.logger.Debug("cron rotation: skipping empty chunk",
			"vault", vaultID)
		return
	}

	sealedID := active.ID
	if err := cm.Seal(); err != nil {
		m.logger.Error("cron rotation: failed to seal chunk",
			"vault", vaultID, "chunk", sealedID.String(), "error", err)
		return
	}

	m.logger.Info("rotating chunk",
		"trigger", "cron",
		"vault", vaultID,
		"chunk", sealedID.String(),
		"bytes", active.Bytes,
		"records", active.RecordCount,
	)

	if m.onSeal != nil {
		m.onSeal(vaultID, sealedID)
	}
}
