package orchestrator

import (
	"fmt"
	"log/slog"

	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// cronJobName returns the scheduler job name for a tier's cron rotation.
func cronJobName(vaultID, tierID uuid.UUID) string {
	return fmt.Sprintf("cron-rotate:%s:%s", vaultID, tierID)
}

// cronRotationManager manages cron-based chunk rotation jobs on the shared scheduler.
// It doesn't own a scheduler — it registers/removes named jobs on the orchestrator's
// shared Scheduler so all scheduled tasks are centrally visible.
type cronRotationManager struct {
	scheduler *Scheduler
	onSeal    func(vaultID uuid.UUID, cm chunk.ChunkManager, chunkID chunk.ChunkID) // called after sealing to trigger compression + indexing
	logger    *slog.Logger
}

func newCronRotationManager(scheduler *Scheduler, logger *slog.Logger) *cronRotationManager {
	return &cronRotationManager{
		scheduler: scheduler,
		logger:    logger,
	}
}

// addJob registers a cron rotation job for a tier.
func (m *cronRotationManager) addJob(vaultID, tierID uuid.UUID, vaultName, cronExpr string, cm chunk.ChunkManager) error {
	name := cronJobName(vaultID, tierID)
	if err := m.scheduler.AddJob(name, cronExpr, m.rotateVault, vaultID, vaultName, cm); err != nil {
		return err
	}
	m.scheduler.Describe(name, fmt.Sprintf("Rotate active chunk in '%s'", vaultName))
	return nil
}

// removeJob stops and removes the cron rotation job for a tier.
func (m *cronRotationManager) removeJob(vaultID, tierID uuid.UUID) {
	m.scheduler.RemoveJob(cronJobName(vaultID, tierID))
}

// removeAllForVault stops and removes all cron rotation jobs for a vault's tiers.
func (m *cronRotationManager) removeAllForVault(vaultID uuid.UUID) {
	m.scheduler.RemoveJobsByPrefix(fmt.Sprintf("cron-rotate:%s:", vaultID))
}

// updateJob replaces the cron rotation job for a tier with a new schedule.
func (m *cronRotationManager) updateJob(vaultID, tierID uuid.UUID, vaultName, cronExpr string, cm chunk.ChunkManager) error {
	name := cronJobName(vaultID, tierID)
	if err := m.scheduler.UpdateJob(name, cronExpr, m.rotateVault, vaultID, vaultName, cm); err != nil {
		return err
	}
	m.scheduler.Describe(name, fmt.Sprintf("Rotate active chunk in '%s'", vaultName))
	return nil
}

// hasJob returns true if a cron rotation job exists for a tier.
func (m *cronRotationManager) hasJob(vaultID, tierID uuid.UUID) bool {
	return m.scheduler.HasJob(cronJobName(vaultID, tierID))
}

// rotateVault seals the active chunk for a vault if it has records.
func (m *cronRotationManager) rotateVault(vaultID uuid.UUID, vaultName string, cm chunk.ChunkManager) {
	active := cm.Active()
	if active == nil || active.RecordCount == 0 {
		m.logger.Debug("cron rotation: skipping empty chunk",
			"vault", vaultID, "name", vaultName)
		return
	}

	sealedID := active.ID
	if err := cm.Seal(); err != nil {
		m.logger.Error("cron rotation: failed to seal chunk",
			"vault", vaultID, "name", vaultName, "chunk", sealedID.String(), "error", err)
		return
	}

	m.logger.Info("rotating chunk",
		"trigger", "cron",
		"vault", vaultID,
		"name", vaultName,
		"chunk", sealedID.String(),
		"bytes", active.Bytes,
		"records", active.RecordCount,
	)

	if m.onSeal != nil {
		m.onSeal(vaultID, cm, sealedID)
	}
}
