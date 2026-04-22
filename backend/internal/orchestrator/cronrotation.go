package orchestrator

import (
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"

	"gastrolog/internal/chunk"
)

// cronJobName returns the scheduler job name for a tier's cron rotation.
func cronJobName(vaultID, tierID glid.GLID) string {
	return fmt.Sprintf("cron-rotate:%s:%s", vaultID, tierID)
}

// cronRotationManager manages cron-based chunk rotation jobs on the shared scheduler.
type cronRotationManager struct {
	scheduler  *Scheduler
	schedules  map[string]string // jobName → cronExpr (tracks current schedule to avoid unnecessary updates)
	onSeal     func(vaultID glid.GLID, cm chunk.ChunkManager, chunkID chunk.ChunkID)
	onRotation func(vaultID, tierID glid.GLID) // optional: called once per successful rotation
	logger     *slog.Logger
}

func newCronRotationManager(scheduler *Scheduler, logger *slog.Logger) *cronRotationManager {
	return &cronRotationManager{
		scheduler: scheduler,
		schedules: make(map[string]string),
		logger:    logger,
	}
}

// ensure creates or updates a cron rotation job only if the schedule changed.
func (m *cronRotationManager) ensure(vaultID, tierID glid.GLID, vaultName, cronExpr string, cm chunk.ChunkManager) {
	name := cronJobName(vaultID, tierID)
	if existing, ok := m.schedules[name]; ok && existing == cronExpr {
		return // schedule unchanged
	}
	// Remove old job if schedule changed.
	if _, ok := m.schedules[name]; ok {
		m.scheduler.RemoveJob(name)
	}
	if err := m.scheduler.AddJob(name, cronExpr, m.rotateVault, vaultID, tierID, vaultName, cm); err != nil {
		m.logger.Error("cron rotation: failed to add job",
			"vault", vaultID, "tier", tierID, "cron", cronExpr, "error", err)
		return
	}
	m.scheduler.Describe(name, fmt.Sprintf("Rotate active chunk in '%s'", vaultName))
	m.schedules[name] = cronExpr
}

// pruneExcept removes all cron rotation jobs NOT in the active set.
func (m *cronRotationManager) pruneExcept(active map[string]bool) {
	for name := range m.schedules {
		if !active[name] {
			m.scheduler.RemoveJob(name)
			delete(m.schedules, name)
		}
	}
}

// removeAllForVault eagerly removes all cron jobs for a vault being unregistered.
func (m *cronRotationManager) removeAllForVault(vaultID glid.GLID) {
	prefix := fmt.Sprintf("cron-rotate:%s:", vaultID)
	m.scheduler.RemoveJobsByPrefix(prefix)
	for name := range m.schedules {
		if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			delete(m.schedules, name)
		}
	}
}

// rotateVault seals the active chunk for a vault tier if it has records.
func (m *cronRotationManager) rotateVault(vaultID, tierID glid.GLID, vaultName string, cm chunk.ChunkManager) {
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
		"tier", tierID,
		"name", vaultName,
		"chunk", sealedID.String(),
		"bytes", active.Bytes,
		"records", active.RecordCount,
	)

	if m.onRotation != nil {
		m.onRotation(vaultID, tierID)
	}
	if m.onSeal != nil {
		m.onSeal(vaultID, cm, sealedID)
	}
}
