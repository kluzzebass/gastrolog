package orchestrator

import (
	"fmt"
	"log/slog"

	"gastrolog/internal/chunk"

	"github.com/go-co-op/gocron/v2"
)

// cronRotationManager manages background cron-based chunk rotation jobs.
// It maintains a single gocron.Scheduler with one job per store that has
// a cron rotation schedule configured.
type cronRotationManager struct {
	scheduler gocron.Scheduler
	jobs      map[string]gocron.Job // storeID → job
	logger    *slog.Logger
}

func newCronRotationManager(logger *slog.Logger) (*cronRotationManager, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("create cron scheduler: %w", err)
	}
	return &cronRotationManager{
		scheduler: s,
		jobs:      make(map[string]gocron.Job),
		logger:    logger,
	}, nil
}

// addJob registers a cron rotation job for a store.
func (m *cronRotationManager) addJob(storeID, cronExpr string, cm chunk.ChunkManager) error {
	if _, exists := m.jobs[storeID]; exists {
		return fmt.Errorf("cron rotation job already exists for store %s", storeID)
	}

	j, err := m.scheduler.NewJob(
		gocron.CronJob(cronExpr, false),
		gocron.NewTask(m.rotateStore, storeID, cm),
		gocron.WithName(fmt.Sprintf("cron-rotate-%s", storeID)),
	)
	if err != nil {
		return fmt.Errorf("create cron rotation job for store %s: %w", storeID, err)
	}

	m.jobs[storeID] = j
	m.logger.Info("cron rotation job added", "store", storeID, "cron", cronExpr)
	return nil
}

// removeJob stops and removes the cron rotation job for a store.
func (m *cronRotationManager) removeJob(storeID string) {
	j, ok := m.jobs[storeID]
	if !ok {
		return
	}
	if err := m.scheduler.RemoveJob(j.ID()); err != nil {
		m.logger.Warn("failed to remove cron rotation job", "store", storeID, "error", err)
	}
	delete(m.jobs, storeID)
	m.logger.Info("cron rotation job removed", "store", storeID)
}

// updateJob replaces the cron rotation job for a store with a new schedule.
func (m *cronRotationManager) updateJob(storeID, cronExpr string, cm chunk.ChunkManager) error {
	m.removeJob(storeID)
	return m.addJob(storeID, cronExpr, cm)
}

// start begins executing all registered cron jobs.
func (m *cronRotationManager) start() {
	m.scheduler.Start()
	m.logger.Info("cron rotation scheduler started", "jobs", len(m.jobs))
}

// stop shuts down the scheduler and waits for running jobs to finish.
func (m *cronRotationManager) stop() error {
	return m.scheduler.Shutdown()
}

// rotateStore seals the active chunk for a store if it has records.
func (m *cronRotationManager) rotateStore(storeID string, cm chunk.ChunkManager) {
	active := cm.Active()
	if active == nil || active.RecordCount == 0 {
		m.logger.Debug("cron rotation: skipping empty chunk", "store", storeID)
		return
	}

	if err := cm.Seal(); err != nil {
		m.logger.Error("cron rotation: failed to seal chunk",
			"store", storeID, "chunk", active.ID.String(), "error", err)
		return
	}

	m.logger.Info("cron rotation: sealed chunk",
		"store", storeID, "chunk", active.ID.String(),
		"records", active.RecordCount, "bytes", active.Bytes)
}
