package orchestrator

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
)

// JobInfo describes a registered scheduled job for external inspection.
type JobInfo struct {
	ID       string    // unique job ID (gocron UUID)
	Name     string    // human-readable name (e.g. "cron-rotate:my-store")
	Schedule string    // cron expression
	LastRun  time.Time // zero if never run
	NextRun  time.Time // zero if not scheduled
}

// Scheduler is the shared cron scheduler for the orchestrator.
// All subsystems (cron rotation, future scheduled tasks) register jobs here
// rather than maintaining their own schedulers.
type Scheduler struct {
	mu        sync.Mutex
	scheduler gocron.Scheduler
	jobs      map[string]gocron.Job // name → job
	schedules map[string]string     // name → cron expression (for ListJobs)
	logger    *slog.Logger
}

func newScheduler(logger *slog.Logger) (*Scheduler, error) {
	s, err := gocron.NewScheduler()
	if err != nil {
		return nil, fmt.Errorf("create cron scheduler: %w", err)
	}
	return &Scheduler{
		scheduler: s,
		jobs:      make(map[string]gocron.Job),
		schedules: make(map[string]string),
		logger:    logger,
	}, nil
}

// AddJob registers a named cron job. The name must be unique across all subsystems.
// The task function and its arguments are passed to gocron.NewTask.
func (s *Scheduler) AddJob(name, cronExpr string, taskFn any, args ...any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[name]; exists {
		return fmt.Errorf("scheduled job already exists: %s", name)
	}

	j, err := s.scheduler.NewJob(
		gocron.CronJob(cronExpr, true),
		gocron.NewTask(taskFn, args...),
		gocron.WithName(name),
	)
	if err != nil {
		return fmt.Errorf("create scheduled job %s: %w", name, err)
	}

	s.jobs[name] = j
	s.schedules[name] = cronExpr
	s.logger.Info("scheduled job added", "name", name, "cron", cronExpr)
	return nil
}

// RemoveJob stops and removes a named job. No-op if the job doesn't exist.
func (s *Scheduler) RemoveJob(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[name]
	if !ok {
		return
	}
	if err := s.scheduler.RemoveJob(j.ID()); err != nil {
		s.logger.Warn("failed to remove scheduled job", "name", name, "error", err)
	}
	delete(s.jobs, name)
	delete(s.schedules, name)
	s.logger.Info("scheduled job removed", "name", name)
}

// UpdateJob replaces a named job with a new schedule. If the job doesn't exist,
// it is created.
func (s *Scheduler) UpdateJob(name, cronExpr string, taskFn any, args ...any) error {
	s.RemoveJob(name)
	return s.AddJob(name, cronExpr, taskFn, args...)
}

// HasJob returns true if a job with the given name exists.
func (s *Scheduler) HasJob(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.jobs[name]
	return ok
}

// ListJobs returns info about all registered jobs.
func (s *Scheduler) ListJobs() []JobInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	infos := make([]JobInfo, 0, len(s.jobs))
	for name, j := range s.jobs {
		info := JobInfo{
			ID:       j.ID().String(),
			Name:     name,
			Schedule: s.schedules[name],
		}
		if lr, err := j.LastRun(); err == nil {
			info.LastRun = lr
		}
		if nr, err := j.NextRun(); err == nil {
			info.NextRun = nr
		}
		infos = append(infos, info)
	}
	return infos
}

// Start begins executing all registered jobs.
func (s *Scheduler) Start() {
	s.scheduler.Start()
	s.logger.Info("scheduler started", "jobs", len(s.jobs))
}

// Stop shuts down the scheduler and waits for running jobs to finish.
func (s *Scheduler) Stop() error {
	return s.scheduler.Shutdown()
}
