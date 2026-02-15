package orchestrator

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/google/uuid"
)

// JobStatus represents the lifecycle state of a job.
type JobStatus int

const (
	JobStatusPending   JobStatus = 1
	JobStatusRunning   JobStatus = 2
	JobStatusCompleted JobStatus = 3
	JobStatusFailed    JobStatus = 4
)

// JobProgress tracks progress counters and errors for a running or completed job.
// Methods are safe for concurrent use.
type JobProgress struct {
	mu           sync.RWMutex
	Status       JobStatus
	ChunksTotal  int64
	ChunksDone   int64
	RecordsDone  int64
	Error        string
	ErrorDetails []string
	StartedAt    time.Time
	CompletedAt  time.Time
}

// SetRunning transitions the job to Running and sets the total chunk count.
func (p *JobProgress) SetRunning(chunksTotal int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Status = JobStatusRunning
	p.ChunksTotal = chunksTotal
}

// IncrChunks increments the chunks-done counter.
func (p *JobProgress) IncrChunks() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ChunksDone++
}

// AddRecords adds n to the records-done counter.
func (p *JobProgress) AddRecords(n int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.RecordsDone += n
}

// Complete transitions the job to Completed.
func (p *JobProgress) Complete(now time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Status = JobStatusCompleted
	p.CompletedAt = now
}

// Fail transitions the job to Failed with an error message.
func (p *JobProgress) Fail(now time.Time, err string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Status = JobStatusFailed
	p.Error = err
	p.CompletedAt = now
}

// AddErrorDetail appends a per-chunk error detail.
func (p *JobProgress) AddErrorDetail(msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ErrorDetails = append(p.ErrorDetails, msg)
}

// JobInfo describes a registered job for external inspection.
type JobInfo struct {
	ID          string
	Name        string
	Description string    // human-readable description for the UI
	Schedule    string    // cron expression, or "once" for one-time jobs
	LastRun     time.Time // zero if never run
	NextRun     time.Time // zero if not scheduled
	Progress    *JobProgress
}

// Snapshot returns a read-consistent copy of the JobInfo's progress fields.
func (info JobInfo) Snapshot() JobInfo {
	if info.Progress == nil {
		return info
	}
	p := info.Progress
	p.mu.RLock()
	defer p.mu.RUnlock()
	info.Progress = &JobProgress{
		Status:       p.Status,
		ChunksTotal:  p.ChunksTotal,
		ChunksDone:   p.ChunksDone,
		RecordsDone:  p.RecordsDone,
		Error:        p.Error,
		ErrorDetails: append([]string(nil), p.ErrorDetails...),
		StartedAt:    p.StartedAt,
		CompletedAt:  p.CompletedAt,
	}
	return info
}

// cronEntry remembers a cron job's definition so it can be re-registered
// when the scheduler is rebuilt (e.g. to change the concurrency limit).
type cronEntry struct {
	name   string
	cron   string
	taskFn any
	args   []any
}

// Scheduler is the shared cron scheduler for the orchestrator.
// All subsystems (cron rotation, future scheduled tasks) register jobs here
// rather than maintaining their own schedulers.
type Scheduler struct {
	mu            sync.Mutex
	scheduler     gocron.Scheduler
	jobs          map[string]gocron.Job    // name → job
	schedules     map[string]string        // name → cron expression (for ListJobs)
	descriptions  map[string]string        // name → human-readable description
	cronEntries   map[string]cronEntry     // name → definition (for rebuild)
	progress      map[string]*JobProgress  // gocron job ID → progress (one-time jobs)
	completed     map[string]JobInfo       // gocron job ID → info (retained after gocron removes one-time jobs)
	maxConcurrent int
	now           func() time.Time
	logger        *slog.Logger
}

func newScheduler(logger *slog.Logger, maxConcurrent int, now func() time.Time) (*Scheduler, error) {
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}
	s, err := gocron.NewScheduler(
		gocron.WithLimitConcurrentJobs(uint(maxConcurrent), gocron.LimitModeWait),
	)
	if err != nil {
		return nil, fmt.Errorf("create cron scheduler: %w", err)
	}
	sched := &Scheduler{
		scheduler:     s,
		jobs:          make(map[string]gocron.Job),
		schedules:     make(map[string]string),
		descriptions:  make(map[string]string),
		cronEntries:   make(map[string]cronEntry),
		progress:      make(map[string]*JobProgress),
		completed:     make(map[string]JobInfo),
		maxConcurrent: maxConcurrent,
		now:           now,
		logger:        logger,
	}
	// Start immediately so RunOnce jobs execute even without explicit Start().
	// Cron jobs added later will begin executing as soon as they're registered.
	s.Start()
	return sched, nil
}

// MaxConcurrent returns the current concurrency limit.
func (s *Scheduler) MaxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxConcurrent
}

// Rebuild recreates the gocron scheduler with a new concurrency limit,
// re-registering all cron jobs. One-time jobs are ephemeral and not preserved.
func (s *Scheduler) Rebuild(maxConcurrent int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}

	// Shut down old scheduler.
	if err := s.scheduler.Shutdown(); err != nil {
		s.logger.Warn("error shutting down old scheduler during rebuild", "error", err)
	}

	// Create new scheduler with updated limit.
	gs, err := gocron.NewScheduler(
		gocron.WithLimitConcurrentJobs(uint(maxConcurrent), gocron.LimitModeWait),
	)
	if err != nil {
		return fmt.Errorf("rebuild scheduler: %w", err)
	}

	s.scheduler = gs
	s.maxConcurrent = maxConcurrent
	s.jobs = make(map[string]gocron.Job, len(s.cronEntries))
	s.schedules = make(map[string]string, len(s.cronEntries))
	oldDescs := s.descriptions
	s.descriptions = make(map[string]string, len(s.cronEntries))

	// Re-register all cron jobs.
	for _, entry := range s.cronEntries {
		j, err := gs.NewJob(
			gocron.CronJob(entry.cron, true),
			gocron.NewTask(entry.taskFn, entry.args...),
			gocron.WithName(entry.name),
		)
		if err != nil {
			s.logger.Error("failed to re-register job during rebuild", "name", entry.name, "error", err)
			continue
		}
		s.jobs[entry.name] = j
		s.schedules[entry.name] = entry.cron
		if desc, ok := oldDescs[entry.name]; ok {
			s.descriptions[entry.name] = desc
		}
	}

	gs.Start()
	s.logger.Info("scheduler rebuilt", "maxConcurrent", maxConcurrent, "jobs", len(s.jobs))
	return nil
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
	s.cronEntries[name] = cronEntry{name: name, cron: cronExpr, taskFn: taskFn, args: args}
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
	delete(s.descriptions, name)
	delete(s.cronEntries, name)
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

// Describe sets a human-readable description for a named job.
func (s *Scheduler) Describe(name, description string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.descriptions[name] = description
}

// ListJobs returns info about all registered cron and one-time jobs,
// plus recently completed one-time jobs retained for status polling.
func (s *Scheduler) ListJobs() []JobInfo {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupCompletedLocked()

	infos := make([]JobInfo, 0, len(s.jobs)+len(s.completed))

	// Active jobs (cron + in-progress one-time).
	for name, j := range s.jobs {
		id := j.ID().String()
		info := JobInfo{
			ID:          id,
			Name:        name,
			Description: s.descriptions[name],
			Schedule:    s.schedules[name],
			Progress:    s.progress[id],
		}
		if lr, err := j.LastRun(); err == nil {
			info.LastRun = lr
		}
		if nr, err := j.NextRun(); err == nil {
			info.NextRun = nr
		}
		infos = append(infos, info)
	}

	// Completed one-time jobs (retained for polling).
	for _, info := range s.completed {
		infos = append(infos, info)
	}

	// Stable sort: scheduled jobs first (by name), then tasks (by name).
	slices.SortFunc(infos, func(a, b JobInfo) int {
		// Scheduled before one-time tasks.
		aScheduled := a.Schedule != ""
		bScheduled := b.Schedule != ""
		if aScheduled != bScheduled {
			if aScheduled {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.Name, b.Name)
	})

	return infos
}

// GetJob returns info about a single job by gocron ID.
func (s *Scheduler) GetJob(id string) (JobInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check completed first (one-time jobs removed from gocron).
	if info, ok := s.completed[id]; ok {
		return info, true
	}

	// Check active jobs.
	for name, j := range s.jobs {
		jID := j.ID().String()
		if jID == id {
			info := JobInfo{
				ID:          jID,
				Name:        name,
				Description: s.descriptions[name],
				Schedule:    s.schedules[name],
				Progress:    s.progress[jID],
			}
			if lr, err := j.LastRun(); err == nil {
				info.LastRun = lr
			}
			if nr, err := j.NextRun(); err == nil {
				info.NextRun = nr
			}
			return info, true
		}
	}

	return JobInfo{}, false
}

// JobSchedule returns the cron expression for a named job, or "" if not found.
func (s *Scheduler) JobSchedule(name string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schedules[name]
}

// Start is a no-op — the scheduler starts eagerly at creation time so that
// RunOnce jobs can execute without requiring an explicit Start() call.
// Retained for API compatibility with the orchestrator lifecycle.
func (s *Scheduler) Start() {}

// RunOnce schedules a one-time job that runs immediately. The job is
// automatically removed from the active maps after completion, but its
// progress info is retained for status polling.
func (s *Scheduler) RunOnce(name string, taskFn any, args ...any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, err := s.scheduler.NewJob(
		gocron.OneTimeJob(gocron.OneTimeJobStartImmediately()),
		gocron.NewTask(taskFn, args...),
		gocron.WithName(name),
		gocron.WithEventListeners(
			gocron.AfterJobRuns(func(_ uuid.UUID, jobName string) {
				s.completeOneTimeJob(jobName)
			}),
			gocron.AfterJobRunsWithError(func(_ uuid.UUID, jobName string, _ error) {
				s.completeOneTimeJob(jobName)
			}),
		),
	)
	if err != nil {
		return fmt.Errorf("create one-time job %s: %w", name, err)
	}

	s.jobs[name] = j
	s.schedules[name] = "once"
	s.logger.Info("one-time job scheduled", "name", name)
	return nil
}

// Submit schedules a one-time job with progress tracking. Returns the gocron
// job ID. The fn receives a context (detached from the caller) and a
// JobProgress for reporting progress.
func (s *Scheduler) Submit(name string, fn func(context.Context, *JobProgress)) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	prog := &JobProgress{
		Status:    JobStatusPending,
		StartedAt: s.now(),
	}

	wrapper := func() {
		prog.SetRunning(0)
		ctx := context.WithoutCancel(context.Background())
		fn(ctx, prog)
		// If fn didn't explicitly complete/fail, mark completed.
		prog.mu.RLock()
		status := prog.Status
		prog.mu.RUnlock()
		if status == JobStatusRunning {
			prog.Complete(s.now())
		}
		s.logger.Info("job finished", "name", name)
	}

	j, err := s.scheduler.NewJob(
		gocron.OneTimeJob(gocron.OneTimeJobStartImmediately()),
		gocron.NewTask(wrapper),
		gocron.WithName(name),
		gocron.WithEventListeners(
			gocron.AfterJobRuns(func(_ uuid.UUID, jobName string) {
				s.completeOneTimeJob(jobName)
			}),
			gocron.AfterJobRunsWithError(func(_ uuid.UUID, jobName string, _ error) {
				s.completeOneTimeJob(jobName)
			}),
		),
	)
	if err != nil {
		s.logger.Error("failed to schedule job", "name", name, "error", err)
		prog.Fail(s.now(), "failed to schedule: "+err.Error())
		// Generate an ID for the failed job so the caller can still look it up.
		failedID := uuid.Must(uuid.NewV7()).String()
		s.completed[failedID] = JobInfo{
			ID:          failedID,
			Name:        name,
			Description: s.descriptions[name],
			Schedule:    "once",
			Progress:    prog,
		}
		return failedID
	}

	id := j.ID().String()
	s.jobs[name] = j
	s.schedules[name] = "once"
	s.progress[id] = prog
	s.logger.Info("job submitted", "name", name, "id", id)
	return id
}

// completeOneTimeJob moves a finished one-time job from the active maps
// to the completed map so its progress remains available for polling.
func (s *Scheduler) completeOneTimeJob(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	j, ok := s.jobs[name]
	if !ok {
		return
	}

	id := j.ID().String()
	info := JobInfo{
		ID:          id,
		Name:        name,
		Description: s.descriptions[name],
		Schedule:    "once",
		Progress:    s.progress[id],
	}
	if lr, err := j.LastRun(); err == nil {
		info.LastRun = lr
	}

	s.completed[id] = info
	delete(s.jobs, name)
	delete(s.schedules, name)
	delete(s.descriptions, name)
	delete(s.progress, id)
}

// cleanupCompletedLocked removes completed jobs older than 1 hour.
// Must be called with s.mu held.
func (s *Scheduler) cleanupCompletedLocked() {
	cutoff := s.now().Add(-1 * time.Hour)
	for id, info := range s.completed {
		if info.Progress == nil {
			delete(s.completed, id)
			continue
		}
		info.Progress.mu.RLock()
		completedAt := info.Progress.CompletedAt
		info.Progress.mu.RUnlock()
		if !completedAt.IsZero() && completedAt.Before(cutoff) {
			delete(s.completed, id)
		}
	}
}

// Stop shuts down the scheduler and waits for running jobs to finish.
func (s *Scheduler) Stop() error {
	return s.scheduler.Shutdown()
}
