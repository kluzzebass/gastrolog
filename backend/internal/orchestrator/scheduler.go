package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"github.com/google/uuid"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-co-op/gocron/v2"
)

// JobStatus represents the lifecycle state of a job.
type JobStatus int

const (
	JobStatusPending   JobStatus = 1
	JobStatusRunning   JobStatus = 2
	JobStatusCompleted JobStatus = 3
	JobStatusFailed    JobStatus = 4
)

// String makes the status readable wherever a job is logged. Without it a
// stalled-job diagnostic reports "status=2", which is the same amount of
// information as reporting nothing.
func (s JobStatus) String() string {
	switch s {
	case JobStatusPending:
		return "pending"
	case JobStatusRunning:
		return "running"
	case JobStatusCompleted:
		return "completed"
	case JobStatusFailed:
		return "failed"
	default:
		return "unknown(" + strconv.Itoa(int(s)) + ")"
	}
}

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
	jobs          map[string]gocron.Job   // name → job
	schedules     map[string]string       // name → cron expression (for ListJobs)
	descriptions  map[string]string       // name → human-readable description
	cronEntries   map[string]cronEntry    // name → definition (for rebuild)
	progress      map[string]*JobProgress // gocron job ID → progress (one-time jobs)
	completed     map[string]JobInfo      // gocron job ID → info (retained after gocron removes one-time jobs)
	maxConcurrent int
	now           func() time.Time
	logger        *slog.Logger
	onJobChange   func() // optional; called (outside lock) when a job transitions state
	// events is the fan-out broker that emits per-transition JobEvents for
	// Watch-style subscribers (e.g. WatchJobs). Both onJobChange and events
	// fire at the same points — the callback is a single-slot legacy path
	// that will retire once all consumers migrate to the broker.
	events *JobEventBroker
}

func newScheduler(logger *slog.Logger, maxConcurrent int, now func() time.Time) (*Scheduler, error) {
	if maxConcurrent <= 0 {
		maxConcurrent = 4
	}
	// Two-level concurrency control:
	//   Global: LimitConcurrentJobs caps total goroutines across ALL jobs (LimitModeWait
	//           queues excess jobs — we want one-time tasks to wait, not be dropped).
	//   Per-job: WithSingletonMode on each cron job prevents self-overlap (LimitModeReschedule
	//           skips missed ticks — we want slow sweeps to finish, not queue a backlog).
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
		events:        NewJobEventBroker(0), // default buffer
	}
	// Start immediately so RunOnce jobs execute even without explicit Start().
	// Cron jobs added later will begin executing as soon as they're registered.
	s.Start()
	return sched, nil
}

// HasPendingPrefix returns true if any active job has a name starting with
// prefix. Used by tests to wait for async transitions to finish.
//
// Membership in s.jobs IS "pending": the completion path records the result in
// s.completed and deletes the job from s.jobs, so a name still present has not
// finished. This used to additionally test s.completed[name], which could never
// match — s.completed is keyed by job ID, not name — so the predicate silently
// degraded to the membership test it now performs openly. Harmless because the
// two agree, but a helper whose body claims a check it does not perform is how
// the next reader mistakes it for a dedup guard (gastrolog-1scomn).
//
// NOT a deduplication guard. Pairing it with RunOnce to mean "enqueue this
// work unless it is already queued" is a check-then-act race, and the answer
// goes stale the instant the job completes. Use RunOnceIfAbsent, which does
// the check and the registration under one lock hold. See gastrolog-3hwngy.
func (s *Scheduler) HasPendingPrefix(prefix string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name := range s.jobs {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

// WaitIdle blocks until all one-time jobs (RunOnce / Submit) have completed,
// reporting whether it got there before the timeout. Used in tests to drain
// async post-seal / replication work before asserting.
//
// The bool is the point: this returned nothing, so a caller that ran out of
// budget was indistinguishable from one that drained, and every test using it
// asserted against a half-finished scheduler on a loaded machine — passing
// alone, failing in the full suite. Callers in this package should go through
// requireIdle rather than reading the bool by hand.
func (s *Scheduler) WaitIdle(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		pending := 0
		// Same as HasPendingPrefix: a completed job is gone from s.jobs, and the
		// s.completed[name] test this used to perform was keyed by name against a
		// map keyed by job ID, so it never matched (gastrolog-1scomn).
		for name := range s.jobs {
			if sched, ok := s.schedules[name]; ok && sched == "once" {
				pending++
			}
		}
		s.mu.Unlock()
		if pending == 0 {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// MaxConcurrent returns the current concurrency limit.
func (s *Scheduler) MaxConcurrent() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxConcurrent
}

// SetOnJobChange registers a callback invoked (outside the lock) whenever a
// job transitions state — started, completed, or failed. Used by the cluster
// broadcast system for immediate peer notification.
//
// Deprecated (but kept live for back-compat while consumers migrate): new
// code should subscribe to Events() instead, which fans out to multiple
// listeners with per-event metadata rather than collapsing every transition
// into a single "something changed" pulse.
func (s *Scheduler) SetOnJobChange(fn func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onJobChange = fn
}

// Events returns the scheduler's job-event broker. Subscribers receive
// per-transition JobEvents and get their own bounded channel; slow
// subscribers drop events rather than stall the scheduler.
func (s *Scheduler) Events() *JobEventBroker { return s.events }

// publishEvent constructs a JobEvent and hands it to the broker. Called
// outside any lock so the broker's non-blocking publish stays fast.
func (s *Scheduler) publishEvent(kind JobEventKind, info JobInfo) {
	if s.events == nil {
		return
	}
	s.events.Publish(JobEvent{Kind: kind, Job: info})
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

	// Create new scheduler with updated limit. See newScheduler for the
	// two-level concurrency rationale (global Wait + per-job Reschedule).
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
			gocron.WithSingletonMode(gocron.LimitModeReschedule),
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

// ErrJobExists is returned by AddJob when a job of that name is already
// registered.
//
// AddJob's presence check and its registration happen under a single hold of
// s.mu, so this error IS the atomic "someone else got there first" answer.
// Callers whose registration is idempotent by nature (re-run on every config
// apply) should test for it with errors.Is rather than guarding the call with
// HasJob: the pre-check is a check-then-act race and it hides genuine
// registration failures behind a shape that looks deliberate. See
// gastrolog-69sjlj.
var ErrJobExists = errors.New("scheduled job already exists")

// AddJob registers a named cron job. The name must be unique across all subsystems.
// The task function and its arguments are passed to gocron.NewTask.
// Returns ErrJobExists if the name is already taken.
func (s *Scheduler) AddJob(name, cronExpr string, taskFn any, args ...any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.jobs[name]; exists {
		return fmt.Errorf("%w: %s", ErrJobExists, name)
	}

	j, err := s.scheduler.NewJob(
		gocron.CronJob(NormalizeCronSchedule(cronExpr), true),
		gocron.NewTask(taskFn, args...),
		gocron.WithName(name),
		gocron.WithSingletonMode(gocron.LimitModeReschedule),
	)
	if err != nil {
		return fmt.Errorf("create scheduled job %s: %w", name, err)
	}

	s.jobs[name] = j
	s.schedules[name] = NormalizeCronSchedule(cronExpr)
	s.cronEntries[name] = cronEntry{name: name, cron: NormalizeCronSchedule(cronExpr), taskFn: taskFn, args: args}
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

// RemoveJobsByPrefix stops and removes all jobs whose name starts with prefix.
// Used to cancel pending one-time jobs (compress, index-build) for a vault
// that is about to be closed, preventing use-after-close on the chunk manager.
func (s *Scheduler) RemoveJobsByPrefix(prefix string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for name, j := range s.jobs {
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		if err := s.scheduler.RemoveJob(j.ID()); err != nil {
			s.logger.Warn("failed to remove scheduled job", "name", name, "error", err)
		}
		delete(s.jobs, name)
		delete(s.schedules, name)
		delete(s.descriptions, name)
		delete(s.cronEntries, name)
	}
}

// countByPrefixLocked counts registered jobs whose name starts with prefix.
// Must be called with s.mu held — it is the budget half of
// RunOnceIfAbsentUnderLimit's decision and must not be observable apart from
// the registration it gates.
func (s *Scheduler) countByPrefixLocked(prefix string) int {
	n := 0
	for name := range s.jobs {
		if strings.HasPrefix(name, prefix) {
			n++
		}
	}
	return n
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
// progress info is retained for status polling. A job of the same name that
// is already registered is replaced — use RunOnceIfAbsent when the name is
// meant to be an idempotency key.
func (s *Scheduler) RunOnce(name string, taskFn any, args ...any) error {
	_, err := s.runOnce(name, nil, taskFn, args...)
	return err
}

// RunOnceIfAbsent schedules a one-time job only when no job of that name is
// currently registered, and reports whether it did. The name is the
// idempotency key: the presence check and the registration happen under a
// single hold of s.mu, so two callers racing on the same name cannot both
// enqueue.
//
// Callers must NOT open-code this as HasJob/HasPendingPrefix followed by
// RunOnce. That shape is a check-then-act race — both callers observe
// "absent" and both enqueue — and it is exactly how the cloud-upload paths
// double-enqueued a chunk (gastrolog-3hwngy). It is also why RunOnce's
// silent same-name overwrite is dangerous: the first job's completion
// listener deletes s.jobs[name], which by then points at the SECOND job, so
// the second job's own completion finds nothing and publishes no terminal
// event.
func (s *Scheduler) RunOnceIfAbsent(name string, taskFn any, args ...any) (bool, error) {
	return s.runOnce(name, &onceClaim{}, taskFn, args...)
}

// RunOnceIfAbsentUnderLimit is RunOnceIfAbsent with a budget: it also declines
// when limit or more jobs whose name starts with prefix are already
// registered. Both conditions and the registration are decided under a single
// hold of s.mu, so a stampede cannot overshoot the budget.
//
// This exists so a bounded pool of one-time jobs needs no second copy of "what
// is outstanding" beside the scheduler's own job map. The GLCB replica
// catch-up used to keep exactly that — a per-chunk inflight map with its own
// mutex, released by the job body's defer — which meant two owners of the same
// fact: cancelling those jobs with RemoveJobsByPrefix would have stranded the
// map entries and stopped those chunks from ever being pulled again. See
// gastrolog-69sjlj and the single-source-of-truth rule in CLAUDE.md.
//
// The claim is a lease on outstanding work, released when the job leaves the
// registry — on completion (completeOneTimeJob) or on cancellation
// (RemoveJob/RemoveJobsByPrefix), which is the point: cancelling the work also
// releases the right to redo it.
func (s *Scheduler) RunOnceIfAbsentUnderLimit(name, prefix string, limit int, taskFn any, args ...any) (bool, error) {
	return s.runOnce(name, &onceClaim{prefix: prefix, limit: limit}, taskFn, args...)
}

// onceClaim describes the conditions a one-time registration must satisfy.
// A zero value means "only when this exact name is absent"; a non-zero limit
// additionally caps how many jobs sharing prefix may be registered at once.
type onceClaim struct {
	prefix string
	limit  int // <= 0: unbounded
}

// runOnce is the shared registration body for RunOnce and the claiming
// variants. When claim is non-nil and its conditions are not met, nothing is
// registered and this returns (false, nil).
func (s *Scheduler) runOnce(name string, claim *onceClaim, taskFn any, args ...any) (bool, error) {
	return s.runOnceWith(name, claim, nil, taskFn, args...)
}

// runOnceWith is runOnce with an optional caller-supplied progress record. The
// record cannot be registered before the job exists — s.progress is keyed by
// job id — so it is threaded in and filed once gocron has minted one.
func (s *Scheduler) runOnceWith(name string, claim *onceClaim, prog *JobProgress, taskFn any, args ...any) (bool, error) {
	s.mu.Lock()

	if claim != nil {
		if _, exists := s.jobs[name]; exists {
			s.mu.Unlock()
			return false, nil
		}
		if claim.limit > 0 && s.countByPrefixLocked(claim.prefix) >= claim.limit {
			// Budget full, and the name is provably absent (checked just
			// above, under this same lock hold). Any description sitting
			// under an unregistered name is garbage — it belongs to this
			// declined attempt, since callers describe before scheduling so
			// the label reaches the Scheduled event. Drop it rather than
			// strand an entry no completion will ever remove.
			delete(s.descriptions, name)
			s.mu.Unlock()
			return false, nil
		}
	}

	j, err := s.scheduler.NewJob(
		gocron.OneTimeJob(gocron.OneTimeJobStartImmediately()),
		gocron.NewTask(taskFn, args...),
		gocron.WithName(name),
		gocron.WithEventListeners(
			gocron.AfterJobRuns(func(jobID uuid.UUID, jobName string) {
				s.completeOneTimeJob(jobID, jobName, false, "")
			}),
			gocron.AfterJobRunsWithError(func(jobID uuid.UUID, jobName string, err error) {
				s.completeOneTimeJob(jobID, jobName, true, err.Error())
			}),
		),
	)
	if err != nil {
		s.mu.Unlock()
		return false, fmt.Errorf("create one-time job %s: %w", name, err)
	}

	s.jobs[name] = j
	s.schedules[name] = "once"
	jobID := j.ID().String()

	// Every one-time job gets a progress record, even when its task never
	// touches it. Two reasons: the job is otherwise invisible in the Jobs
	// inspector beyond a bare name, and cleanupCompletedLocked DELETES any
	// completed entry whose Progress is nil — so a RunOnce job used to vanish
	// entirely the moment anything called ListJobs, leaving no trace that it
	// ran, succeeded or failed (gastrolog-68dusi).
	//
	// If the caller supplied its own record (RunOnceWithProgress) it is already
	// in s.progress under this id; otherwise start a status-only one.
	if prog == nil {
		prog = &JobProgress{Status: JobStatusRunning, StartedAt: s.now()}
	}
	s.progress[jobID] = prog

	info := JobInfo{
		ID:          jobID,
		Name:        name,
		Description: s.descriptions[name],
		Schedule:    "once",
		Progress:    s.progress[jobID],
	}
	s.logger.Debug("one-time job scheduled", "name", name)
	s.mu.Unlock()

	// Publish outside the lock so slow subscribers can't stall scheduler
	// operations. Broker.Publish is non-blocking but still takes its own
	// RWMutex — keep the two lock domains disjoint.
	s.publishEvent(JobEventScheduled, info)
	return true, nil
}

// RunOnceWithProgress schedules a one-time job whose task reports its own
// progress. The fn receives a detached context and the job's JobProgress, and
// its error marks the job failed.
//
// Plain RunOnce jobs now get a status-only record automatically, so this is for
// work that has something to count — chunks pushed, records written, per-item
// error detail. Same registration semantics as RunOnce, including the
// last-writer-wins overwrite; use RunOnceIfAbsentWithProgress to claim a name.
func (s *Scheduler) RunOnceWithProgress(name string, fn func(context.Context, *JobProgress) error) error {
	_, err := s.runOnceProgress(name, nil, fn)
	return err
}

// RunOnceIfAbsentWithProgress is RunOnceWithProgress with RunOnceIfAbsent's
// claim: it registers only when the name is free, and reports whether it did.
func (s *Scheduler) RunOnceIfAbsentWithProgress(name string, fn func(context.Context, *JobProgress) error) (bool, error) {
	return s.runOnceProgress(name, &onceClaim{}, fn)
}

// runOnceProgress pre-registers a progress record, then schedules a task closed
// over it. The record has to exist before the job does, because runOnce records
// it in the Scheduled event's JobInfo and the task may start immediately.
func (s *Scheduler) runOnceProgress(name string, claim *onceClaim, fn func(context.Context, *JobProgress) error) (bool, error) {
	prog := &JobProgress{Status: JobStatusRunning, StartedAt: s.now()}
	task := func() error {
		ctx := context.WithoutCancel(context.Background())
		if err := fn(ctx, prog); err != nil {
			prog.Fail(s.now(), err.Error())
			return err
		}
		prog.Complete(s.now())
		return nil
	}
	return s.runOnceWith(name, claim, prog, task)
}

// Submit schedules a one-time job with progress tracking. Returns the gocron
// job ID. The fn receives a context (detached from the caller) and a
// JobProgress for reporting progress.
func (s *Scheduler) Submit(name string, fn func(context.Context, *JobProgress)) string {
	s.mu.Lock()

	prog := &JobProgress{
		Status:    JobStatusPending,
		StartedAt: s.now(),
	}

	wrapper := func() {
		prog.SetRunning(0)
		if notify := s.onJobChange; notify != nil {
			notify()
		}
		// Snapshot JobInfo for the Started event outside any scheduler lock
		// (we're already inside the gocron worker goroutine).
		s.mu.Lock()
		startInfo := JobInfo{
			ID:          "",
			Name:        name,
			Description: s.descriptions[name],
			Schedule:    "once",
			Progress:    prog,
		}
		if j := s.jobs[name]; j != nil {
			startInfo.ID = j.ID().String()
		}
		s.mu.Unlock()
		s.publishEvent(JobEventStarted, startInfo)

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
			gocron.AfterJobRuns(func(jobID uuid.UUID, jobName string) {
				s.completeOneTimeJob(jobID, jobName, false, "")
			}),
			gocron.AfterJobRunsWithError(func(jobID uuid.UUID, jobName string, err error) {
				s.completeOneTimeJob(jobID, jobName, true, err.Error())
			}),
		),
	)
	if err != nil {
		s.logger.Error("failed to schedule job", "name", name, "error", err)
		prog.Fail(s.now(), "failed to schedule: "+err.Error())
		// Generate an ID for the failed job so the caller can still look it up.
		failedID := glid.New().String()
		failedInfo := JobInfo{
			ID:          failedID,
			Name:        name,
			Description: s.descriptions[name],
			Schedule:    "once",
			Progress:    prog,
		}
		s.completed[failedID] = failedInfo
		s.mu.Unlock()
		s.publishEvent(JobEventFailed, failedInfo)
		return failedID
	}

	id := j.ID().String()
	s.jobs[name] = j
	s.schedules[name] = "once"
	s.progress[id] = prog
	scheduledInfo := JobInfo{
		ID:          id,
		Name:        name,
		Description: s.descriptions[name],
		Schedule:    "once",
		Progress:    prog,
	}
	s.logger.Info("job submitted", "name", name, "id", id)
	s.mu.Unlock()
	s.publishEvent(JobEventScheduled, scheduledInfo)
	return id
}

// completeOneTimeJob moves a finished one-time job from the active maps into
// the completed registry.
//
// Keyed by the job's OWN id, which gocron hands to the event listener. It used
// to discard that id and re-derive one by looking the NAME up in s.jobs — which
// is only the same job when no one has since registered another under that
// name. RunOnce overwrites s.jobs[name] without touching the job already
// running, so after an overwrite the first job's completion recorded the
// SECOND job's id, stamped it with a LastRun the second job had not reached,
// and deleted the registry entry while that job was still running. The second
// job's own completion then found nothing under the name and returned silently:
// no completion record, no onJobChange notification, and any progress it wrote
// afterwards leaked with nothing left to delete it. WaitIdle and
// HasPendingPrefix read s.jobs, so they reported idle mid-flight
// (gastrolog-1scomn).
//
// Both bodies still run — a duplicate schedule is the caller's bug, and this
// does not try to hide it. What it stops is the scheduler misreporting its own
// state when it happens.
func (s *Scheduler) completeOneTimeJob(id uuid.UUID, name string, failed bool, taskErr string) {
	s.mu.Lock()

	jobID := id.String()
	info := JobInfo{
		ID:          jobID,
		Name:        name,
		Description: s.descriptions[name],
		Schedule:    "once",
		Progress:    s.progress[jobID],
	}
	// Retire the name only when it still points at THIS job. If another job has
	// taken it, that job is still live and its registry entry is not ours to
	// delete.
	if j, ok := s.jobs[name]; ok && j.ID() == id {
		if lr, err := j.LastRun(); err == nil {
			info.LastRun = lr
		}
		delete(s.jobs, name)
		delete(s.schedules, name)
		delete(s.descriptions, name)
	}

	s.completed[jobID] = info
	delete(s.progress, jobID)
	notify := s.onJobChange
	s.mu.Unlock()

	// Stamp the terminal status. A task that reported its own outcome (Complete
	// or Fail) keeps it; one that never touched its record is completed here, so
	// the retained entry says something rather than sitting at "running" forever
	// (gastrolog-68dusi).
	if info.Progress != nil {
		info.Progress.mu.RLock()
		status := info.Progress.Status
		info.Progress.mu.RUnlock()
		if status != JobStatusCompleted && status != JobStatusFailed {
			if failed {
				info.Progress.Fail(s.now(), taskErr)
			} else {
				info.Progress.Complete(s.now())
			}
		}
	}

	// Classify the terminal event from the progress record, which every
	// one-time job now has. gocron calls a DIFFERENT listener on error, so the
	// task's failure reaches us as the `failed` argument rather than having to
	// be inferred (gastrolog-68dusi).
	kind := JobEventCompleted
	if info.Progress != nil {
		info.Progress.mu.RLock()
		if info.Progress.Status == JobStatusFailed {
			kind = JobEventFailed
		}
		info.Progress.mu.RUnlock()
	}
	// A failed one-time job used to write nothing to the log. The failure was
	// recorded on the progress record and published as an event, and the
	// success path logged "job finished" — so the only observable difference
	// between a job that worked and one that died was the absence of a line.
	// That is how a failing post-seal can strand a chunk in Sealing with no
	// trace anywhere (gastrolog-231ik: the stall was unfalsifiable across runs
	// precisely because this path is silent).
	//
	// Logged BEFORE the listeners are woken, so anything that reacts to the
	// terminal event — an operator tailing logs, a test waiting on the
	// notification — finds the explanation already written rather than racing
	// it.
	if kind == JobEventFailed {
		s.logger.Warn("one-time job failed", "name", name, "id", jobID, "error", taskErr)
	}

	if notify != nil {
		notify()
	}
	s.publishEvent(kind, info)
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
