package orchestrator

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCronJobSingletonMode verifies that cron jobs registered via AddJob
// run with singleton mode — a second tick is rescheduled (not overlapped)
// while the previous invocation is still running.
func TestCronJobSingletonMode(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second cron-tick test")
	}
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32
	var invocations atomic.Int32

	// Job takes 1.5s — longer than the 1s cron interval.
	// Without singleton mode, tick 2 would start while tick 1 is running.
	// With LimitModeReschedule, tick 2 is dropped entirely.
	if err := sched.AddJob("test-singleton", "* * * * * *", func() {
		invocations.Add(1)
		n := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if n <= old || maxConcurrent.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(1500 * time.Millisecond)
		concurrent.Add(-1)
	}); err != nil {
		t.Fatal(err)
	}

	// Wait for 4 seconds — enough for 4 ticks.
	// With 1.5s per invocation and reschedule mode, we expect ~2 invocations
	// (tick 1 runs, tick 2 dropped, tick 3 runs, tick 4 dropped).
	time.Sleep(4 * time.Second)

	if got := maxConcurrent.Load(); got > 1 {
		t.Errorf("max concurrent invocations = %d; singleton mode should prevent overlap", got)
	}
	// With reschedule mode, missed ticks are dropped, not queued.
	// 4 seconds / 1.5s per run ≈ 2-3 invocations (not 4).
	if got := invocations.Load(); got > 3 {
		t.Errorf("invocations = %d; reschedule mode should drop missed ticks", got)
	}
}

// TestCronJobSingletonModePreservedAcrossRebuild verifies that singleton mode
// is maintained when the scheduler is rebuilt (e.g., concurrency limit change).
func TestCronJobSingletonModePreservedAcrossRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip("multi-second cron-tick test")
	}
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	var concurrent atomic.Int32
	var maxConcurrent atomic.Int32

	if err := sched.AddJob("rebuild-singleton", "* * * * * *", func() {
		n := concurrent.Add(1)
		for {
			old := maxConcurrent.Load()
			if n <= old || maxConcurrent.CompareAndSwap(old, n) {
				break
			}
		}
		time.Sleep(1500 * time.Millisecond)
		concurrent.Add(-1)
	}); err != nil {
		t.Fatal(err)
	}

	// Rebuild with a different concurrency limit.
	if err := sched.Rebuild(8); err != nil {
		t.Fatal(err)
	}

	time.Sleep(4 * time.Second)

	if got := maxConcurrent.Load(); got > 1 {
		t.Errorf("max concurrent after rebuild = %d; singleton mode should survive rebuild", got)
	}
}

// TestRunOnceIfAbsentClaimsExactlyOnce pins the primitive that replaced the
// check-then-act dedup guard in the cloud-upload paths (gastrolog-3hwngy):
// with N goroutines racing on one job name, exactly one wins the claim and
// exactly one task body runs. The winning task blocks on a gate for the whole
// stampede, so a non-atomic guard would let losers through.
func TestRunOnceIfAbsentClaimsExactlyOnce(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 8, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	entered := make(chan struct{}, 32)
	release := make(chan struct{})
	var ran atomic.Int32
	var claimed atomic.Int32

	const racers = 16
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := sched.RunOnceIfAbsent("claim-me", func() {
				ran.Add(1)
				entered <- struct{}{}
				<-release
			})
			if err != nil {
				t.Errorf("RunOnceIfAbsent: %v", err)
				return
			}
			if ok {
				claimed.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	// The winner is inside the task and holds the claim until release.
	<-entered
	if got := claimed.Load(); got != 1 {
		t.Errorf("%d racers claimed the name %d times, want 1", racers, got)
	}
	if !sched.HasJob("claim-me") {
		t.Error("the claimed job should still be registered while it runs")
	}

	close(release)
	if got := ran.Load(); got != 1 {
		t.Errorf("task body ran %d times, want 1", got)
	}
}

// TestRunOnceIfAbsentReclaimsAfterCompletion verifies the claim is a lease on
// outstanding work, not a permanent lock: once the job finishes and leaves the
// registry, the same name can be claimed again (that is what lets a failed
// cloud upload be retried by a later sweep).
func TestRunOnceIfAbsentReclaimsAfterCompletion(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	sub, cancel := sched.Events().Subscribe()
	defer cancel()

	var ran atomic.Int32
	ok, err := sched.RunOnceIfAbsent("retryable", func() { ran.Add(1) })
	if err != nil || !ok {
		t.Fatalf("first claim: scheduled=%v err=%v", ok, err)
	}
	awaitSchedulerJobDone(t, sub, "retryable")

	ok, err = sched.RunOnceIfAbsent("retryable", func() { ran.Add(1) })
	if err != nil || !ok {
		t.Fatalf("second claim after completion: scheduled=%v err=%v", ok, err)
	}
	awaitSchedulerJobDone(t, sub, "retryable")

	if got := ran.Load(); got != 2 {
		t.Errorf("task ran %d times across two claims, want 2", got)
	}
}

// awaitSchedulerJobDone blocks until the scheduler publishes a terminal event
// for name — the scheduler's own completion signal, not a sleep.
func awaitSchedulerJobDone(t *testing.T, sub *JobSubscription, name string) {
	t.Helper()
	for {
		select {
		case evt, ok := <-sub.Events():
			if !ok {
				t.Fatalf("job event stream closed before %s completed", name)
			}
			if evt.Job.Name == name &&
				(evt.Kind == JobEventCompleted || evt.Kind == JobEventFailed) {
				return
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for %s to complete", name)
		}
	}
}

// awaitSchedulerJobsDone blocks until every named job has published a terminal
// event. It tracks the whole set at once because the events interleave in
// completion order, and a per-name wait would consume — and discard — the
// events it is not currently looking for.
func awaitSchedulerJobsDone(t *testing.T, sub *JobSubscription, names ...string) {
	t.Helper()
	pending := make(map[string]bool, len(names))
	for _, n := range names {
		pending[n] = true
	}
	for len(pending) > 0 {
		select {
		case evt, ok := <-sub.Events():
			if !ok {
				t.Fatalf("job event stream closed with %d jobs outstanding", len(pending))
			}
			if evt.Kind == JobEventCompleted || evt.Kind == JobEventFailed {
				delete(pending, evt.Job.Name)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out with %d jobs outstanding", len(pending))
		}
	}
}

// hasDescription reports whether the scheduler still holds a description entry
// for name. completeOneTimeJob deletes the entry when a one-time job finishes,
// so a lingering entry after completion is a leak.
func hasDescription(s *Scheduler, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.descriptions[name]
	return ok
}

// awaitSchedulerJobScheduled returns the Scheduled event for name — the event
// that carries the label to the operator inspector.
func awaitSchedulerJobScheduled(t *testing.T, sub *JobSubscription, name string) JobInfo {
	t.Helper()
	for {
		select {
		case evt, ok := <-sub.Events():
			if !ok {
				t.Fatalf("job event stream closed before %s was scheduled", name)
			}
			if evt.Job.Name == name && evt.Kind == JobEventScheduled {
				return evt.Job
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for %s to be scheduled", name)
		}
	}
}

// TestDescribeBeforeRunOnceLabelsEventAndReleases pins the ordering contract
// every one-time job site must follow: Describe FIRST, then schedule. The
// description is snapshotted into the Scheduled event's JobInfo at
// registration time, and completeOneTimeJob deletes the entry when the job
// finishes — so describing first is what puts the label on the event AND what
// lets the entry be released. See gastrolog-69sjlj.
func TestDescribeBeforeRunOnceLabelsEventAndReleases(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	sub, cancel := sched.Events().Subscribe()
	defer cancel()

	sched.Describe("described-first", "a label the inspector can show")
	if err := sched.RunOnce("described-first", func() {}); err != nil {
		t.Fatal(err)
	}

	info := awaitSchedulerJobScheduled(t, sub, "described-first")
	if info.Description != "a label the inspector can show" {
		t.Errorf("Scheduled event description = %q, want the label set before scheduling", info.Description)
	}

	awaitSchedulerJobDone(t, sub, "described-first")
	if hasDescription(sched, "described-first") {
		t.Error("description entry survived job completion — it should have been released")
	}
}

// TestDescribeAfterRunOnceLosesLabelAndLeaks is the control for the ordering
// above: it pins the defect the call sites had, so the reason for the rule is
// executable rather than folklore. Describing after scheduling puts no label
// on the Scheduled event, and when the job finishes first the deletion runs
// before the description is added — leaving an entry with no job, one per
// invocation, for the process lifetime.
func TestDescribeAfterRunOnceLosesLabelAndLeaks(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	sub, cancel := sched.Events().Subscribe()
	defer cancel()

	if err := sched.RunOnce("described-late", func() {}); err != nil {
		t.Fatal(err)
	}
	info := awaitSchedulerJobScheduled(t, sub, "described-late")
	if info.Description != "" {
		t.Errorf("Scheduled event description = %q; describing after scheduling cannot label the event", info.Description)
	}

	// Join on the job's own completion, then describe — the ordering that
	// leaks. No sleep: the terminal event IS the "job finished first" state.
	awaitSchedulerJobDone(t, sub, "described-late")
	sched.Describe("described-late", "too late to matter")

	if !hasDescription(sched, "described-late") {
		t.Fatal("expected the late description to strand — if this now releases, the leak is fixed in the scheduler and the call-site ordering rule can be revisited")
	}
}

// blockingClaims registers n gated jobs under prefix and returns the release
// func. Each job is provably inside its body when this returns, so the caller
// is testing the claim state, not racing the scheduler.
func blockingClaims(t *testing.T, sched *Scheduler, prefix string, limit, n int) (func(), *atomic.Int32) {
	t.Helper()
	entered := make(chan struct{}, n)
	release := make(chan struct{})
	var ran atomic.Int32
	for i := range n {
		ok, err := sched.RunOnceIfAbsentUnderLimit(
			fmt.Sprintf("%s%d", prefix, i), prefix, limit, func() {
				ran.Add(1)
				entered <- struct{}{}
				<-release
			})
		if err != nil || !ok {
			t.Fatalf("claim %d: scheduled=%v err=%v", i, ok, err)
		}
	}
	for range n {
		<-entered
	}
	var once sync.Once
	return func() { once.Do(func() { close(release) }) }, &ran
}

// TestRunOnceIfAbsentUnderLimitHoldsTheBudget pins the budget half of the
// claim: with the limit's worth of jobs outstanding, further distinct names
// are declined until one finishes. This is the property that used to live in
// the orchestrator's own glcbPullInflight map (maxConcurrentGLCBPulls), moved
// onto the scheduler so "what is outstanding" has one owner. See
// gastrolog-69sjlj.
func TestRunOnceIfAbsentUnderLimitHoldsTheBudget(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 8, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	const limit = 4
	release, ran := blockingClaims(t, sched, "budgeted:", limit, limit)
	defer release()

	// Budget is full: a fresh name is declined, not queued.
	ok, err := sched.RunOnceIfAbsentUnderLimit("budgeted:over", "budgeted:", limit, func() {
		t.Error("a job past the budget must not run")
	})
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("claim succeeded past the in-flight budget")
	}
	if sched.HasJob("budgeted:over") {
		t.Error("declined claim left a job registered")
	}
	// Jobs under a different prefix share no budget with these.
	ok, err = sched.RunOnceIfAbsentUnderLimit("other:0", "other:", limit, func() {})
	if err != nil || !ok {
		t.Fatalf("unrelated prefix declined: scheduled=%v err=%v", ok, err)
	}

	sub, cancel := sched.Events().Subscribe()
	defer cancel()
	release()
	names := make([]string, 0, limit)
	for i := range limit {
		names = append(names, fmt.Sprintf("budgeted:%d", i))
	}
	awaitSchedulerJobsDone(t, sub, names...)
	if got := ran.Load(); got != limit {
		t.Errorf("ran %d budgeted jobs, want %d", got, limit)
	}

	// The budget is a lease, not a quota: it frees as jobs complete.
	ok, err = sched.RunOnceIfAbsentUnderLimit("budgeted:over", "budgeted:", limit, func() {})
	if err != nil || !ok {
		t.Fatalf("claim after the budget drained: scheduled=%v err=%v", ok, err)
	}
}

// TestRunOnceIfAbsentUnderLimitConcurrentClaimsRespectBudget is the
// stampede case: the GLCB catch-up sweep walks a whole manifest, so many
// distinct chunk names hit the budget at once. A count-then-register guard
// would overshoot; the count and the registration happen under one lock hold.
func TestRunOnceIfAbsentUnderLimitConcurrentClaimsRespectBudget(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 16, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	const (
		limit  = 4
		racers = 24
		prefix = "stampede:"
	)
	release := make(chan struct{})
	defer close(release)
	var ran, claimed atomic.Int32

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			ok, err := sched.RunOnceIfAbsentUnderLimit(
				fmt.Sprintf("%s%d", prefix, i), prefix, limit, func() {
					ran.Add(1)
					<-release
				})
			if err != nil {
				t.Errorf("claim %d: %v", i, err)
				return
			}
			if ok {
				claimed.Add(1)
			}
		}(i)
	}
	close(start)
	wg.Wait()

	if got := claimed.Load(); got != limit {
		t.Errorf("%d racers claimed %d slots, want exactly the budget %d", racers, got, limit)
	}
	if got := countJobsByPrefix(sched, prefix); got != limit {
		t.Errorf("jobs registered under %q = %d, want the budget %d", prefix, got, limit)
	}
	if got := ran.Load(); got > limit {
		t.Errorf("%d job bodies started, want at most the budget %d", got, limit)
	}
}

// TestRunOnceIfAbsentUnderLimitCancelReleasesClaim is the regression the
// migration off glcbPullInflight exists for. Cancelling a pending job —
// RemoveJobsByPrefix, as vault teardown does — must release the right to redo
// that work. With the claim held in a separate map, the cancelled job's entry
// stranded and that chunk would never be pulled again.
func TestRunOnceIfAbsentUnderLimitCancelReleasesClaim(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 8, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	const limit = 2
	release, _ := blockingClaims(t, sched, "cancelled:", limit, limit)
	defer release()

	if ok, err := sched.RunOnceIfAbsentUnderLimit("cancelled:0", "cancelled:", limit, func() {}); err != nil || ok {
		t.Fatalf("re-claim while outstanding: scheduled=%v err=%v, want declined", ok, err)
	}

	sched.RemoveJobsByPrefix("cancelled:")

	if got := countJobsByPrefix(sched, "cancelled:"); got != 0 {
		t.Fatalf("jobs still registered after cancellation = %d", got)
	}
	ok, err := sched.RunOnceIfAbsentUnderLimit("cancelled:0", "cancelled:", limit, func() {})
	if err != nil || !ok {
		t.Fatalf("re-claim after cancellation: scheduled=%v err=%v, want granted — a cancelled job must not strand its claim", ok, err)
	}
}

// TestRunOnceIfAbsentUnderLimitDeclineReleasesDescription closes the leak the
// budget path would otherwise open. Callers describe before scheduling (so
// the label reaches the Scheduled event), so a claim declined by the budget
// leaves a description under a name with no job and no completion to remove
// it — one per chunk the sweep ever deferred.
func TestRunOnceIfAbsentUnderLimitDeclineReleasesDescription(t *testing.T) {
	t.Parallel()

	sched, err := newScheduler(slog.Default(), 8, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sched.Stop() }()

	const limit = 2
	release, _ := blockingClaims(t, sched, "deferred:", limit, limit)
	defer release()

	sched.Describe("deferred:over", "a pull that will be deferred")
	ok, err := sched.RunOnceIfAbsentUnderLimit("deferred:over", "deferred:", limit, func() {})
	if err != nil || ok {
		t.Fatalf("claim past the budget: scheduled=%v err=%v, want declined", ok, err)
	}
	if hasDescription(sched, "deferred:over") {
		t.Error("a budget-declined claim stranded its description — no job exists to release it")
	}

	// A claim declined because the NAME is taken must leave the live job's
	// label alone — that description belongs to the running job.
	sched.Describe("deferred:0", "the running pull's label")
	if ok, err := sched.RunOnceIfAbsentUnderLimit("deferred:0", "deferred:", limit, func() {}); err != nil || ok {
		t.Fatalf("re-claim of a held name: scheduled=%v err=%v, want declined", ok, err)
	}
	if !hasDescription(sched, "deferred:0") {
		t.Error("declining a held name dropped the running job's description")
	}
}

// countJobsByPrefix counts registered jobs under a name prefix.
func countJobsByPrefix(s *Scheduler, prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.countByPrefixLocked(prefix)
}
