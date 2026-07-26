package orchestrator

import (
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
