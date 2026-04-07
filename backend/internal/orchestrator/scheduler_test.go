package orchestrator

import (
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

// TestCronJobSingletonMode verifies that cron jobs registered via AddJob
// run with singleton mode — a second tick is rescheduled (not overlapped)
// while the previous invocation is still running.
func TestCronJobSingletonMode(t *testing.T) {
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
