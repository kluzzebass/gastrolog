package orchestrator

// HasPendingPrefix used to test s.completed[name] as well as s.jobs membership.
// s.completed is keyed by job ID, so that lookup could never match and the
// predicate quietly degraded to the membership test. The two agree — the
// completion path deletes from s.jobs — so nothing was broken, but a helper
// whose body claims a check it does not perform is how the next reader mistakes
// it for a dedup guard, which is exactly the shape that produced duplicate S3
// PUTs in gastrolog-3hwngy. These tests pin the behaviour the name promises
// (gastrolog-1scomn).

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestHasPendingPrefixTracksJobLifecycle(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	if sched.HasPendingPrefix("transition:") {
		t.Fatal("no jobs registered, yet a prefix reports pending")
	}

	release := make(chan struct{})
	started := make(chan struct{})
	if err := sched.RunOnce("transition:chunk-1", func(context.Context) error {
		close(started)
		<-release
		return nil
	}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	<-started
	if !sched.HasPendingPrefix("transition:") {
		t.Error("a running job must report as pending")
	}
	if sched.HasPendingPrefix("other:") {
		t.Error("an unrelated prefix must not report as pending")
	}

	close(release)
	requireIdle(t, sched, 5*time.Second)

	// The point of the fix: once the job has completed it is gone from s.jobs,
	// so this must go false. It did before too — via absence rather than via
	// the completed[] test the body appeared to be making.
	if sched.HasPendingPrefix("transition:") {
		t.Error("a completed job must not report as pending")
	}
}

// WaitIdle carried the identical dead lookup. It must still drain one-time
// jobs, and must not be fooled into returning early while one is running.
func TestWaitIdleDrainsOneTimeJobs(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	done := make(chan struct{})
	if err := sched.RunOnce("drain-me", func(context.Context) error {
		time.Sleep(50 * time.Millisecond)
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	requireIdle(t, sched, 5*time.Second)
	select {
	case <-done:
	default:
		t.Error("WaitIdle returned while a one-time job was still running")
	}
	if sched.HasPendingPrefix("drain-me") {
		t.Error("job still pending after WaitIdle")
	}
}
