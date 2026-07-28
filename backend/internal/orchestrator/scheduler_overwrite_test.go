package orchestrator

// RunOnce overwrites s.jobs[name] without touching a job already running under
// that name, so both bodies run. That is the caller's bug. What the scheduler
// must not do is misreport its own state when it happens — which it did, because
// the completion listener discarded the job id gocron gave it and re-derived one
// by looking the NAME up. See gastrolog-1scomn.

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestOverwrittenOneTimeJobsBothComplete(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	var notifications atomic.Int32
	sched.SetOnJobChange(func() { notifications.Add(1) })

	const name = "overwritten"
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondDone := make(chan struct{})

	if err := sched.RunOnce(name, func(context.Context) error {
		close(firstStarted)
		<-releaseFirst
		return nil
	}); err != nil {
		t.Fatalf("RunOnce (first): %v", err)
	}
	<-firstStarted

	// Second registration under the same name, while the first still runs.
	if err := sched.RunOnce(name, func(context.Context) error {
		<-releaseFirst
		time.Sleep(30 * time.Millisecond)
		close(secondDone)
		return nil
	}); err != nil {
		t.Fatalf("RunOnce (second): %v", err)
	}

	close(releaseFirst)
	<-secondDone
	sched.WaitIdle(5 * time.Second)

	// Two distinct jobs ran, so two terminal events must fire. Before the fix
	// the first job's completion was filed under the SECOND job's id, and the
	// second job's own completion found nothing under the name and returned
	// silently — one event instead of two.
	//
	// Counted through onJobChange rather than ListJobs: a RunOnce job carries no
	// progress record, and cleanupCompletedLocked drops progress-less entries on
	// the next ListJobs call, so the completion registry is deliberately
	// ephemeral for them.
	if got := notifications.Load(); got < 2 {
		t.Errorf("terminal notifications = %d, want at least 2 (one per job that ran)", got)
	}
}

// The registry entry must not be retired by a job that no longer owns the name:
// doing so made WaitIdle and HasPendingPrefix report idle while a job was still
// running, which is what the test-drain primitives rely on being false.
func TestOverwrittenJobDoesNotRetireTheLiveEntry(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	const name = "still-running"
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	secondStarted := make(chan struct{})

	if err := sched.RunOnce(name, func(context.Context) error {
		close(firstStarted)
		<-releaseFirst
		return nil
	}); err != nil {
		t.Fatalf("RunOnce (first): %v", err)
	}
	<-firstStarted

	if err := sched.RunOnce(name, func(context.Context) error {
		close(secondStarted)
		<-releaseSecond
		return nil
	}); err != nil {
		t.Fatalf("RunOnce (second): %v", err)
	}
	<-secondStarted

	// Retire the first job. The second is still running and still owns the name.
	close(releaseFirst)
	time.Sleep(100 * time.Millisecond)

	if !sched.HasPendingPrefix(name) {
		t.Error("scheduler reported idle while a job under that name was still running")
	}

	close(releaseSecond)
	sched.WaitIdle(5 * time.Second)
	if sched.HasPendingPrefix(name) {
		t.Error("still pending after both jobs finished")
	}
}
