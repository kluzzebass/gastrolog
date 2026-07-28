package orchestrator

// RunOnce overwrites s.jobs[name] without touching a job already running under
// that name, so both bodies run. That is the caller's bug. What the scheduler
// must not do is misreport its own state when it happens — which it did, because
// the completion listener discarded the job id gocron gave it and re-derived one
// by looking the NAME up. See gastrolog-1scomn.

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// awaitNotifications blocks until the scheduler has fired n job-change events.
//
// The bound is a deadlock backstop, not the synchronisation: each event is
// waited FOR, so a loaded machine simply waits longer. The first version of
// these tests inverted that — it slept, or called the WaitIdle helper (which
// returns nothing, so a timeout there is indistinguishable from success) and
// then read a counter. Under full-suite load the wait expired, the assertion
// ran against a half-finished scheduler, and the test failed while passing 20/20
// in isolation.
func awaitNotifications(t *testing.T, ch <-chan struct{}, n int, what string) {
	t.Helper()
	for i := range n {
		select {
		case <-ch:
		case <-time.After(30 * time.Second):
			t.Fatalf("timed out waiting for %s: got %d of %d job-change events", what, i, n)
		}
	}
}

// notifyChan wires a buffered channel to the scheduler's job-change callback.
// Buffered because the callback runs on the scheduler's own goroutine and must
// never block on a test that is not yet receiving.
func notifyChan(sched *Scheduler) chan struct{} {
	ch := make(chan struct{}, 64)
	sched.SetOnJobChange(func() {
		select {
		case ch <- struct{}{}:
		default:
		}
	})
	return ch
}

func TestOverwrittenOneTimeJobsBothComplete(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	notified := notifyChan(sched)

	const name = "overwritten"
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})

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
		return nil
	}); err != nil {
		t.Fatalf("RunOnce (second): %v", err)
	}

	close(releaseFirst)

	// Two distinct jobs ran, so two terminal events must fire. Before the fix
	// the first job's completion was filed under the SECOND job's id, and the
	// second job's own completion found nothing under the name and returned
	// silently — one event instead of two, so this wait would never satisfy.
	//
	// Counted through onJobChange rather than ListJobs: a RunOnce job carries no
	// progress record, and cleanupCompletedLocked drops progress-less entries on
	// the next ListJobs call, so the completion registry is deliberately
	// ephemeral for them.
	awaitNotifications(t, notified, 2, "both one-time jobs to report completion")
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

	notified := notifyChan(sched)

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

	// Retire the first job and wait for the scheduler to have actually processed
	// that completion — the state this asserts on only exists afterwards. Sleeping
	// a fixed 100ms here was the same mistake as above: on a loaded machine the
	// retirement had not happened yet, so the assertion tested nothing.
	close(releaseFirst)
	awaitNotifications(t, notified, 1, "the first job's retirement")

	if !sched.HasPendingPrefix(name) {
		t.Error("scheduler reported idle while a job under that name was still running")
	}

	// completeOneTimeJob drops the registry entry under the lock and fires the
	// notification after releasing it, so once the event lands the name is
	// already retired — waiting on it is sound where WaitIdle's silent timeout
	// was not.
	close(releaseSecond)
	awaitNotifications(t, notified, 1, "the second job's completion")
	if sched.HasPendingPrefix(name) {
		t.Error("still pending after both jobs finished")
	}
}
