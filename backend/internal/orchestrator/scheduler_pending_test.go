package orchestrator

// HasPendingPrefix and PendingOnce answer from s.jobs membership: a completed
// job is deleted from s.jobs, and s.completed is keyed by job ID, not name.
// These tests pin that both report a running job as pending and a completed
// one as gone — a dedup decision built on either must see exactly that.

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
	requireIdle(t, sched)

	if sched.HasPendingPrefix("transition:") {
		t.Error("a completed job must not report as pending")
	}
}

// PendingOnce counts a running one-time job until it completes, so a drain on
// it cannot return early.
func TestPendingOnceTracksOneTimeJobs(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	if n := sched.PendingOnce(); n != 0 {
		t.Fatalf("PendingOnce = %d with no jobs, want 0", n)
	}

	release := make(chan struct{})
	started := make(chan struct{})
	done := make(chan struct{})
	if err := sched.RunOnce("drain-me", func(context.Context) error {
		close(started)
		<-release
		close(done)
		return nil
	}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	<-started
	if n := sched.PendingOnce(); n != 1 {
		t.Errorf("PendingOnce = %d while the job runs, want 1", n)
	}

	close(release)
	requireIdle(t, sched)
	select {
	case <-done:
	default:
		t.Error("requireIdle returned while a one-time job was still running")
	}
	if n := sched.PendingOnce(); n != 0 {
		t.Errorf("PendingOnce = %d after the drain, want 0", n)
	}
}
