package orchestrator

// Only Submit-registered jobs used to carry a JobProgress. That had a
// consequence nobody would guess from the API: cleanupCompletedLocked deletes
// any completed entry whose Progress is nil, on the next ListJobs call — so a
// RunOnce job appeared while running and then VANISHED, leaving no trace it had
// run, succeeded or failed. Post-seal, GLCB build, replication, cloud upload and
// backfill are all RunOnce.

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func jobByName(t *testing.T, s *Scheduler, name string) (JobInfo, bool) {
	t.Helper()
	for _, info := range s.ListJobs() {
		if info.Name == name {
			return info, true
		}
	}
	return JobInfo{}, false
}

func TestRunOnceLeavesACompletionRecord(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	if err := sched.RunOnce("post-seal:chunk-1", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	requireIdle(t, sched, 5*time.Second)

	// ListJobs runs cleanupCompletedLocked, which is what used to delete this.
	info, ok := jobByName(t, sched, "post-seal:chunk-1")
	if !ok {
		t.Fatal("completed RunOnce job left no record: the operator has no way to see that it ran")
	}
	if info.Progress == nil {
		t.Fatal("completed RunOnce job carries no progress record")
	}
	if info.Progress.Status != JobStatusCompleted {
		t.Errorf("status = %v, want completed", info.Progress.Status)
	}
	if info.Progress.CompletedAt.IsZero() {
		t.Error("completed job has no completion time")
	}
}

func TestRunOnceFailureIsVisible(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	if err := sched.RunOnce("build:chunk-2", func(context.Context) error {
		return errors.New("disk full")
	}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	requireIdle(t, sched, 5*time.Second)

	info, ok := jobByName(t, sched, "build:chunk-2")
	if !ok {
		t.Fatal("failed RunOnce job left no record")
	}
	if info.Progress == nil || info.Progress.Status != JobStatusFailed {
		t.Fatalf("progress = %+v, want failed", info.Progress)
	}
	if info.Progress.Error == "" {
		t.Error("failed job records no error text: the operator sees a failure with no reason")
	}
}

// The opt-in variant: work with something to count can report it.
func TestRunOnceWithProgressReportsDetail(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	if err := sched.RunOnceWithProgress("replicate:vault-a", func(_ context.Context, p *JobProgress) error {
		p.SetRunning(3)
		for range 3 {
			p.IncrChunks()
			p.AddRecords(10)
		}
		return nil
	}); err != nil {
		t.Fatalf("RunOnceWithProgress: %v", err)
	}
	requireIdle(t, sched, 5*time.Second)

	info, ok := jobByName(t, sched, "replicate:vault-a")
	if !ok {
		t.Fatal("job left no record")
	}
	p := info.Progress
	if p == nil {
		t.Fatal("no progress record")
	}
	if p.ChunksTotal != 3 || p.ChunksDone != 3 || p.RecordsDone != 30 {
		t.Errorf("progress = {total:%d done:%d records:%d}, want {3 3 30}",
			p.ChunksTotal, p.ChunksDone, p.RecordsDone)
	}
	if p.Status != JobStatusCompleted {
		t.Errorf("status = %v, want completed", p.Status)
	}
}

// A task that reports its own failure keeps that outcome and its message,
// rather than being overwritten by the generic completion stamp.
func TestRunOnceWithProgressKeepsItsOwnFailure(t *testing.T) {
	t.Parallel()
	sched, err := newScheduler(slog.Default(), 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	if err := sched.RunOnceWithProgress("upload:chunk-3", func(_ context.Context, p *JobProgress) error {
		p.SetRunning(2)
		p.IncrChunks()
		p.AddErrorDetail("chunk 2: connection reset")
		return errors.New("cloud store unreachable")
	}); err != nil {
		t.Fatalf("RunOnceWithProgress: %v", err)
	}
	requireIdle(t, sched, 5*time.Second)

	info, ok := jobByName(t, sched, "upload:chunk-3")
	if !ok {
		t.Fatal("job left no record")
	}
	p := info.Progress
	if p.Status != JobStatusFailed || p.Error != "cloud store unreachable" {
		t.Errorf("progress = {status:%v error:%q}, want failed with the task's error", p.Status, p.Error)
	}
	// Partial work done before the failure must still be visible.
	if p.ChunksDone != 1 {
		t.Errorf("chunks done = %d, want 1: work completed before the failure is still work done", p.ChunksDone)
	}
	if len(p.ErrorDetails) != 1 {
		t.Errorf("error details = %v, want the one the task added", p.ErrorDetails)
	}
}
