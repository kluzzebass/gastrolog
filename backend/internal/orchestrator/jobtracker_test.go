package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/logging"
)

func newTestScheduler(t *testing.T) *Scheduler {
	t.Helper()
	logger := logging.Discard()
	sched, err := newScheduler(logger, 4, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })
	return sched
}

func TestScheduler_SubmitAndGet(t *testing.T) {
	sched := newTestScheduler(t)

	done := make(chan struct{})
	id := sched.Submit("test-job", func(ctx context.Context, prog *JobProgress) {
		prog.SetRunning(5)
		prog.IncrChunks()
		prog.AddRecords(100)
		close(done)
	})

	if id == "" {
		t.Fatal("expected non-empty job ID")
	}

	<-done
	time.Sleep(50 * time.Millisecond)

	info, ok := sched.GetJob(id)
	if !ok {
		t.Fatal("expected to find job")
	}

	snap := info.Snapshot()
	if snap.Name != "test-job" {
		t.Errorf("name = %q, want %q", snap.Name, "test-job")
	}
	if snap.Progress.Status != JobStatusCompleted {
		t.Errorf("status = %d, want %d", snap.Progress.Status, JobStatusCompleted)
	}
	if snap.Progress.ChunksTotal != 5 {
		t.Errorf("chunks_total = %d, want 5", snap.Progress.ChunksTotal)
	}
	if snap.Progress.ChunksDone != 1 {
		t.Errorf("chunks_done = %d, want 1", snap.Progress.ChunksDone)
	}
	if snap.Progress.RecordsDone != 100 {
		t.Errorf("records_done = %d, want 100", snap.Progress.RecordsDone)
	}
}

func TestScheduler_ExplicitComplete(t *testing.T) {
	sched := newTestScheduler(t)

	done := make(chan struct{})
	id := sched.Submit("explicit-complete", func(ctx context.Context, prog *JobProgress) {
		prog.SetRunning(1)
		prog.IncrChunks()
		prog.Complete(time.Now())
		close(done)
	})

	<-done
	time.Sleep(50 * time.Millisecond)

	info, ok := sched.GetJob(id)
	if !ok {
		t.Fatal("job not found")
	}
	snap := info.Snapshot()
	if snap.Progress.Status != JobStatusCompleted {
		t.Errorf("status = %d, want %d", snap.Progress.Status, JobStatusCompleted)
	}
	if snap.Progress.CompletedAt.IsZero() {
		t.Error("expected non-zero completed_at")
	}
}

func TestScheduler_Fail(t *testing.T) {
	sched := newTestScheduler(t)

	done := make(chan struct{})
	id := sched.Submit("fail-job", func(ctx context.Context, prog *JobProgress) {
		prog.SetRunning(0)
		prog.AddErrorDetail("chunk abc: bad data")
		prog.Fail(time.Now(), "something broke")
		close(done)
	})

	<-done
	time.Sleep(50 * time.Millisecond)

	info, ok := sched.GetJob(id)
	if !ok {
		t.Fatal("job not found")
	}
	snap := info.Snapshot()
	if snap.Progress.Status != JobStatusFailed {
		t.Errorf("status = %d, want %d", snap.Progress.Status, JobStatusFailed)
	}
	if snap.Progress.Error != "something broke" {
		t.Errorf("error = %q, want %q", snap.Progress.Error, "something broke")
	}
	if len(snap.Progress.ErrorDetails) != 1 || snap.Progress.ErrorDetails[0] != "chunk abc: bad data" {
		t.Errorf("error_details = %v", snap.Progress.ErrorDetails)
	}
}

func TestScheduler_ListIncludesSubmitted(t *testing.T) {
	sched := newTestScheduler(t)

	var wg sync.WaitGroup
	wg.Add(2)

	sched.Submit("job-a", func(ctx context.Context, prog *JobProgress) {
		wg.Done()
	})
	sched.Submit("job-b", func(ctx context.Context, prog *JobProgress) {
		wg.Done()
	})

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	jobs := sched.ListJobs()
	names := map[string]bool{}
	for _, j := range jobs {
		names[j.Name] = true
	}
	if !names["job-a"] || !names["job-b"] {
		t.Errorf("expected job-a and job-b in list, got %v", names)
	}
}

func TestScheduler_GetNonexistent(t *testing.T) {
	sched := newTestScheduler(t)
	if _, ok := sched.GetJob("nonexistent"); ok {
		t.Error("expected not found")
	}
}

func TestScheduler_Cleanup(t *testing.T) {
	now := time.Now()
	mu := sync.Mutex{}

	logger := logging.Discard()
	sched, err := newScheduler(logger, 4, func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return now
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sched.Stop() })

	done := make(chan struct{})
	id := sched.Submit("old-job", func(ctx context.Context, prog *JobProgress) {
		close(done)
	})
	<-done
	time.Sleep(50 * time.Millisecond)

	if _, ok := sched.GetJob(id); !ok {
		t.Fatal("job should exist before cleanup")
	}

	// Advance time past the 1-hour cutoff.
	mu.Lock()
	now = now.Add(2 * time.Hour)
	mu.Unlock()

	// ListJobs triggers cleanup.
	jobs := sched.ListJobs()
	found := false
	for _, j := range jobs {
		if j.ID == id {
			found = true
		}
	}
	if found {
		t.Error("old job should have been cleaned up")
	}
}

func TestScheduler_ConcurrentProgress(t *testing.T) {
	sched := newTestScheduler(t)

	done := make(chan struct{})
	id := sched.Submit("concurrent-job", func(ctx context.Context, prog *JobProgress) {
		prog.SetRunning(100)

		var wg sync.WaitGroup
		for range 10 {
			wg.Go(func() {
				for range 10 {
					prog.IncrChunks()
					prog.AddRecords(5)
				}
			})
		}
		wg.Wait()
		close(done)
	})

	<-done
	time.Sleep(50 * time.Millisecond)

	info, ok := sched.GetJob(id)
	if !ok {
		t.Fatal("job not found")
	}
	snap := info.Snapshot()
	if snap.Progress.ChunksDone != 100 {
		t.Errorf("chunks_done = %d, want 100", snap.Progress.ChunksDone)
	}
	if snap.Progress.RecordsDone != 500 {
		t.Errorf("records_done = %d, want 500", snap.Progress.RecordsDone)
	}
}
