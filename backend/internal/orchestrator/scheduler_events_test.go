package orchestrator

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func newQuietScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s, err := newScheduler(slog.New(slog.NewTextHandler(io.Discard, nil)), 4, time.Now)
	if err != nil {
		t.Fatalf("newScheduler: %v", err)
	}
	return s
}

func collectEvents(t *testing.T, sub *JobSubscription, want int, timeout time.Duration) []JobEvent {
	t.Helper()
	var got []JobEvent
	deadline := time.After(timeout)
	for len(got) < want {
		select {
		case evt, ok := <-sub.Events():
			if !ok {
				return got
			}
			got = append(got, evt)
		case <-deadline:
			t.Fatalf("timed out after %v waiting for %d events; got %d: %+v", timeout, want, len(got), got)
		}
	}
	return got
}

// TestScheduler_Events_RunOnce verifies that RunOnce emits Scheduled then
// Completed for a successful job.
func TestScheduler_Events_RunOnce(t *testing.T) {
	s := newQuietScheduler(t)
	sub, cancel := s.Events().Subscribe()
	defer cancel()

	done := make(chan struct{})
	if err := s.RunOnce("happy", func() { close(done) }); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	<-done

	evts := collectEvents(t, sub, 2, 2*time.Second)
	if evts[0].Kind != JobEventScheduled {
		t.Errorf("event[0] kind=%v, want Scheduled", evts[0].Kind)
	}
	if evts[1].Kind != JobEventCompleted {
		t.Errorf("event[1] kind=%v, want Completed", evts[1].Kind)
	}
	for _, e := range evts {
		if e.Job.Name != "happy" {
			t.Errorf("event %v has name %q, want 'happy'", e.Kind, e.Job.Name)
		}
	}
}

// TestScheduler_Events_Submit verifies the Submit lifecycle: Scheduled →
// Started → Completed.
func TestScheduler_Events_Submit(t *testing.T) {
	s := newQuietScheduler(t)
	sub, cancel := s.Events().Subscribe()
	defer cancel()

	start := make(chan struct{})
	s.Submit("work", func(_ context.Context, p *JobProgress) {
		close(start)
		p.Complete(time.Now())
	})
	<-start

	evts := collectEvents(t, sub, 3, 2*time.Second)
	kinds := []JobEventKind{evts[0].Kind, evts[1].Kind, evts[2].Kind}
	want := []JobEventKind{JobEventScheduled, JobEventStarted, JobEventCompleted}
	for i, k := range want {
		if kinds[i] != k {
			t.Errorf("event[%d] kind=%v, want %v (all kinds: %v)", i, kinds[i], k, kinds)
		}
	}
}

// TestScheduler_Events_SubmitFailure verifies that Submit emits
// JobEventFailed when the progress record was marked failed.
func TestScheduler_Events_SubmitFailure(t *testing.T) {
	s := newQuietScheduler(t)
	sub, cancel := s.Events().Subscribe()
	defer cancel()

	s.Submit("bad", func(_ context.Context, p *JobProgress) {
		p.Fail(time.Now(), "simulated")
	})

	evts := collectEvents(t, sub, 3, 2*time.Second)
	if evts[len(evts)-1].Kind != JobEventFailed {
		t.Errorf("last event kind=%v, want Failed (all: %v)", evts[len(evts)-1].Kind, evts)
	}
}

// TestScheduler_Events_MultipleSubscribers verifies two subscribers both
// receive the same sequence.
func TestScheduler_Events_MultipleSubscribers(t *testing.T) {
	s := newQuietScheduler(t)
	subA, cancelA := s.Events().Subscribe()
	defer cancelA()
	subB, cancelB := s.Events().Subscribe()
	defer cancelB()

	done := make(chan struct{})
	if err := s.RunOnce("shared", func() { close(done) }); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	<-done

	gotA := collectEvents(t, subA, 2, 2*time.Second)
	gotB := collectEvents(t, subB, 2, 2*time.Second)
	if len(gotA) != 2 || len(gotB) != 2 {
		t.Errorf("counts: A=%d B=%d, want 2 each", len(gotA), len(gotB))
	}
}

// TestScheduler_Events_OnJobChange_StillFires verifies the legacy
// SetOnJobChange callback continues to work alongside the broker — the
// broker is additive, not a replacement in this change.
func TestScheduler_Events_OnJobChange_StillFires(t *testing.T) {
	s := newQuietScheduler(t)
	changed := make(chan struct{}, 4)
	s.SetOnJobChange(func() { changed <- struct{}{} })

	s.Submit("cb", func(_ context.Context, p *JobProgress) {
		p.Complete(time.Now())
	})

	// Submit → Running (SetRunning fires onJobChange) → completion fires again.
	seen := 0
	timeout := time.After(2 * time.Second)
	for seen < 2 {
		select {
		case <-changed:
			seen++
		case <-timeout:
			t.Fatalf("onJobChange fired only %d time(s), want >= 2", seen)
		}
	}
}

// TestScheduler_Events_RunOnceFailureStillCompletes confirms that RunOnce
// jobs whose task panics/errors still emit JobEventCompleted (there's no
// JobProgress to carry failure state; gocron's AfterJobRunsWithError path
// routes through the same completeOneTimeJob, which publishes Completed).
func TestScheduler_Events_RunOnceFailureStillCompletes(t *testing.T) {
	s := newQuietScheduler(t)
	sub, cancel := s.Events().Subscribe()
	defer cancel()

	if err := s.RunOnce("err", func() error {
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	evts := collectEvents(t, sub, 2, 2*time.Second)
	if evts[1].Kind != JobEventCompleted {
		t.Errorf("RunOnce returning an error still emits Completed for the broker; got %v", evts[1].Kind)
	}
}
