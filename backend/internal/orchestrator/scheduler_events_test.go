package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/waittest"
)

func newQuietScheduler(t *testing.T) *Scheduler {
	t.Helper()
	s, err := newScheduler(slog.New(slog.NewTextHandler(io.Discard, nil)), 4, time.Now)
	if err != nil {
		t.Fatalf("newScheduler: %v", err)
	}
	return s
}

func collectEvents(t *testing.T, sub *JobSubscription, want int) []JobEvent {
	t.Helper()
	var got []JobEvent
	for len(got) < want {
		evt := waittest.Recv(t, fmt.Sprintf("job event %d of %d", len(got)+1, want), sub.Events(), nil)
		if evt.Kind == 0 {
			return got
		}
		got = append(got, evt)
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

	evts := collectEvents(t, sub, 2)
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

	evts := collectEvents(t, sub, 3)
	kinds := []JobEventKind{evts[0].Kind, evts[1].Kind, evts[2].Kind}
	want := []JobEventKind{JobEventScheduled, JobEventStarted, JobEventCompleted}
	for i, k := range want {
		if kinds[i] != k {
			t.Errorf("event[%d] kind=%v, want %v (all kinds: %v)", i, kinds[i], k, kinds)
		}
	}
}

// TestScheduler_Events_SubmitFailure verifies a job that fails the instant
// it runs still presents its lifecycle in order: Scheduled, Started, Failed.
// gocron starts a one-time job immediately, so pre-broker-ordering the
// terminal events raced Submit's own Scheduled publish and slower hardware
// observed failed-then-scheduled; the broker's per-job hold makes the order
// structural, so the full sequence is assertable deterministically.
func TestScheduler_Events_SubmitFailure(t *testing.T) {
	s := newQuietScheduler(t)
	sub, cancel := s.Events().Subscribe()
	defer cancel()

	s.Submit("bad", func(_ context.Context, p *JobProgress) {
		p.Fail(time.Now(), "simulated")
	})

	evts := collectEvents(t, sub, 3)
	want := []JobEventKind{JobEventScheduled, JobEventStarted, JobEventFailed}
	for i, k := range want {
		if evts[i].Kind != k {
			t.Errorf("event[%d] kind=%v, want %v (all: %v)", i, evts[i].Kind, k, evts)
		}
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

	gotA := collectEvents(t, subA, 2)
	gotB := collectEvents(t, subB, 2)
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
	for i := range 2 {
		waittest.Recv(t, fmt.Sprintf("onJobChange call %d of 2", i+1), changed, nil)
	}
}

// TestScheduler_Events_RunOnceFailureEmitsFailed pins that a RunOnce task
// returning an error emits JobEventFailed.
//
// This assertion used to be the opposite, and its comment said why: "there's no
// JobProgress to carry failure state". That was a limitation being recorded as
// a requirement — a job that failed was reported to every subscriber, and to the
// Jobs inspector, as having completed. Every one-time job now carries a progress
// record, and gocron routes errors to a different listener, so the outcome is
// known rather than inferred.
func TestScheduler_Events_RunOnceFailureEmitsFailed(t *testing.T) {
	s := newQuietScheduler(t)
	sub, cancel := s.Events().Subscribe()
	defer cancel()

	if err := s.RunOnce("err", func() error {
		return errors.New("boom")
	}); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}

	evts := collectEvents(t, sub, 2)
	if evts[1].Kind != JobEventFailed {
		t.Errorf("a RunOnce task that returned an error emitted %v, want Failed", evts[1].Kind)
	}
	if p := evts[1].Job.Progress; p == nil {
		t.Error("failed one-time job carries no progress record")
	} else if p.Status != JobStatusFailed || p.Error == "" {
		t.Errorf("progress = {status:%v error:%q}, want failed with the task's error", p.Status, p.Error)
	}
}
