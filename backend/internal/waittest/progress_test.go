package waittest

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// A zero-length stall window makes the observation count the only stall
// criterion, so these tests decide on observations, never on the clock.
var observationLimits = limits{
	stall:        0,
	backstop:     time.Hour,
	observations: 5,
	interval:     time.Microsecond,
}

func TestProgressReturnsOnceTheConditionHolds(t *testing.T) {
	t.Parallel()
	calls := 0
	Progress(t, "third sample", func() (string, bool) {
		calls++
		return fmt.Sprint(calls), calls >= 3
	})
	if calls != 3 {
		t.Fatalf("sample called %d times, want 3", calls)
	}
}

func TestProgressFailsOnStallWithTrajectory(t *testing.T) {
	t.Parallel()
	calls := 0
	err := pollProgress(observationLimits, "segments collected", func() (string, bool) {
		calls++
		n := min(calls, 3)
		return fmt.Sprintf("collected=%d", n), false
	})
	if err == nil {
		t.Fatal("a sample that stops changing must fail the wait")
	}
	msg := err.Error()
	for _, want := range []string{"segments collected", "progress stalled", "collected=1", "collected=2", "collected=3"} {
		if !strings.Contains(msg, want) {
			t.Errorf("failure %q lacks %q", msg, want)
		}
	}
	// Three changes, then the stall must span the full observation budget.
	if wantCalls := 3 + observationLimits.observations; calls != wantCalls {
		t.Errorf("sample called %d times, want %d", calls, wantCalls)
	}
}

func TestProgressNeverStallsWhileProgressChanges(t *testing.T) {
	t.Parallel()
	lim := observationLimits
	lim.observations = 1
	calls := 0
	err := pollProgress(lim, "steady progress", func() (string, bool) {
		calls++
		return fmt.Sprint(calls), calls >= 500
	})
	if err != nil {
		t.Fatalf("changing progress must not stall: %v", err)
	}
}

func TestProgressBackstopsALivelock(t *testing.T) {
	t.Parallel()
	lim := observationLimits
	lim.backstop = 0
	calls := 0
	err := pollProgress(lim, "oscillating", func() (string, bool) {
		calls++
		return fmt.Sprint(calls % 2), false
	})
	if err == nil || !strings.Contains(err.Error(), "backstop") {
		t.Fatalf("a changing-but-never-done sample must hit the backstop, got %v", err)
	}
}

func TestRecvReturnsTheEvent(t *testing.T) {
	t.Parallel()
	ch := make(chan int, 1)
	ch <- 7
	if got := Recv(t, "event", ch, nil); got != 7 {
		t.Fatalf("Recv = %d, want 7", got)
	}
}

func TestRecvWaitsWhileProgressChanges(t *testing.T) {
	t.Parallel()
	lim := observationLimits
	lim.observations = 1
	ch := make(chan string, 1)
	samples := 0
	got, err := recvEvent(lim, "ack", ch, func() string {
		samples++
		if samples == 200 {
			ch <- "acked"
		}
		return fmt.Sprintf("durable=%d", samples)
	})
	if err != nil {
		t.Fatalf("changing progress must not stall: %v", err)
	}
	if got != "acked" {
		t.Fatalf("Recv = %q, want acked", got)
	}
}

func TestRecvFailsWhenProgressStalls(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{})
	_, err := recvEvent(observationLimits, "ingest ack", ch, func() string { return "appended=3 durable=3" })
	if err == nil {
		t.Fatal("an event that never arrives while progress stands still must fail the wait")
	}
	for _, want := range []string{"ingest ack", "progress stalled", "appended=3 durable=3"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("failure %q lacks %q", err, want)
		}
	}
}

func TestRecvWithoutProgressFailsOnTheStallWindow(t *testing.T) {
	t.Parallel()
	ch := make(chan struct{})
	_, err := recvEvent(observationLimits, "ingester started", ch, nil)
	if err == nil || !strings.Contains(err.Error(), "no progress measure") {
		t.Fatalf("a missing event without a progress measure must fail, got %v", err)
	}
}
