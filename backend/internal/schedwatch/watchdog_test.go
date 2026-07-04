package schedwatch

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
)

type sinkSpy struct {
	mu      sync.Mutex
	set     map[string]string
	cleared int
}

func (s *sinkSpy) Set(id string, _ alert.Severity, _, msg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.set == nil {
		s.set = map[string]string{}
	}
	s.set[id] = msg
}

func (s *sinkSpy) Clear(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.set, id)
	s.cleared++
}

func (s *sinkSpy) active() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.set)
}

func TestRecordCountsTiers(t *testing.T) {
	t.Parallel()
	w := New(slog.Default(), nil)
	base := time.Unix(0, 0)

	w.record(base, 120*time.Millisecond)  // note only
	w.record(base, 300*time.Millisecond)  // warn
	w.record(base, 1600*time.Millisecond) // critical

	n100, n250, n1500 := w.Counters()
	if n100 != 3 || n250 != 2 || n1500 != 1 {
		t.Fatalf("counters = %d/%d/%d, want 3/2/1", n100, n250, n1500)
	}
	if got := w.TakeMaxStall(); got != 1600*time.Millisecond {
		t.Fatalf("TakeMaxStall = %v, want 1.6s", got)
	}
	if got := w.TakeMaxStall(); got != 0 {
		t.Fatalf("TakeMaxStall after take = %v, want 0 (take-once)", got)
	}
}

func TestCriticalStallRaisesAndClearsAlert(t *testing.T) {
	t.Parallel()
	sink := &sinkSpy{}
	w := New(slog.Default(), sink)
	base := time.Unix(1_700_000_000, 0)

	w.record(base, 2*time.Second)
	if sink.active() != 1 {
		t.Fatal("critical stall must raise the scheduler-stall alert")
	}

	// Quiet ticks inside the clear window keep the alert up.
	w.maybeClearAlert(base.Add(time.Minute))
	if sink.active() != 1 {
		t.Fatal("alert cleared too early")
	}

	// A clean window clears it.
	w.maybeClearAlert(base.Add(alertClearAfter + time.Second))
	if sink.active() != 0 {
		t.Fatal("alert must clear after a clean window")
	}
}

func TestRunMeasuresRealGaps(t *testing.T) {
	t.Parallel()
	w := New(slog.Default(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()

	// A healthy scheduler must not register 100ms+ stalls while idling.
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done
	n100, _, n1500 := w.Counters()
	if n1500 != 0 {
		t.Fatalf("idle run recorded %d critical stalls", n1500)
	}
	// Allow a little CI jitter at the lowest tier, but a quiet run should
	// stay near zero.
	if n100 > 5 {
		t.Fatalf("idle run recorded %d >=100ms stalls — watchdog misconfigured", n100)
	}
}
