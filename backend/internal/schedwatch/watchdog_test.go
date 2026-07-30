package schedwatch

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

func TestRecordCountsTiers(t *testing.T) {
	t.Parallel()
	w := New(slog.Default(), 0)

	w.record(120 * time.Millisecond)  // note only
	w.record(300 * time.Millisecond)  // debug tier
	w.record(1600 * time.Millisecond) // critical

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

// TestCriticalTierFollowsConfiguredLease: with a widened leader lease, gaps
// past the 1.5s default but inside the lease stay sub-critical — no critical
// count. Critical means lease-lethal, so it has to follow the configured
// lease rather than a constant.
func TestCriticalTierFollowsConfiguredLease(t *testing.T) {
	t.Parallel()
	w := New(slog.Default(), 4*time.Second)

	w.record(2 * time.Second) // lethal at default lease, survivable at 4s
	_, _, nCritical := w.Counters()
	if nCritical != 0 {
		t.Fatalf("2s gap under a 4s lease: criticals=%d, want 0", nCritical)
	}

	w.record(5 * time.Second)
	_, _, nCritical = w.Counters()
	if nCritical != 1 {
		t.Fatalf("5s gap under a 4s lease: criticals=%d, want 1", nCritical)
	}
}

func TestRunMeasuresRealGaps(t *testing.T) {
	t.Parallel()
	w := New(slog.Default(), 0)
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
