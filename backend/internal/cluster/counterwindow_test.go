package cluster

import (
	"testing"
	"time"
)

func TestCounterWindow(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tick := 5 * time.Second

	t.Run("first reading is the baseline", func(t *testing.T) {
		t.Parallel()
		w := counterWindow{span: time.Minute}
		if got := w.observe(t0, 7, true); got != 0 {
			t.Fatalf("rise = %d, want 0: counts before the first reading are history", got)
		}
	})

	t.Run("one event in one short tick reads as one", func(t *testing.T) {
		t.Parallel()
		w := counterWindow{span: time.Minute}
		w.observe(t0, 0, true)
		if got := w.observe(t0.Add(tick), 1, true); got != 1 {
			t.Fatalf("rise = %d, want 1", got)
		}
	})

	t.Run("events across ticks inside the span accumulate", func(t *testing.T) {
		t.Parallel()
		w := counterWindow{span: time.Minute}
		w.observe(t0, 0, true)
		var got uint64
		for i := 1; i <= 3; i++ {
			got = w.observe(t0.Add(time.Duration(i)*20*time.Second), uint64(i), true)
		}
		if got != 3 {
			t.Fatalf("rise = %d, want 3 events within one span", got)
		}
	})

	t.Run("events slide out of the span", func(t *testing.T) {
		t.Parallel()
		w := counterWindow{span: time.Minute}
		w.observe(t0, 0, true)
		w.observe(t0.Add(tick), 2, true)
		at := t0.Add(tick)
		for at.Before(t0.Add(time.Minute)) {
			at = at.Add(tick)
			if got := w.observe(at, 2, true); got != 2 {
				t.Fatalf("rise at %s = %d, want 2 while the events are inside the span", at.Sub(t0), got)
			}
		}
		if got := w.observe(at.Add(tick), 2, true); got != 0 {
			t.Fatalf("rise = %d, want 0 once the events are a span old", got)
		}
		if n := len(w.samples); n > int(time.Minute/tick)+1 {
			t.Fatalf("window holds %d samples, want at most one span of ticks plus the baseline", n)
		}
	})

	t.Run("read between ticks measures without recording", func(t *testing.T) {
		t.Parallel()
		w := counterWindow{span: time.Minute}
		w.observe(t0, 0, true)
		w.observe(t0.Add(tick), 1, true)
		if got := w.observe(t0.Add(tick+time.Second), 3, false); got != 3 {
			t.Fatalf("between-tick rise = %d, want 3", got)
		}
		if n := len(w.samples); n != 2 {
			t.Fatalf("between-tick read recorded a sample: %d samples, want 2", n)
		}
	})

	t.Run("counter reset re-anchors", func(t *testing.T) {
		t.Parallel()
		w := counterWindow{span: time.Minute}
		w.observe(t0, 50, true)
		w.observe(t0.Add(tick), 60, true)
		if got := w.observe(t0.Add(2*tick), 1, true); got != 0 {
			t.Fatalf("rise after reset = %d, want 0", got)
		}
		if got := w.observe(t0.Add(3*tick), 2, true); got != 1 {
			t.Fatalf("rise after reset = %d, want 1", got)
		}
	})

	t.Run("no readings reads zero", func(t *testing.T) {
		t.Parallel()
		w := counterWindow{span: time.Minute}
		if got := w.observe(t0, 4, false); got != 0 {
			t.Fatalf("rise = %d, want 0 with nothing recorded", got)
		}
	})
}
