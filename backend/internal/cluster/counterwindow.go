package cluster

import "time"

// counterWindow measures how much a cumulative counter rose over the most
// recent span, from readings recorded at stats ticks. Unlike a per-tick
// rate it does not extrapolate: one event inside the span reads as one,
// however short the tick that saw it.
type counterWindow struct {
	span time.Duration
	// samples is ascending by time. Once the span is full, samples[0] is
	// the newest reading at or before the span's start — the baseline the
	// rise is measured from.
	samples []counterSample
}

type counterSample struct {
	at    time.Time
	value uint64
}

// observe returns the counter's rise over the span ending at now. A step
// records the reading; a read between ticks measures against the recorded
// readings without changing them. Before the span has filled, the rise is
// measured from the first reading.
func (w *counterWindow) observe(now time.Time, value uint64, step bool) uint64 {
	if n := len(w.samples); n > 0 && value < w.samples[n-1].value {
		w.samples = w.samples[:0]
	}
	if step {
		w.samples = append(w.samples, counterSample{at: now, value: value})
		start := now.Add(-w.span)
		for len(w.samples) > 1 && !w.samples[1].at.After(start) {
			w.samples = w.samples[1:]
		}
	}
	if len(w.samples) == 0 {
		return 0
	}
	return value - w.baseline(now)
}

func (w *counterWindow) baseline(now time.Time) uint64 {
	start := now.Add(-w.span)
	base := w.samples[0].value
	for _, s := range w.samples[1:] {
		if s.at.After(start) {
			break
		}
		base = s.value
	}
	return base
}
