package scatterbox

import (
	"context"
	"time"
)

// nextEmission returns the next wall-clock-aligned instant strictly after now
// for interval. Boundaries are anchored to the Unix epoch, so every node with
// a reasonably synchronized clock fires on the same schedule regardless of
// when the ingester started — unlike a delay-from-start ticker.
func nextEmission(now time.Time, interval time.Duration) time.Time {
	if interval <= 0 {
		return now
	}
	step := interval.Nanoseconds()
	if step <= 0 {
		return now
	}
	n := now.UnixNano()
	return time.Unix(0, ((n/step)+1)*step)
}

// waitForEmission blocks until the next aligned boundary, ctx is cancelled,
// or trigger fires (manual one-shot emission in continuous mode).
func waitForEmission(ctx context.Context, interval time.Duration, trigger <-chan struct{}) (scheduled time.Time, triggered bool, err error) {
	for {
		now := time.Now()
		scheduled = nextEmission(now, interval)
		timer := time.NewTimer(time.Until(scheduled))
		select {
		case <-ctx.Done():
			timer.Stop()
			return time.Time{}, false, ctx.Err()
		case <-trigger:
			timer.Stop()
			return time.Time{}, true, nil
		case <-timer.C:
			return scheduled, false, nil
		}
	}
}
