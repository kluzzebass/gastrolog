package orchestrator

import (
	"errors"
	"sync/atomic"
	"time"
)

// retentionFanOutStallWindow is how long a route fan-out may go without a
// single record accepted before it is aborted and the chunk retained for a
// later sweep. Generous on purpose: it exists to stop a sweep from parking
// forever on a jammed pipeline (a destination that passes its admission
// gate but stops draining), not to police throughput.
const retentionFanOutStallWindow = 2 * time.Minute

// errRetentionFanOutStalled is the abort cause when the watchdog fires. The
// abort does not consume the one-shot route flag, so the next sweep retries
// the chunk from scratch.
var errRetentionFanOutStalled = errors.New(
	"route fan-out made no progress for a full stall window; chunk retained for a later sweep")

// progressWatch is the watchdog's progress ledger: submit workers bump it on
// every completed record submit, the monitor consumes it per tick. stalled()
// is check-and-consume: it reports whether NO bump happened since the
// previous call. Single-consumer (the monitor goroutine); bump is safe from
// any number of workers.
type progressWatch struct {
	progressed atomic.Uint64
	seen       uint64
}

func (w *progressWatch) bump() { w.progressed.Add(1) }

func (w *progressWatch) stalled() bool {
	cur := w.progressed.Load()
	if cur != w.seen {
		w.seen = cur
		return false
	}
	return true
}

// runStallMonitor aborts a fan-out that makes no progress for one full tick
// interval. The tick channel is injected so tests drive it without clocks;
// fireRetentionEvent passes a real time.Ticker channel. Returns when done
// closes or after aborting.
func runStallMonitor(done <-chan struct{}, tick <-chan time.Time, w *progressWatch, abort func(error)) {
	for {
		select {
		case <-done:
			return
		case <-tick:
			if w.stalled() {
				abort(errRetentionFanOutStalled)
				return
			}
		}
	}
}
