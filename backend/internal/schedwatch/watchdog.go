// Package schedwatch measures Go scheduler starvation (gastrolog-1io54g
// phase 2). A 10ms ticker records how late its own wake-ups arrive: gaps of
// hundreds of milliseconds mean NO goroutine ran on schedule — heartbeat
// senders, heartbeat receivers, and election timers included, in every Raft
// group at once, on leader and follower alike.
//
// Why this exists: consensus liveness is designed to be disk-independent
// (the multiraft heartbeat fast path does no I/O), yet followers lose live
// leaders under bulk-fsync load across ALL groups simultaneously. The one
// resource every group shares on a node is the runtime itself. mmap major
// page faults (GLCB merge reads, blob access) pin their P inside a
// non-preemptible kernel fault handler; enough goroutines faulting against
// a saturated disk stall the whole scheduler in bursts. With a 2s heartbeat
// timeout and a 1.5s leader lease, a multi-second stall IS an election.
//
// The watchdog turns that hypothesis falsifiable: if stall episodes line up
// with election timestamps, the mechanism is pinned; if elections happen
// with a quiet watchdog, the mechanism is elsewhere (transport, WAL).
package schedwatch

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"gastrolog/internal/alert"
)

const (
	// tick is the intended wake-up interval. Small enough to resolve
	// heartbeat-scale gaps, cheap enough to run always-on (one goroutine,
	// one timer read per tick).
	tick = 10 * time.Millisecond

	// stallNote is the smallest gap worth counting: scheduling jitter under
	// load sits well below this; page-fault convoys sit well above.
	stallNote = 100 * time.Millisecond

	// stallDebug logs at Debug: gaps this size delay Raft heartbeats by a
	// meaningful fraction of their 200ms send interval but are routine on a
	// loaded macOS host — business as usual, counted and visible in stats,
	// not worth a WARN per occurrence (they were near-constant under soak
	// load while consensus stayed healthy).
	stallDebug = 250 * time.Millisecond

	// stallCritical raises an operator alert: with a 1.5s leader lease and
	// 2s heartbeat timeout, gaps of this size manufacture elections.
	stallCritical = 1500 * time.Millisecond

	// alertID identifies the scheduler-stall alert; cleared after a clean
	// window with no critical stalls.
	alertID = "scheduler-stall"

	// alertClearAfter is how long the node must run without a critical
	// stall before the alert clears.
	alertClearAfter = 2 * time.Minute
)

// AlertSink is the subset of alert.Collector the watchdog raises through.
type AlertSink interface {
	Set(id string, severity alert.Severity, source, message string)
	Clear(id string)
}

// Watchdog measures scheduler wake-up lag. All counters are cumulative;
// MaxStallSince is take-once (resets on read) for rolling-window consumers.
type Watchdog struct {
	logger *slog.Logger
	alerts AlertSink

	stalls100 atomic.Uint64 // gaps >= 100ms
	stalls250 atomic.Uint64 // gaps >= 250ms
	stalls1s5 atomic.Uint64 // gaps >= 1.5s (election-lethal)

	maxStallNanos atomic.Int64 // max gap since last TakeMaxStall

	lastCriticalNanos atomic.Int64 // wall time of last critical stall
}

// New creates a watchdog. logger must be non-nil; alerts may be nil (no
// operator alerts, counters and logs only — tests).
func New(logger *slog.Logger, alerts AlertSink) *Watchdog {
	return &Watchdog{logger: logger, alerts: alerts}
}

// Counters returns cumulative stall counts (>=100ms, >=250ms, >=1.5s).
func (w *Watchdog) Counters() (n100, n250, n1500 uint64) {
	return w.stalls100.Load(), w.stalls250.Load(), w.stalls1s5.Load()
}

// TakeMaxStall returns the largest gap observed since the previous call and
// resets it. Mirrors the WAL max-latency take-once semantics so rolling
// stats windows see per-tick maxima.
func (w *Watchdog) TakeMaxStall() time.Duration {
	return time.Duration(w.maxStallNanos.Swap(0))
}

// Run blocks until ctx is cancelled, measuring wake-up lag every tick.
func (w *Watchdog) Run(ctx context.Context) {
	t := time.NewTicker(tick)
	defer t.Stop()
	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		now := time.Now()
		// Gap beyond the intended interval. Ticker ticks coalesce under
		// starvation, so one late wake-up carries the whole stall.
		gap := now.Sub(last) - tick
		last = now
		if gap < stallNote {
			w.maybeClearAlert(now)
			continue
		}
		w.record(now, gap)
	}
}

func (w *Watchdog) record(now time.Time, gap time.Duration) {
	w.stalls100.Add(1)
	for {
		cur := w.maxStallNanos.Load()
		if int64(gap) <= cur || w.maxStallNanos.CompareAndSwap(cur, int64(gap)) {
			break
		}
	}
	if gap < stallDebug {
		return
	}
	w.stalls250.Add(1)
	if gap < stallCritical {
		w.logger.Debug("scheduler stall: no goroutine ran on schedule",
			"gap", gap.Round(time.Millisecond),
			"heartbeat_interval", "200ms", "leader_lease", "1.5s")
		return
	}
	w.stalls1s5.Add(1)
	w.lastCriticalNanos.Store(now.UnixNano())
	w.logger.Warn("scheduler stall: no goroutine ran on schedule",
		"gap", gap.Round(time.Millisecond),
		"heartbeat_interval", "200ms", "leader_lease", "1.5s")
	if w.alerts != nil {
		w.alerts.Set(alertID, alert.Error, "runtime",
			"scheduler stalled "+gap.Round(time.Millisecond).String()+
				" — longer than the Raft leader lease; heartbeats and election timers did not run (gastrolog-1io54g)")
	}
}

func (w *Watchdog) maybeClearAlert(now time.Time) {
	if w.alerts == nil {
		return
	}
	lastCrit := w.lastCriticalNanos.Load()
	if lastCrit == 0 {
		return
	}
	if now.Sub(time.Unix(0, lastCrit)) >= alertClearAfter {
		w.alerts.Clear(alertID)
		w.lastCriticalNanos.Store(0)
	}
}
