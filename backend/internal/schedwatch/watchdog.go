// Package schedwatch measures Go scheduler starvation. A 10ms ticker
// records how late its own wake-ups arrive: gaps of
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

	// stallCritical logs at Warn: gaps at or beyond the Raft leader lease
	// manufacture elections. This is the fallback when New is given no
	// lease; production passes the configured lease so the critical tier
	// stays lease-lethal by definition. Critical stalls are a diagnostic —
	// there is no operator action beyond knowing — so they surface as logs
	// and health counters, never as alarms (EEMUA 191 actionability test).
	stallCritical = 1500 * time.Millisecond
)

// Watchdog measures scheduler wake-up lag. All counters are cumulative;
// MaxStallSince is take-once (resets on read) for rolling-window consumers.
type Watchdog struct {
	logger *slog.Logger
	// critical is the election-lethal gap threshold — the configured Raft
	// leader lease (stallCritical when New is given zero).
	critical time.Duration

	stalls100      atomic.Uint64 // gaps >= 100ms
	stalls250      atomic.Uint64 // gaps >= 250ms
	stallsCritical atomic.Uint64 // gaps >= critical (election-lethal)

	maxStallNanos atomic.Int64 // max gap since last TakeMaxStall
}

// New creates a watchdog. logger must be non-nil. leaderLease sets the
// critical (election-lethal) tier; zero falls back to the 1.5s default.
func New(logger *slog.Logger, leaderLease time.Duration) *Watchdog {
	if leaderLease <= 0 {
		leaderLease = stallCritical
	}
	return &Watchdog{logger: logger, critical: leaderLease}
}

// Counters returns cumulative stall counts (>=100ms, >=250ms, >=lease).
func (w *Watchdog) Counters() (n100, n250, nCritical uint64) {
	return w.stalls100.Load(), w.stalls250.Load(), w.stallsCritical.Load()
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
			continue
		}
		w.record(gap)
	}
}

func (w *Watchdog) record(gap time.Duration) {
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
	if gap < w.critical {
		w.logger.Debug("scheduler stall: no goroutine ran on schedule",
			"gap", gap.Round(time.Millisecond),
			"leader_lease", w.critical)
		return
	}
	w.stallsCritical.Add(1)
	w.logger.Warn("scheduler stall: no goroutine ran on schedule",
		"gap", gap.Round(time.Millisecond),
		"leader_lease", w.critical)
}
