package logging

import (
	"sync"
	"time"
)

// Throttle rate-limits repeated log emissions per key: the first Allow in
// each interval passes and reports how many were suppressed since the last
// pass. Retry loops against a known-failing dependency are correct behavior;
// logging every attempt buries the signal under a firehose: a 6h blocked
// build once produced 300k+ identical warn lines. The emitting site includes
// the suppressed count so operators still see magnitude ("still blocked, N
// retries suppressed"), just not N lines.
type Throttle struct {
	// Interval is the minimum spacing between emissions per key.
	Interval time.Duration
	// Now overrides the clock (tests). Nil uses time.Now.
	Now func() time.Time

	mu      sync.Mutex
	entries map[string]*throttleEntry
}

type throttleEntry struct {
	lastEmit   time.Time
	suppressed int
}

// throttlePruneFactor times Interval is how long an idle key survives
// before it is dropped during Allow bookkeeping; throttlePruneAbove is the
// map size that triggers pruning. Keys are vault/peer scoped (dozens), so
// pruning exists only to bound pathological key churn.
const (
	throttlePruneFactor = 4
	throttlePruneAbove  = 1024
)

// Allow reports whether the caller should emit now. When true, suppressed
// is the number of Allow calls swallowed for this key since the previous
// emission.
func (t *Throttle) Allow(key string) (suppressed int, ok bool) {
	now := time.Now()
	if t.Now != nil {
		now = t.Now()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries == nil {
		t.entries = make(map[string]*throttleEntry)
	}
	e := t.entries[key]
	if e == nil {
		t.entries[key] = &throttleEntry{lastEmit: now}
		t.pruneLocked(now)
		return 0, true
	}
	if now.Sub(e.lastEmit) < t.Interval {
		e.suppressed++
		return 0, false
	}
	suppressed = e.suppressed
	e.suppressed = 0
	e.lastEmit = now
	return suppressed, true
}

func (t *Throttle) pruneLocked(now time.Time) {
	if len(t.entries) <= throttlePruneAbove {
		return
	}
	idle := time.Duration(throttlePruneFactor) * t.Interval
	for k, e := range t.entries {
		if now.Sub(e.lastEmit) > idle && e.suppressed == 0 {
			delete(t.entries, k)
		}
	}
}
