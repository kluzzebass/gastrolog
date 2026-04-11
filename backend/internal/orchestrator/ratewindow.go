package orchestrator

import (
	"sync"
	"time"
)

// RateWindow is a fixed-memory sliding-window event counter. It tracks the
// number of events recorded over a configurable window (e.g., last 30s) and
// reports the average rate as events per second.
//
// Internally it maintains a ring of N one-second buckets. Recording an event
// increments the bucket for the current second; if that bucket's timestamp is
// stale (older than one full window), it is reset to zero before incrementing.
// Rate() walks the ring once and sums every bucket whose timestamp is within
// the window of "now".
//
// Memory is constant per RateWindow regardless of event rate. Both Record and
// Rate are O(N) where N is the bucket count, but with N=30 that's negligible.
//
// Safe for concurrent use.
type RateWindow struct {
	mu      sync.Mutex
	buckets []rateBucket
	window  time.Duration
}

type rateBucket struct {
	second int64 // unix seconds; zero means "never written"
	count  int64
}

// NewRateWindow returns a RateWindow that tracks events over the given
// window. The window is divided into one-second buckets (so window=30s gives
// 30 buckets). A panic is raised if window < 1s, since sub-second resolution
// would require either a different bucket scheme or returning a zero rate
// for any single-event observation.
func NewRateWindow(window time.Duration) *RateWindow {
	if window < time.Second {
		panic("ratewindow: window must be >= 1 second")
	}
	n := int(window / time.Second)
	return &RateWindow{
		buckets: make([]rateBucket, n),
		window:  window,
	}
}

// Record increments the bucket for the second containing now. If the bucket
// is stale (its second is from a previous window cycle), it is reset before
// incrementing.
func (r *RateWindow) Record(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	sec := now.Unix()
	idx := int(sec % int64(len(r.buckets)))
	if r.buckets[idx].second != sec {
		r.buckets[idx].second = sec
		r.buckets[idx].count = 0
	}
	r.buckets[idx].count++
}

// Rate returns the average events-per-second over the window ending at now.
// Buckets older than the window are excluded; empty buckets contribute zero.
// The denominator is the full window duration so the rate is comparable
// across calls regardless of how recently events were recorded.
func (r *RateWindow) Rate(now time.Time) float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := now.Unix() - int64(r.window/time.Second) + 1
	var total int64
	for i := range r.buckets {
		if r.buckets[i].second >= cutoff && r.buckets[i].second <= now.Unix() {
			total += r.buckets[i].count
		}
	}
	return float64(total) / r.window.Seconds()
}

// Count returns the total number of events recorded within the window
// ending at now (i.e., Rate × window). Useful for alert messages where
// "47 events in the last 30s" reads better than "1.57 events/s".
func (r *RateWindow) Count(now time.Time) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := now.Unix() - int64(r.window/time.Second) + 1
	var total int64
	for i := range r.buckets {
		if r.buckets[i].second >= cutoff && r.buckets[i].second <= now.Unix() {
			total += r.buckets[i].count
		}
	}
	return total
}
