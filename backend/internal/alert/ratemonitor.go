package alert

import (
	"fmt"
	"sync"
	"time"
)

// Rate self-monitoring (EEMUA 191 principle 2): the alarm system measures
// its own annunciation rate and says so when it floods. A flooding alarm
// system is a degraded alarm system — during an upset the list outruns what
// an operator can read exactly when clarity is needed most.
//
// The monitor is per-node, like the collector it observes: a node flooding
// is a fact about that node, and per-node attribution in the aggregated UI
// shows which one. There is no cluster-aggregate flood condition — summing
// rates across nodes would blur which alarm system is degraded.
const (
	// FloodTypeID is the alarm type of the flood meta-alarm. The monitor
	// never counts it toward the rate: a flood must not feed itself, and
	// exactly one flood alarm exists per node no matter the overshoot.
	FloodTypeID = "alarm-flood"

	// FloodWindow is the rolling window the rate is measured over. Fixed by
	// the standard's rate principle (~1 alarm per operator per 10 minutes),
	// not operator-adjustable — the threshold is.
	FloodWindow = 10 * time.Minute

	// DefaultFloodThreshold is the default alarm-activation count per
	// rolling window above which the flood alarm raises. Operator-adjustable
	// via the cluster settings (alarm_flood_threshold); a stored 0 means
	// this default.
	DefaultFloodThreshold = 10
)

// RateMonitor tracks alarm activations on this node over a rolling window
// and raises exactly one alarm-flood meta-alarm while the rate is over
// threshold. It observes the collector via the activation hook
// (Collector.SetOnActivate) and raises/clears the flood alarm back through
// the same collector.
//
// The clock is injectable so tests advance time deterministically — the
// window is a time construct and must never be tested with sleeps.
type RateMonitor struct {
	sink Sink
	now  func() time.Time

	mu          sync.Mutex
	threshold   int
	activations []time.Time // rolling window of activation instants, oldest first
	flooding    bool
	// overUntil is the instant the rolling count decays (or decayed) back
	// to the threshold after last exceeding it: the expiry of the newest
	// activation that was surplus at the time. The flood clears once a full
	// window has passed after this instant with the rate staying under.
	overUntil time.Time
}

// NewRateMonitor creates a monitor raising through sink with the given
// clock. A nil now defaults to time.Now. The threshold starts at
// DefaultFloodThreshold until SetThreshold overrides it.
func NewRateMonitor(sink Sink, now func() time.Time) *RateMonitor {
	if now == nil {
		now = time.Now
	}
	return &RateMonitor{
		sink:      sink,
		now:       now,
		threshold: DefaultFloodThreshold,
	}
}

// SetThreshold sets the flood threshold (activations per rolling window).
// Zero or negative selects DefaultFloodThreshold — the stored setting uses 0
// for "default". The new threshold applies from the next Observe/Evaluate;
// a flood already latched by the old threshold still clears only after a
// full clean window (the over-threshold instant is history, not config).
func (m *RateMonitor) SetThreshold(n int) {
	if n <= 0 {
		n = DefaultFloodThreshold
	}
	m.mu.Lock()
	m.threshold = n
	m.mu.Unlock()
}

// Threshold returns the effective flood threshold.
func (m *RateMonitor) Threshold() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.threshold
}

// Observe records one alarm activation (an alarm transitioning inactive →
// active in the collector) and advances the flood state machine. It is the
// Collector.SetOnActivate hook. Activations of the flood alarm itself are
// ignored — before locking, so the sink.Raise inside the state machine may
// re-enter here without deadlocking.
func (m *RateMonitor) Observe(typeID string) {
	if typeID == FloodTypeID {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.activations = append(m.activations, m.now())
	m.evaluateLocked()
}

// Evaluate prunes the window and advances the flood state machine — raising
// the flood alarm if a lowered threshold put the current rate over, and
// clearing it once the rate has stayed under threshold for a full window.
// Returns the current rolling activation count (the rate gauge). Called
// periodically (scheduler job) and from tests with an advanced clock.
func (m *RateMonitor) Evaluate() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.evaluateLocked()
}

// Rate returns the current rolling activation count without touching the
// flood state machine. Used for the NodeStats gauge.
func (m *RateMonitor) Rate() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.prune(m.now())
	return len(m.activations)
}

// evaluateLocked prunes expired activations and drives the flood alarm.
// Caller holds m.mu. Sink calls are made while holding the lock; that is
// safe because the collector invokes the activation hook outside its own
// lock and Observe rejects FloodTypeID before locking.
func (m *RateMonitor) evaluateLocked() int {
	now := m.now()
	m.prune(now)
	n := len(m.activations)
	switch {
	case n > m.threshold:
		// The count only grows at an activation and decays as activations
		// age out, so the instant it returns to the threshold is the expiry
		// of the newest currently-surplus activation.
		m.overUntil = m.activations[n-m.threshold-1].Add(FloodWindow)
		m.sink.Raise(FloodTypeID, "", fmt.Sprintf(
			"%d alarms raised on this node in the last 10 minutes (flood threshold %d).",
			n, m.threshold))
		m.flooding = true
	case m.flooding && now.Sub(m.overUntil) >= FloodWindow:
		m.flooding = false
		m.sink.Clear(FloodTypeID, "")
	}
	return n
}

// prune drops activations that have aged out of the rolling window.
// Caller holds m.mu.
func (m *RateMonitor) prune(now time.Time) {
	cutoff := now.Add(-FloodWindow)
	i := 0
	for i < len(m.activations) && !m.activations[i].After(cutoff) {
		i++
	}
	if i > 0 {
		m.activations = append(m.activations[:0], m.activations[i:]...)
	}
}
