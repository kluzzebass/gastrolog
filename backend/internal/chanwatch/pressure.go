package chanwatch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// PressureLevel categorizes how full a monitored channel is. Values are
// ordered so callers can compare with `>=` to decide how to react.
type PressureLevel int32

const (
	// PressureNormal means the channel is operating below the elevated
	// threshold. Ingesters should run at full rate.
	PressureNormal PressureLevel = iota

	// PressureElevated means the channel has crossed the elevated threshold.
	// Ingesters should pause briefly or reduce their rate until the gate
	// returns to normal.
	PressureElevated

	// PressureCritical means the channel is close to full. Ingesters must
	// pause entirely until the gate returns to normal.
	PressureCritical
)

// String returns a human-readable name for the level.
func (p PressureLevel) String() string {
	switch p {
	case PressureNormal:
		return "normal"
	case PressureElevated:
		return "elevated"
	case PressureCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// PressureThresholds control when a gate escalates or relaxes its state.
// Hysteresis is encoded by the gap between the escalate thresholds
// (ElevatedAt, CriticalAt) and the single de-escalate threshold (NormalAt).
// Once pressure is elevated or critical, the gate only returns to normal
// when the ratio drops below NormalAt — this prevents throttle flapping.
type PressureThresholds struct {
	NormalAt    float64 // gate returns to Normal below this ratio
	ElevatedAt  float64 // gate enters Elevated at or above this ratio
	CriticalAt  float64 // gate enters Critical at or above this ratio
}

// DefaultThresholds returns the recommended thresholds from gastrolog-4fguu:
// escalate to Elevated at 80%, to Critical at 95%, return to Normal at 50%.
func DefaultThresholds() PressureThresholds {
	return PressureThresholds{
		NormalAt:   0.50,
		ElevatedAt: 0.80,
		CriticalAt: 0.95,
	}
}

// PressureTransition is delivered to the OnChange callback whenever the
// gate's aggregate pressure level changes. Callers use this to emit alerts
// or trigger side effects.
type PressureTransition struct {
	From  PressureLevel
	To    PressureLevel
	Cause string // name of the channel that forced this transition
	Ratio float64
}

// PressureGate aggregates the pressure state of one or more channels and
// exposes a shared signal that ingesters can consult to decide whether to
// throttle. The aggregate level is the maximum level across all probes:
// if any one channel is critical, the gate reports critical.
//
// Callers wait on Wait() to block while pressure is elevated or critical,
// or use Level() for a non-blocking check.
type PressureGate struct {
	thresholds PressureThresholds
	level      atomic.Int32 // PressureLevel

	mu        sync.Mutex
	probes    []pressureProbe
	waiters   []chan struct{}
	changed   chan struct{} // closed+recreated on every level change
	onChanges []func(PressureTransition)
}

type pressureProbe struct {
	name    string
	probe   Probe
	level   PressureLevel // last computed level for this probe
}

// NewPressureGate creates a gate with the given thresholds. Use
// DefaultThresholds() for the standard 50%/80%/95% configuration.
func NewPressureGate(thresholds PressureThresholds) *PressureGate {
	g := &PressureGate{
		thresholds: thresholds,
		changed:    make(chan struct{}),
	}
	return g
}

// AddProbe registers a channel to monitor for pressure. Safe to call after
// Run has started.
func (g *PressureGate) AddProbe(name string, probe Probe) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.probes = append(g.probes, pressureProbe{name: name, probe: probe})
}

// AddOnChange registers a callback fired (outside the gate lock) on every
// aggregate pressure transition. Multiple callbacks can be registered;
// each is invoked in registration order on every transition. Use this to
// raise alerts, emit logs, or adjust capture filters.
func (g *PressureGate) AddOnChange(fn func(PressureTransition)) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onChanges = append(g.onChanges, fn)
}

// Level returns the current aggregate pressure level. Non-blocking.
func (g *PressureGate) Level() PressureLevel {
	return PressureLevel(g.level.Load())
}

// IsNormal returns true when the gate is in the Normal state. Equivalent
// to g.Level() == PressureNormal but clearer at call sites.
func (g *PressureGate) IsNormal() bool {
	return g.Level() == PressureNormal
}

// Wait blocks until the gate returns to Normal, or ctx is cancelled.
// Returns ctx.Err() if the context fires before pressure clears, or nil
// if pressure is (or becomes) normal. Ingesters call this immediately
// before sending a record into a monitored channel.
func (g *PressureGate) Wait(ctx context.Context) error {
	for {
		if g.IsNormal() {
			return nil
		}
		g.mu.Lock()
		ch := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			// Level changed — re-check.
		}
	}
}

// Run polls all probes at the given interval, updating the aggregate level
// and notifying waiters on transitions. Blocks until ctx is cancelled.
func (g *PressureGate) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.tick()
		}
	}
}

func (g *PressureGate) tick() {
	g.mu.Lock()
	var (
		maxLevel = PressureNormal
		maxCause string
		maxRatio float64
	)
	for i := range g.probes {
		p := &g.probes[i]
		length, capacity := p.probe()
		if capacity == 0 {
			continue
		}
		ratio := float64(length) / float64(capacity)
		next := g.classify(p.level, ratio)
		p.level = next
		if next > maxLevel {
			maxLevel = next
			maxCause = p.name
			maxRatio = ratio
		}
	}
	prev := PressureLevel(g.level.Load())
	if prev == maxLevel {
		g.mu.Unlock()
		return
	}
	g.level.Store(int32(maxLevel))
	// Wake all waiters by closing + recreating the changed channel.
	close(g.changed)
	g.changed = make(chan struct{})
	// Snapshot callbacks so we can invoke them outside the lock.
	callbacks := make([]func(PressureTransition), len(g.onChanges))
	copy(callbacks, g.onChanges)
	g.mu.Unlock()

	tr := PressureTransition{
		From:  prev,
		To:    maxLevel,
		Cause: maxCause,
		Ratio: maxRatio,
	}
	for _, fn := range callbacks {
		fn(tr)
	}
}

// classify computes the next level for a single probe given its previous
// level and the current fill ratio. Hysteresis: a probe that's elevated
// stays elevated until the ratio drops below NormalAt; same for critical.
func (g *PressureGate) classify(prev PressureLevel, ratio float64) PressureLevel {
	// Upward transitions — use escalate thresholds.
	if ratio >= g.thresholds.CriticalAt {
		return PressureCritical
	}
	if ratio >= g.thresholds.ElevatedAt {
		// If already critical, stay critical until ratio drops below NormalAt.
		if prev == PressureCritical {
			return PressureCritical
		}
		return PressureElevated
	}
	// Below ElevatedAt but above NormalAt — hold the previous level.
	if ratio >= g.thresholds.NormalAt {
		return prev
	}
	// Below NormalAt — clear to normal.
	return PressureNormal
}
