package orchestrator

import (
	"fmt"
	"gastrolog/internal/glid"
	"sync"
	"time"

	"gastrolog/internal/alert"
)

// RateAlerter tracks per-vault event rates over a sliding window and raises
// or clears a cataloged alarm when the sustained rate crosses a threshold.
// It is the mechanism behind gastrolog-47qyw: detecting and surfacing
// pathological rotation or retention configurations as operator-visible
// signals rather than silent throughput collapse.
//
// The alerter owns only the CONDITION definition — the sustained-rate
// predicate (threshold + window) that decides when "<kind>-rate" is true
// for a vault. Priority, cause and response come from the alarm catalog
// like every other alarm: it raises through the ordinary
// Raise(typeID, instanceKey, detail) path, never choosing a priority.
//
// The alerter owns one RateWindow per vault and looks up vault names through
// an injected callback (so it doesn't need to know about the orchestrator's
// vault registry directly). Alarm type IDs are stable strings of the form
// "<kind>-rate" keyed per vault, so each vault has an independent
// Raise/Clear pair.
//
// Hysteresis: the alarm clears when the observed rate drops back below the
// threshold — there is no separate "clear at X" knob, because the rate
// window itself smooths over short bursts (a 30s window of 30 events at
// instant t is still 1/sec at instant t+15 even if no new events arrive).
// This naturally prevents flapping at the threshold.
type RateAlerter struct {
	mu      sync.Mutex
	windows map[glid.GLID]*RateWindow
	// active tracks whether the alarm is currently raised for each vault so
	// Evaluate can decide whether the condition changed.
	active map[glid.GLID]bool

	window    time.Duration
	kind      string  // e.g. "rotation" or "retention"
	threshold float64 // events/sec that makes the condition true
	alerts    alert.Sink
	vaultName func(glid.GLID) string // best-effort human label, "" if unknown
}

// rateAlerterConfig bundles the constructor parameters so RateAlerter
// constructions read clearly at the call site.
type rateAlerterConfig struct {
	Window    time.Duration
	Kind      string
	Threshold float64
	Alerts    alert.Sink
	VaultName func(glid.GLID) string
}

// newRateAlerter constructs a RateAlerter. vaultName may be nil; if provided,
// it returns a human label for the vault (e.g., the operator's chosen vault
// name from config) and is invoked under no locks so it must be safe to call
// concurrently.
func newRateAlerter(cfg rateAlerterConfig) *RateAlerter {
	return &RateAlerter{
		windows:   make(map[glid.GLID]*RateWindow),
		active:    make(map[glid.GLID]bool),
		window:    cfg.Window,
		kind:      cfg.Kind,
		threshold: cfg.Threshold,
		alerts:    cfg.Alerts,
		vaultName: cfg.VaultName,
	}
}

// Record marks one event for the given vault at the given time. Lazily
// creates a per-vault RateWindow on first call. Safe for concurrent use.
func (r *RateAlerter) Record(vaultID glid.GLID, now time.Time) {
	r.mu.Lock()
	w, ok := r.windows[vaultID]
	if !ok {
		w = NewRateWindow(r.window)
		r.windows[vaultID] = w
	}
	r.mu.Unlock()
	w.Record(now)
}

// Forget removes a vault's tracking and clears any active alarm for it.
// Call this when a vault is removed from the orchestrator.
func (r *RateAlerter) Forget(vaultID glid.GLID) {
	r.mu.Lock()
	delete(r.windows, vaultID)
	wasActive := r.active[vaultID]
	delete(r.active, vaultID)
	r.mu.Unlock()
	if wasActive && r.alerts != nil {
		r.alerts.Clear(r.alarmTypeID(), vaultID.String())
	}
}

// Evaluate walks every tracked vault, computes its current rate, and raises
// or clears the alarm as the threshold dictates. Intended to be called on
// a fixed cadence (e.g., every 5 seconds) by a background goroutine.
func (r *RateAlerter) Evaluate(now time.Time) {
	type pending struct {
		vaultID glid.GLID
		up      bool // condition true (raise) vs resolved (clear)
		rate    float64
		count   int64
	}
	var work []pending

	r.mu.Lock()
	for vaultID, w := range r.windows {
		rate := w.Rate(now)
		count := w.Count(now)
		up := rate >= r.threshold
		if up == r.active[vaultID] {
			continue
		}
		r.active[vaultID] = up
		work = append(work, pending{vaultID: vaultID, up: up, rate: rate, count: count})
	}
	r.mu.Unlock()

	if r.alerts == nil {
		return
	}
	for _, p := range work {
		if !p.up {
			r.alerts.Clear(r.alarmTypeID(), p.vaultID.String())
			continue
		}
		r.alerts.Raise(r.alarmTypeID(), p.vaultID.String(), r.message(p.vaultID, p.rate, p.count))
	}
}

func (r *RateAlerter) alarmTypeID() string {
	return r.kind + "-rate"
}

func (r *RateAlerter) message(vaultID glid.GLID, rate float64, count int64) string {
	label := vaultID.String()
	if r.vaultName != nil {
		if name := r.vaultName(vaultID); name != "" {
			label = fmt.Sprintf("%s (%s)", name, vaultID.String()[:8])
		}
	}
	return fmt.Sprintf(
		"Vault %s: %s rate %.2f/s (%d events in last %s) — review policy",
		label, r.kind, rate, count, r.window,
	)
}
