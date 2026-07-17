package orchestrator

import (
	"fmt"
	"gastrolog/internal/glid"
	"sync"
	"time"

	"gastrolog/internal/alert"
)

// RateAlerter tracks per-vault event rates over a sliding window and raises
// or clears alarms when sustained rates exceed configured thresholds. It is
// the mechanism behind gastrolog-47qyw: detecting and surfacing pathological
// rotation or retention configurations as operator-visible signals rather
// than silent throughput collapse.
//
// Rate alarms are OPERATOR-DEFINED: the thresholds are the rule, so the
// priority comes from which threshold was crossed rather than from the
// static alarm catalog. They enter the collector through RaiseOperator and
// live beside the catalog, never inside it — see the design doc's
// "<kind>-rate" row. Crossing lowAt raises Low; crossing highAt escalates
// to High (never Critical: a pathological policy degrades throughput, it
// does not lose accepted data).
//
// The alerter owns one RateWindow per vault and looks up vault names through
// an injected callback (so it doesn't need to know about the orchestrator's
// vault registry directly). Alarm type IDs are stable strings of the form
// "<kind>-rate" keyed per vault, so each vault has an independent
// Raise/Clear pair.
//
// Hysteresis: lowAt and highAt are escalation thresholds. The alarm only
// clears when the observed rate drops back to below lowAt — there is no
// separate "clear at X" knob, because the rate window itself smooths over
// short bursts (a 30s window of 30 events at instant t is still 1/sec at
// instant t+15 even if no new events arrive). This naturally prevents
// flapping at the threshold.
type RateAlerter struct {
	mu      sync.Mutex
	windows map[glid.GLID]*RateWindow
	// active tracks the last priority we raised for each vault so Evaluate
	// can decide whether the alarm state changed.
	active map[glid.GLID]alert.Priority

	window    time.Duration
	kind      string  // e.g. "rotation" or "retention"
	source    string  // alarm "source" field, e.g. "rotation"
	lowAt     float64 // events/sec to raise Low
	highAt    float64 // events/sec to escalate to High (0 disables escalation)
	alerts    alert.Sink
	vaultName func(glid.GLID) string // best-effort human label, "" if unknown
}

// rateAlerterConfig bundles the constructor parameters so RateAlerter
// constructions read clearly at the call site (there are five tunable
// fields and a positional API would be unreadable).
type rateAlerterConfig struct {
	Window    time.Duration
	Kind      string
	Source    string
	LowAt     float64
	HighAt    float64 // 0 = no High escalation
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
		active:    make(map[glid.GLID]alert.Priority),
		window:    cfg.Window,
		kind:      cfg.Kind,
		source:    cfg.Source,
		lowAt:     cfg.LowAt,
		highAt:    cfg.HighAt,
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
	prev, hadActive := r.active[vaultID]
	delete(r.active, vaultID)
	r.mu.Unlock()
	if hadActive && prev != 0 && r.alerts != nil {
		r.alerts.Clear(r.alarmTypeID(), vaultID.String())
	}
}

// Evaluate walks every tracked vault, computes its current rate, and raises
// or clears the alarm as the threshold dictates. Intended to be called on
// a fixed cadence (e.g., every 5 seconds) by a background goroutine.
func (r *RateAlerter) Evaluate(now time.Time) {
	type pending struct {
		vaultID  glid.GLID
		priority alert.Priority // 0 = clear
		rate     float64
		count    int64
	}
	var work []pending

	r.mu.Lock()
	for vaultID, w := range r.windows {
		rate := w.Rate(now)
		count := w.Count(now)
		desired := r.classify(rate)
		prev := r.active[vaultID]
		if desired == prev {
			continue
		}
		r.active[vaultID] = desired
		work = append(work, pending{vaultID: vaultID, priority: desired, rate: rate, count: count})
	}
	r.mu.Unlock()

	if r.alerts == nil {
		return
	}
	for _, p := range work {
		if p.priority == 0 {
			r.alerts.Clear(r.alarmTypeID(), p.vaultID.String())
			continue
		}
		r.alerts.RaiseOperator(alert.OperatorAlarm{
			TypeID:      r.alarmTypeID(),
			InstanceKey: p.vaultID.String(),
			Priority:    p.priority,
			Source:      r.source,
			Detail:      r.message(p.vaultID, p.rate, p.count),
			Cause:       fmt.Sprintf("The vault's %s rate crossed an operator-configured threshold (sustained over a %s window).", r.kind, r.window),
			Response:    fmt.Sprintf("Review the vault's %s policy and the configured threshold — a pathological configuration degrades throughput until corrected.", r.kind),
		})
	}
}

// classify maps a rate to the appropriate alarm priority. Returns 0 to
// indicate "clear / no alarm".
func (r *RateAlerter) classify(rate float64) alert.Priority {
	if r.highAt > 0 && rate >= r.highAt {
		return alert.High
	}
	if rate >= r.lowAt {
		return alert.Low
	}
	return 0
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
