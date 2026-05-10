package orchestrator

import (
	"fmt"
	"gastrolog/internal/glid"
	"sync"
	"time"

	"gastrolog/internal/alert"
)

// RateAlerter tracks per-vault event rates over a sliding window and raises
// or clears alerts when sustained rates exceed configured thresholds. It is
// the mechanism behind gastrolog-47qyw: detecting and surfacing pathological
// rotation or retention configurations as operator-visible signals rather
// than silent throughput collapse.
//
// The alerter owns one RateWindow per vault and looks up vault names through
// an injected callback (so it doesn't need to know about the orchestrator's
// vault registry directly). Alert IDs are stable strings of the form
// "<kind>-rate:<vaultID>", so each vault has an independent Set/Clear pair.
//
// Hysteresis: warningAt and errorAt are escalation thresholds. The alert
// only clears when the observed rate drops back to below warningAt — there
// is no separate "clear at X" knob, because the rate window itself smooths
// over short bursts (a 30s window of 30 events at instant t is still 1/sec
// at instant t+15 even if no new events arrive). This naturally prevents
// flapping at the threshold.
type RateAlerter struct {
	mu      sync.Mutex
	windows map[glid.GLID]*RateWindow
	// active tracks the last severity we raised for each vault so Evaluate
	// can decide whether the alert state changed.
	active map[glid.GLID]alert.Severity

	window    time.Duration
	kind      string  // e.g. "rotation" or "retention"
	source    string  // alert "source" field, e.g. "rotation"
	warningAt float64 // events/sec to raise Warning
	errorAt   float64 // events/sec to raise Error (0 disables Error escalation)
	alerts    AlertCollector
	vaultName func(glid.GLID) string // best-effort human label, "" if unknown
}

// rateAlerterConfig bundles the constructor parameters so RateAlerter
// constructions read clearly at the call site (there are five tunable
// fields and a positional API would be unreadable).
type rateAlerterConfig struct {
	Window    time.Duration
	Kind      string
	Source    string
	WarningAt float64
	ErrorAt   float64 // 0 = no error escalation
	Alerts    AlertCollector
	VaultName func(glid.GLID) string
}

// newRateAlerter constructs a RateAlerter. vaultName may be nil; if provided,
// it returns a human label for the vault (e.g., the operator's chosen vault
// name from config) and is invoked under no locks so it must be safe to call
// concurrently.
func newRateAlerter(cfg rateAlerterConfig) *RateAlerter {
	return &RateAlerter{
		windows:   make(map[glid.GLID]*RateWindow),
		active:    make(map[glid.GLID]alert.Severity),
		window:    cfg.Window,
		kind:      cfg.Kind,
		source:    cfg.Source,
		warningAt: cfg.WarningAt,
		errorAt:   cfg.ErrorAt,
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

// Forget removes a vault's tracking and clears any active alert for it.
// Call this when a vault is removed from the orchestrator.
func (r *RateAlerter) Forget(vaultID glid.GLID) {
	r.mu.Lock()
	delete(r.windows, vaultID)
	prev, hadActive := r.active[vaultID]
	delete(r.active, vaultID)
	r.mu.Unlock()
	if hadActive && prev != 0 && r.alerts != nil {
		r.alerts.Clear(r.alertID(vaultID))
	}
}

// Evaluate walks every tracked vault, computes its current rate, and raises
// or clears the alert as the threshold dictates. Intended to be called on
// a fixed cadence (e.g., every 5 seconds) by a background goroutine.
func (r *RateAlerter) Evaluate(now time.Time) {
	type pending struct {
		vaultID  glid.GLID
		severity alert.Severity // 0 = clear
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
		work = append(work, pending{vaultID: vaultID, severity: desired, rate: rate, count: count})
	}
	r.mu.Unlock()

	if r.alerts == nil {
		return
	}
	for _, p := range work {
		if p.severity == 0 {
			r.alerts.Clear(r.alertID(p.vaultID))
			continue
		}
		r.alerts.Set(
			r.alertID(p.vaultID),
			p.severity,
			r.source,
			r.message(p.vaultID, p.rate, p.count),
		)
	}
}

// classify maps a rate to the appropriate alert severity. Returns 0 to
// indicate "clear / no alert".
func (r *RateAlerter) classify(rate float64) alert.Severity {
	if r.errorAt > 0 && rate >= r.errorAt {
		return alert.Error
	}
	if rate >= r.warningAt {
		return alert.Warning
	}
	return 0
}

func (r *RateAlerter) alertID(vaultID glid.GLID) string {
	return fmt.Sprintf("%s-rate:%s", r.kind, vaultID)
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
