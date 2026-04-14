package orchestrator

import (
	"gastrolog/internal/glid"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"

)

// fakeAlerts captures Set/Clear calls for assertion. Implements
// AlertCollector.
type fakeAlerts struct {
	mu    sync.Mutex
	calls []alertCall
}

type alertCall struct {
	op       string // "set" or "clear"
	id       string
	severity alert.Severity
	source   string
	message  string
}

func (f *fakeAlerts) Set(id string, severity alert.Severity, source, message string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alertCall{op: "set", id: id, severity: severity, source: source, message: message})
}

func (f *fakeAlerts) Clear(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alertCall{op: "clear", id: id})
}

func (f *fakeAlerts) snapshot() []alertCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]alertCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeAlerts) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func newTestRateAlerter(alerts AlertCollector) *RateAlerter {
	return newRateAlerter(rateAlerterConfig{
		Window:    10 * time.Second,
		Kind:      "rotation",
		Source:    "rotation",
		WarningAt: 1.0, // >= 10 events in 10s
		ErrorAt:   5.0, // >= 50 events in 10s
		Alerts:    alerts,
		TierName:  func(id glid.GLID) string { return "test-tier-" + id.String()[:4] },
	})
}

func TestRateAlerterStaysSilentBelowThreshold(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	// 5 events in 10s = 0.5/s, below the 1.0 warning threshold.
	for i := range 5 {
		ra.Record(tierID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	if len(alerts.snapshot()) != 0 {
		t.Errorf("expected no alerts, got %v", alerts.snapshot())
	}
}

func TestRateAlerterRaisesWarningAtThreshold(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	// 10 events in 10s = exactly 1.0/s (the warning threshold).
	for i := range 10 {
		ra.Record(tierID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert call, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[0].severity != alert.Warning {
		t.Errorf("expected Warning Set, got %+v", calls[0])
	}
	if calls[0].id == "" {
		t.Error("alert ID empty")
	}
}

func TestRateAlerterEscalatesToError(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	// 50 events in 10s = 5.0/s (exactly the error threshold).
	for i := range 50 {
		ra.Record(tierID, baseTime.Add(time.Duration(i%10)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 || calls[0].severity != alert.Error {
		t.Errorf("expected single Error alert, got %v", calls)
	}
}

func TestRateAlerterClearsWhenRateDrops(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	// Cross threshold.
	for i := range 10 {
		ra.Record(tierID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))
	if len(alerts.snapshot()) != 1 {
		t.Fatalf("setup: expected raise, got %v", alerts.snapshot())
	}

	// Move time well past the window — events have aged out, rate = 0.
	ra.Evaluate(baseTime.Add(60 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected raise + clear, got %v", calls)
	}
	if calls[1].op != "clear" {
		t.Errorf("expected clear, got %+v", calls[1])
	}
}

func TestRateAlerterIdempotentRepeatedEvaluations(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	for i := range 10 {
		ra.Record(tierID, baseTime.Add(time.Duration(i)*time.Second))
	}
	// Evaluate three times at the same instant — should produce only one Set.
	for range 3 {
		ra.Evaluate(baseTime.Add(9 * time.Second))
	}

	if got := len(alerts.snapshot()); got != 1 {
		t.Errorf("expected 1 alert (idempotent), got %d", got)
	}
}

func TestRateAlerterTransitionsWarningToErrorEmitsResetSet(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	// First, push into warning territory.
	for i := range 10 {
		ra.Record(tierID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	// Now push to error territory in the same buckets.
	for i := range 40 {
		ra.Record(tierID, baseTime.Add(time.Duration(i%10)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected warning then error, got %v", calls)
	}
	if calls[0].severity != alert.Warning || calls[1].severity != alert.Error {
		t.Errorf("severity sequence wrong: %v", calls)
	}
}

func TestRateAlerterPerTierIndependence(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tier1 := glid.New()
	tier2 := glid.New()

	// Tier 1 crosses threshold; tier 2 stays below.
	for i := range 10 {
		ra.Record(tier1, baseTime.Add(time.Duration(i)*time.Second))
	}
	for range 3 {
		ra.Record(tier2, baseTime)
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert (only tier1), got %v", calls)
	}
	if calls[0].id != ra.alertID(tier1) {
		t.Errorf("wrong tier alerted: got id %q, want %q", calls[0].id, ra.alertID(tier1))
	}
}

func TestRateAlerterForgetClearsActiveAlert(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	for i := range 10 {
		ra.Record(tierID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))
	alerts.reset()

	ra.Forget(tierID)

	calls := alerts.snapshot()
	if len(calls) != 1 || calls[0].op != "clear" {
		t.Errorf("expected single Clear after Forget, got %v", calls)
	}
}

func TestRateAlerterForgetWithoutActiveDoesNotClear(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	tierID := glid.New()

	// Record a small number that doesn't trip the threshold.
	for range 3 {
		ra.Record(tierID, baseTime)
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))
	alerts.reset()

	ra.Forget(tierID)

	if got := len(alerts.snapshot()); got != 0 {
		t.Errorf("expected no Clear (alert was never active), got %d calls", got)
	}
}
