package orchestrator

import (
	"strings"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/glid"
)

// fakeAlerts captures Raise/Clear calls for assertion. Implements
// alert.Sink. The sink carries no priority — that is the point: the
// alerter raises the raw condition and the catalog owns the verdict.
type fakeAlerts struct {
	mu    sync.Mutex
	calls []alertCall
}

type alertCall struct {
	op      string // "set" or "clear"
	id      string
	message string
}

func (f *fakeAlerts) Raise(typeID, instanceKey, detail string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alertCall{op: "set", id: fakeAlarmID(typeID, instanceKey), message: detail})
}

func (f *fakeAlerts) Clear(typeID, instanceKey string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alertCall{op: "clear", id: fakeAlarmID(typeID, instanceKey)})
}

// fakeAlarmID mirrors the collector's type:instance ID composition.
func fakeAlarmID(typeID, instanceKey string) string {
	if instanceKey == "" {
		return typeID
	}
	return typeID + ":" + instanceKey
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

func newTestRateAlerter(alerts alert.Sink) *RateAlerter {
	return newRateAlerter(rateAlerterConfig{
		Window:    10 * time.Second,
		Kind:      "retention",
		Threshold: 1.0, // >= 10 events in 10s
		Alerts:    alerts,
		VaultName: func(id glid.GLID) string { return "test-vault-" + id.String()[:4] },
	})
}

func TestRateAlerterStaysSilentBelowThreshold(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// 5 events in 10s = 0.5/s, below the 1.0 threshold.
	for i := range 5 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	if len(alerts.snapshot()) != 0 {
		t.Errorf("expected no alerts, got %v", alerts.snapshot())
	}
}

func TestRateAlerterRaisesAtThreshold(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// 10 events in 10s = exactly 1.0/s (the threshold).
	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert call, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" {
		t.Errorf("expected raise, got %+v", calls[0])
	}
	if want := "retention-rate:" + vaultID.String(); calls[0].id != want {
		t.Errorf("alarm ID = %q, want %q", calls[0].id, want)
	}
	if !strings.Contains(calls[0].message, "retention rate") {
		t.Errorf("detail %q missing the rate description", calls[0].message)
	}
}

// TestRateAlerterCatalogOwnsPriority pins gastrolog-1cruar end to end: the
// alerter raises through the ordinary catalog path, so a real collector
// stamps retention-rate with the catalog's Low verdict — the alerter never
// chooses a priority.
func TestRateAlerterCatalogOwnsPriority(t *testing.T) {
	t.Parallel()
	collector := alert.NewWithClock(func() time.Time { return baseTime })
	ra := newTestRateAlerter(collector)
	vaultID := glid.New()

	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	active := collector.Standing()
	if len(active) != 1 {
		t.Fatalf("expected 1 active alarm, got %d: %v", len(active), active)
	}
	a := active[0]
	if a.ID != "retention-rate:"+vaultID.String() {
		t.Errorf("alarm ID = %q", a.ID)
	}
	if a.Priority != alert.Low {
		t.Errorf("priority = %v, want Low (stamped from the catalog)", a.Priority)
	}
	if a.Source != "retention" || a.Cause == "" || a.Response == "" {
		t.Errorf("catalog fields not stamped: %+v", a)
	}
}

func TestRateAlerterClearsWhenRateDrops(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// Cross threshold.
	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
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
	vaultID := glid.New()

	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	// Evaluate three times at the same instant — should produce only one Set.
	for range 3 {
		ra.Evaluate(baseTime.Add(9 * time.Second))
	}

	if got := len(alerts.snapshot()); got != 1 {
		t.Errorf("expected 1 alert (idempotent), got %d", got)
	}
}

func TestRateAlerterNoReRaiseWhileRateClimbs(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// Cross the threshold.
	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	// The rate climbing further while the alarm stands is the same
	// condition — no escalation, no second raise (the catalog's verdict
	// does not change with the rate).
	for i := range 40 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i%10)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected a single raise while the condition stands, got %v", calls)
	}
}

func TestRateAlerterPerInstanceIndependence(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultA := glid.New()
	vaultB := glid.New()

	// Vault A crosses threshold; vault B stays below.
	for i := range 10 {
		ra.Record(vaultA, baseTime.Add(time.Duration(i)*time.Second))
	}
	for range 3 {
		ra.Record(vaultB, baseTime)
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert (only vaultA), got %v", calls)
	}
	if want := ra.alarmTypeID() + ":" + vaultA.String(); calls[0].id != want {
		t.Errorf("wrong vaultInst alerted: got id %q, want %q", calls[0].id, want)
	}
}

func TestRateAlerterForgetClearsActiveAlert(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))
	alerts.reset()

	ra.Forget(vaultID)

	calls := alerts.snapshot()
	if len(calls) != 1 || calls[0].op != "clear" {
		t.Errorf("expected single Clear after Forget, got %v", calls)
	}
}

func TestRateAlerterForgetWithoutActiveDoesNotClear(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// Record a small number that doesn't trip the threshold.
	for range 3 {
		ra.Record(vaultID, baseTime)
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))
	alerts.reset()

	ra.Forget(vaultID)

	if got := len(alerts.snapshot()); got != 0 {
		t.Errorf("expected no Clear (alert was never active), got %d calls", got)
	}
}
