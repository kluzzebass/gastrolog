package orchestrator

import (
	"gastrolog/internal/glid"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
)

// fakeAlerts captures Raise/Clear calls for assertion. Implements
// alert.Sink.
type fakeAlerts struct {
	mu    sync.Mutex
	calls []alertCall
}

type alertCall struct {
	op       string // "set" or "clear"
	id       string
	priority alert.Priority
	source   string
	message  string
	cause    string
	response string
}

func (f *fakeAlerts) Raise(typeID, instanceKey, detail string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alertCall{op: "set", id: fakeAlarmID(typeID, instanceKey), message: detail})
}

func (f *fakeAlerts) RaiseOperator(a alert.OperatorAlarm) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, alertCall{
		op: "set", id: fakeAlarmID(a.TypeID, a.InstanceKey),
		priority: a.Priority, source: a.Source, message: a.Detail,
		cause: a.Cause, response: a.Response,
	})
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
		Kind:      "rotation",
		Source:    "rotation",
		LowAt:     1.0, // >= 10 events in 10s
		HighAt:    5.0, // >= 50 events in 10s
		Alerts:    alerts,
		VaultName: func(id glid.GLID) string { return "test-vault-" + id.String()[:4] },
	})
}

func TestRateAlerterStaysSilentBelowThreshold(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// 5 events in 10s = 0.5/s, below the 1.0 warning threshold.
	for i := range 5 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
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
	vaultID := glid.New()

	// 10 events in 10s = exactly 1.0/s (the warning threshold).
	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert call, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[0].priority != alert.Low {
		t.Errorf("expected Low raise, got %+v", calls[0])
	}
	if calls[0].id == "" {
		t.Error("alert ID empty")
	}
}

func TestRateAlerterEscalatesToError(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// 50 events in 10s = 5.0/s (exactly the error threshold).
	for i := range 50 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i%10)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 1 || calls[0].priority != alert.High {
		t.Errorf("expected single High alarm, got %v", calls)
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

func TestRateAlerterTransitionsWarningToErrorEmitsResetSet(t *testing.T) {
	t.Parallel()
	alerts := &fakeAlerts{}
	ra := newTestRateAlerter(alerts)
	vaultID := glid.New()

	// First, push into warning territory.
	for i := range 10 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	// Now push to error territory in the same buckets.
	for i := range 40 {
		ra.Record(vaultID, baseTime.Add(time.Duration(i%10)*time.Second))
	}
	ra.Evaluate(baseTime.Add(9 * time.Second))

	calls := alerts.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected warning then error, got %v", calls)
	}
	if calls[0].priority != alert.Low || calls[1].priority != alert.High {
		t.Errorf("priority sequence wrong: %v", calls)
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
