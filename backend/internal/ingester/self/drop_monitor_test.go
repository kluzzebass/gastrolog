package self

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"gastrolog/internal/alert"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"

	"github.com/google/uuid"
)

// fakeAlerts is a local mock implementing orchestrator.AlertCollector.
// Separate from the orchestrator package's fakeAlerts since the
// self-ingester can't import orchestrator's test code.
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

// newTestIngester constructs a self-ingester with a tiny capture channel
// so drops are trivial to trigger, plus a wired AlertCollector. Returns
// the ingester, the CaptureHandler, and the AlertCollector mock.
func newTestIngester(t *testing.T) (*ingester, *logging.CaptureHandler, *fakeAlerts) {
	t.Helper()
	ch := make(chan logging.CapturedRecord, 1)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	capture := logging.NewCaptureHandler(inner, ch, nil)
	capture.SetMinCaptureLevel(slog.LevelDebug)

	alerts := &fakeAlerts{}
	return &ingester{
		id:        uuid.New().String(),
		ch:        ch,
		logger:    slog.New(inner),
		capture:   capture,
		baseLevel: slog.LevelDebug,
		alerts:    alerts,
	}, capture, alerts
}

// emitToCapture pushes a record through the CaptureHandler so the drop
// counter increments if the channel is full.
func emitToCapture(t *testing.T, h *logging.CaptureHandler, msg string) {
	t.Helper()
	rec := slog.NewRecord(time.Time{}, slog.LevelInfo, msg, 0)
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestDropMonitorRaisesAlertOnDrops verifies that evaluateDrops fires
// the "self-ingester-drops" alert when the capture handler's counter
// has advanced since the last poll.
func TestDropMonitorRaisesAlertOnDrops(t *testing.T) {
	t.Parallel()
	ing, capture, alerts := newTestIngester(t)

	// Fill the channel (cap=1) and overflow it a few times. Drop count
	// should be 3 (first emit fills the slot, next 3 drop).
	emitToCapture(t, capture, "fill")
	for range 3 {
		emitToCapture(t, capture, "overflow")
	}
	if got := capture.DroppedCount(); got != 3 {
		t.Fatalf("setup: DroppedCount = %d, want 3", got)
	}

	ing.evaluateDrops(0, false, 0)

	calls := alerts.snapshot()
	if len(calls) != 1 {
		t.Fatalf("expected 1 alert call, got %d: %v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[0].severity != alert.Warning {
		t.Errorf("expected Warning Set, got %+v", calls[0])
	}
	if calls[0].id != dropAlertID {
		t.Errorf("alert ID = %q, want %q", calls[0].id, dropAlertID)
	}
}

// TestDropMonitorDoesNotRaiseWithoutDrops verifies that evaluateDrops
// stays silent when the drop counter has not advanced.
func TestDropMonitorDoesNotRaiseWithoutDrops(t *testing.T) {
	t.Parallel()
	ing, _, alerts := newTestIngester(t)

	ing.evaluateDrops(0, false, 0)
	ing.evaluateDrops(0, false, 0)
	ing.evaluateDrops(0, false, 0)

	if got := len(alerts.snapshot()); got != 0 {
		t.Errorf("expected no alerts when no drops, got %d", got)
	}
}

// TestDropMonitorClearsAfterDwell verifies that once the drop counter
// goes quiet, the alert clears after dropAlertClearAfter ticks.
func TestDropMonitorClearsAfterDwell(t *testing.T) {
	t.Parallel()
	ing, capture, alerts := newTestIngester(t)

	// Cause a drop to arm the alert.
	emitToCapture(t, capture, "fill")
	emitToCapture(t, capture, "overflow")
	lastSeen, alertActive, ticksSinceLastDrop := ing.evaluateDrops(0, false, 0)
	if !alertActive {
		t.Fatal("expected alertActive=true after drop")
	}

	// Advance dropAlertClearAfter-1 ticks with zero new drops. The
	// alert should still be active.
	for range dropAlertClearAfter - 1 {
		lastSeen, alertActive, ticksSinceLastDrop = ing.evaluateDrops(lastSeen, alertActive, ticksSinceLastDrop)
		if !alertActive {
			t.Errorf("alert cleared too early at tick %d", ticksSinceLastDrop)
		}
	}

	// One more zero-delta tick should clear it.
	_, alertActive, _ = ing.evaluateDrops(lastSeen, alertActive, ticksSinceLastDrop)
	if alertActive {
		t.Error("expected alert to clear after dwell")
	}

	calls := alerts.snapshot()
	if len(calls) != 2 {
		t.Fatalf("expected [set, clear], got %+v", calls)
	}
	if calls[1].op != "clear" {
		t.Errorf("expected final call to be clear, got %+v", calls[1])
	}
}

// TestDropMonitorReArmsAfterClear verifies that a new drop after the
// alert cleared raises the alert again.
func TestDropMonitorReArmsAfterClear(t *testing.T) {
	t.Parallel()
	ing, capture, alerts := newTestIngester(t)

	// First drop → alert set.
	emitToCapture(t, capture, "fill")
	emitToCapture(t, capture, "overflow-1")
	lastSeen, alertActive, ticksSinceLastDrop := ing.evaluateDrops(0, false, 0)

	// Dwell ticks → alert clear.
	for range dropAlertClearAfter {
		lastSeen, alertActive, ticksSinceLastDrop = ing.evaluateDrops(lastSeen, alertActive, ticksSinceLastDrop)
	}
	if alertActive {
		t.Fatal("setup: expected alert cleared before re-arm test")
	}

	// A fresh drop should re-arm the alert on the next tick.
	emitToCapture(t, capture, "overflow-2")
	_, alertActive, _ = ing.evaluateDrops(lastSeen, alertActive, ticksSinceLastDrop)
	if !alertActive {
		t.Error("expected alert to re-arm after new drop")
	}

	// Should be: set, clear, set.
	calls := alerts.snapshot()
	if len(calls) != 3 {
		t.Fatalf("expected [set, clear, set], got %d calls: %+v", len(calls), calls)
	}
	if calls[0].op != "set" || calls[1].op != "clear" || calls[2].op != "set" {
		t.Errorf("call sequence wrong: %+v", calls)
	}
}

// TestDropMonitorNilGuardedByRun verifies that when either capture or
// alerts is nil, Run does not start a ticker and evaluateDrops is never
// called (it would panic on the nil capture).
func TestDropMonitorNilGuardedByRun(t *testing.T) {
	t.Parallel()

	// Nil alerts: start Run with a real capture but no alerts, verify
	// it doesn't crash on ticker events.
	ch := make(chan logging.CapturedRecord, 1)
	inner := slog.NewTextHandler(io.Discard, nil)
	capture := logging.NewCaptureHandler(inner, ch, nil)
	ing := &ingester{
		id:        uuid.New().String(),
		ch:        ch,
		logger:    slog.New(inner),
		capture:   capture,
		baseLevel: slog.LevelDebug,
		alerts:    nil, // explicitly nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan orchestrator.IngestMessage, 1)
	go func() { _ = ing.Run(ctx, out) }()

	time.Sleep(20 * time.Millisecond)
	cancel()
}
