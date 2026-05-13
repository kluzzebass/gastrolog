package logging

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// newTestCaptureHandler constructs a CaptureHandler backed by a tiny
// bounded channel so tests can exercise overflow with few records.
// Returns the handler and the channel (so tests can drain it if needed).
func newTestCaptureHandler(t *testing.T, capSize int) (*CaptureHandler, chan CapturedRecord) {
	t.Helper()
	ch := make(chan CapturedRecord, capSize)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := NewCaptureHandler(inner, ch, nil)
	// Default threshold is Warn — lower so tests can emit Info records.
	h.SetMinCaptureLevel(slog.LevelDebug)
	// Default gate is OFF (gastrolog-6bvu6) — open it so tests that
	// exercise the capture path actually see records on the channel.
	h.SetEnabled(true)
	return h, ch
}

func emitRecord(t *testing.T, h *CaptureHandler, msg string) {
	t.Helper()
	rec := slog.NewRecord(time.Time{}, slog.LevelInfo, msg, 0)
	if err := h.Handle(context.Background(), rec); err != nil {
		t.Fatalf("Handle: %v", err)
	}
}

// TestCaptureHandlerDroppedCountStartsAtZero verifies that a freshly
// constructed CaptureHandler reports zero drops.
func TestCaptureHandlerDroppedCountStartsAtZero(t *testing.T) {
	t.Parallel()
	h, _ := newTestCaptureHandler(t, 4)
	if got := h.DroppedCount(); got != 0 {
		t.Errorf("initial DroppedCount = %d, want 0", got)
	}
}

// TestCaptureHandlerCountsDropsOnOverflow verifies that records rejected
// because the capture channel is full are counted. Uses a channel of
// size 2 and emits 5 records without draining, so 3 should drop.
func TestCaptureHandlerCountsDropsOnOverflow(t *testing.T) {
	t.Parallel()
	h, _ := newTestCaptureHandler(t, 2)

	for range 5 {
		emitRecord(t, h, "overflow")
	}

	if got := h.DroppedCount(); got != 3 {
		t.Errorf("DroppedCount after 5 emits into cap=2 channel = %d, want 3", got)
	}
}

// TestCaptureHandlerNoDropsWhenChannelDrained verifies the counter does
// not increment when the consumer keeps pace with the producer.
func TestCaptureHandlerNoDropsWhenChannelDrained(t *testing.T) {
	t.Parallel()
	h, ch := newTestCaptureHandler(t, 2)

	for range 10 {
		emitRecord(t, h, "normal")
		<-ch // drain as we go
	}

	if got := h.DroppedCount(); got != 0 {
		t.Errorf("DroppedCount with draining consumer = %d, want 0", got)
	}
}

// TestCaptureHandlerWithAttrsClonesShareDroppedCounter verifies that
// handlers returned from WithAttrs share the same drop counter as the
// original — without this, drops through scoped loggers would be
// invisible to the monitor.
func TestCaptureHandlerWithAttrsClonesShareDroppedCounter(t *testing.T) {
	t.Parallel()
	h, _ := newTestCaptureHandler(t, 1)

	scoped := h.WithAttrs([]slog.Attr{slog.String("component", "test")}).(*CaptureHandler)

	// Fill via the original handler...
	emitRecord(t, h, "fill")
	// ...and overflow via the scoped clone.
	emitRecord(t, scoped, "overflow")

	if got := h.DroppedCount(); got != 1 {
		t.Errorf("original DroppedCount = %d, want 1", got)
	}
	if got := scoped.DroppedCount(); got != 1 {
		t.Errorf("scoped DroppedCount = %d, want 1 (shared counter)", got)
	}
}

// TestCaptureHandlerDefaultGateClosed verifies that a freshly constructed
// CaptureHandler does NOT tee records into the channel until SetEnabled(true)
// is called. This is the steady-state behavior when the self ingester is
// disabled — producers must not fill the channel without a consumer.
// See gastrolog-6bvu6.
func TestCaptureHandlerDefaultGateClosed(t *testing.T) {
	t.Parallel()
	ch := make(chan CapturedRecord, 16)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := NewCaptureHandler(inner, ch, nil)
	h.SetMinCaptureLevel(slog.LevelDebug)
	// Note: NOT calling SetEnabled — exercising the default closed state.

	for range 50 {
		emitRecord(t, h, "would-overflow-if-gate-leaked")
	}

	if got := len(ch); got != 0 {
		t.Errorf("len(ch) with gate closed = %d, want 0", got)
	}
	if got := h.DroppedCount(); got != 0 {
		t.Errorf("DroppedCount with gate closed = %d, want 0", got)
	}
	if h.IsEnabled() {
		t.Error("IsEnabled() = true on fresh handler, want false")
	}
}

// TestCaptureHandlerGateToggle verifies that flipping the gate at runtime
// starts and stops the capture path without reinstalling the handler chain.
// Models the self-ingester enable/disable cycle.
func TestCaptureHandlerGateToggle(t *testing.T) {
	t.Parallel()
	ch := make(chan CapturedRecord, 16)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := NewCaptureHandler(inner, ch, nil)
	h.SetMinCaptureLevel(slog.LevelDebug)

	// Gate closed: records discarded.
	emitRecord(t, h, "closed")
	if got := len(ch); got != 0 {
		t.Fatalf("len(ch) before open = %d, want 0", got)
	}

	// Gate open: records flow.
	h.SetEnabled(true)
	emitRecord(t, h, "open")
	if got := len(ch); got != 1 {
		t.Fatalf("len(ch) after open = %d, want 1", got)
	}
	<-ch // drain

	// Gate closed again: records discarded.
	h.SetEnabled(false)
	emitRecord(t, h, "closed-again")
	if got := len(ch); got != 0 {
		t.Errorf("len(ch) after re-close = %d, want 0", got)
	}
}

// TestCaptureHandlerGateSharedAcrossClones verifies that WithAttrs/WithGroup
// clones share the gate atomic with the parent — toggling on the parent
// must take effect on scoped loggers too, and vice versa. Without this,
// component-tagged loggers would have a stale gate.
func TestCaptureHandlerGateSharedAcrossClones(t *testing.T) {
	t.Parallel()
	ch := make(chan CapturedRecord, 16)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := NewCaptureHandler(inner, ch, nil)
	h.SetMinCaptureLevel(slog.LevelDebug)

	scoped := h.WithAttrs([]slog.Attr{slog.String("k", "v")}).(*CaptureHandler)
	grouped := h.WithGroup("g").(*CaptureHandler)

	// All gates start closed.
	emitRecord(t, scoped, "scoped-closed")
	emitRecord(t, grouped, "grouped-closed")
	if got := len(ch); got != 0 {
		t.Fatalf("len(ch) with parent gate closed = %d, want 0", got)
	}

	// Open via the parent — clones must observe the change.
	h.SetEnabled(true)
	if !scoped.IsEnabled() {
		t.Error("scoped clone reports gate closed after parent opened")
	}
	if !grouped.IsEnabled() {
		t.Error("grouped clone reports gate closed after parent opened")
	}
	emitRecord(t, scoped, "scoped-open")
	emitRecord(t, grouped, "grouped-open")
	if got := len(ch); got != 2 {
		t.Errorf("len(ch) after parent opened = %d, want 2", got)
	}
}

// TestCaptureHandlerSkipComponentNotCounted verifies that records from
// skip-listed components (pipeline-internal goroutines) are never sent
// to the channel and therefore never contribute to the drop count, even
// when the channel is full.
func TestCaptureHandlerSkipComponentNotCounted(t *testing.T) {
	t.Parallel()
	ch := make(chan CapturedRecord, 1)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	h := NewCaptureHandler(inner, ch, []string{"chunk"})
	h.SetMinCaptureLevel(slog.LevelDebug)
	h.SetEnabled(true)

	scoped := h.WithAttrs([]slog.Attr{slog.String("component", "chunk")}).(*CaptureHandler)

	// Fill the channel so any further sends would drop.
	rec := slog.NewRecord(time.Time{}, slog.LevelInfo, "filler", 0)
	_ = h.Handle(context.Background(), rec)

	// Emit many records from the skip-listed component. They should
	// never even attempt to send (skipped before the send/default
	// branch), so they don't count as drops.
	for range 100 {
		_ = scoped.Handle(context.Background(), rec)
	}

	if got := h.DroppedCount(); got != 0 {
		t.Errorf("DroppedCount with skip-listed component = %d, want 0", got)
	}
}
