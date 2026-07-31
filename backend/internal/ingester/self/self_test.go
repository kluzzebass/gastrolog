package self

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/pipeline/ingestion"
)

func TestSelfIngesterEmit(t *testing.T) {
	ch := make(chan logging.CapturedRecord, 64)
	capture := logging.NewCaptureHandler(slog.Default().Handler(), ch, nil)
	capture.SetMinCaptureLevel(slog.LevelInfo)

	factory := NewFactory(ch, capture)
	ing, err := factory([16]byte{1}, nil, nil)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	out := make(chan ingestion.IngesterMessage, 10)

	go func() { _ = ing.Run(t.Context(), out) }()

	// Feed a captured record.
	rec := slog.NewRecord(time.Now(), slog.LevelWarn, "test log message", 0)
	rec.AddAttrs(slog.String("component", "test"))
	ch <- logging.CapturedRecord{Record: rec}

	select {
	case msg := <-out:
		if msg.Attrs["level"] != "warn" {
			t.Errorf("expected level=warn, got %q", msg.Attrs["level"])
		}
		if msg.Attrs["ingester_type"] != "self" {
			t.Errorf("expected ingester_type=self, got %q", msg.Attrs["ingester_type"])
		}
		if len(msg.Raw) == 0 {
			t.Error("expected non-empty JSON body")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ingester to emit a message")
	}
}

// TestSelfIngesterRunOpensAndClosesCaptureGate verifies that the self
// ingester's Run loop flips the CaptureHandler gate on at start and off
// at teardown. This is the contract that keeps the capture channel
// empty when the self ingester is disabled.
func TestSelfIngesterRunOpensAndClosesCaptureGate(t *testing.T) {
	ch := make(chan logging.CapturedRecord, 4)
	inner := slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})
	capture := logging.NewCaptureHandler(inner, ch, nil)

	if capture.IsEnabled() {
		t.Fatal("CaptureHandler reports enabled before Run started")
	}

	factory := NewFactory(ch, capture)
	ing, err := factory([16]byte{1}, nil, nil)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	out := make(chan ingestion.IngesterMessage, 1)
	done := make(chan struct{})
	go func() {
		_ = ing.Run(ctx, out)
		close(done)
	}()

	// Wait for Run to open the gate. Run sets it immediately as its
	// first action, but goroutine scheduling means we poll briefly.
	deadline := time.Now().Add(2 * time.Second)
	for !capture.IsEnabled() {
		if time.Now().After(deadline) {
			t.Fatal("CaptureHandler still disabled after Run started")
		}
		time.Sleep(5 * time.Millisecond)
	}

	cancel()
	<-done

	if capture.IsEnabled() {
		t.Error("CaptureHandler still enabled after Run returned")
	}
}
