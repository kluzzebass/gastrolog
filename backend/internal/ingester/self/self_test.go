package self

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
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

	out := make(chan orchestrator.IngestMessage, 10)

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
