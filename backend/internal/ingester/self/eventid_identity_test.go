package self

import (
	"log/slog"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// TestEventIDIdentity pins gastrolog-44b9r for the self ingester.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	ch := make(chan logging.CapturedRecord, 4)
	capture := logging.NewCaptureHandler(slog.Default().Handler(), ch, nil)
	capture.SetMinCaptureLevel(slog.LevelInfo)

	id := glid.New()
	factory := NewFactory(ch, capture, nil)
	ing, err := factory(id, nil, nil)
	if err != nil {
		t.Fatalf("factory: %v", err)
	}

	out := make(chan orchestrator.IngestMessage, 4)
	go func() { _ = ing.Run(t.Context(), out) }()

	rec := slog.NewRecord(time.Now(), slog.LevelWarn, "identity probe", 0)
	ch <- logging.CapturedRecord{Record: rec}

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, id.String())
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for self message")
	}
}
