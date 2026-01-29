package logging

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func TestDiscard(t *testing.T) {
	logger := Discard()
	if logger == nil {
		t.Fatal("Discard() returned nil")
	}

	// Should not panic when logging.
	logger.Info("test message")
	logger.Debug("debug message")
}

func TestDefault(t *testing.T) {
	t.Run("nil returns discard", func(t *testing.T) {
		logger := Default(nil)
		if logger == nil {
			t.Fatal("Default(nil) returned nil")
		}
		// Verify it's a discard logger by checking Enabled returns false.
		if logger.Enabled(context.Background(), slog.LevelInfo) {
			t.Error("Default(nil) should return a discard logger")
		}
	})

	t.Run("non-nil returns same logger", func(t *testing.T) {
		var buf bytes.Buffer
		original := slog.New(slog.NewTextHandler(&buf, nil))
		result := Default(original)
		if result != original {
			t.Error("Default should return the same logger when non-nil")
		}
	})
}

// captureHandler captures log records for testing.
// Uses a shared records pointer so WithAttrs clones share the same storage.
type captureHandler struct {
	mu      *sync.Mutex
	records *[]slog.Record
	attrs   []slog.Attr
}

func newCaptureHandler() *captureHandler {
	var mu sync.Mutex
	var records []slog.Record
	return &captureHandler{
		mu:      &mu,
		records: &records,
	}
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	*h.records = append(*h.records, r)
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	newAttrs := make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	copy(newAttrs[len(h.attrs):], attrs)
	return &captureHandler{
		mu:      h.mu,
		records: h.records, // Share the same records slice.
		attrs:   newAttrs,
	}
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return h
}

func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(*h.records)
}

func TestComponentFilterHandler_BasicFiltering(t *testing.T) {
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	// INFO should pass through (at default level).
	logger.Info("info message", "component", "test")
	if capture.count() != 1 {
		t.Errorf("expected 1 record, got %d", capture.count())
	}

	// DEBUG should be filtered (below default INFO level).
	logger.Debug("debug message", "component", "test")
	if capture.count() != 1 {
		t.Errorf("expected 1 record (debug filtered), got %d", capture.count())
	}

	// WARN should pass through.
	logger.Warn("warn message", "component", "test")
	if capture.count() != 2 {
		t.Errorf("expected 2 records, got %d", capture.count())
	}
}

func TestComponentFilterHandler_SetLevel(t *testing.T) {
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	// DEBUG should be filtered initially.
	logger.Debug("debug message", "component", "orchestrator")
	if capture.count() != 0 {
		t.Errorf("expected 0 records (debug filtered), got %d", capture.count())
	}

	// Enable DEBUG for orchestrator.
	filter.SetLevel("orchestrator", slog.LevelDebug)

	// DEBUG should now pass through for orchestrator.
	logger.Debug("debug message", "component", "orchestrator")
	if capture.count() != 1 {
		t.Errorf("expected 1 record, got %d", capture.count())
	}

	// DEBUG should still be filtered for other components.
	logger.Debug("debug message", "component", "query-engine")
	if capture.count() != 1 {
		t.Errorf("expected 1 record (other component filtered), got %d", capture.count())
	}
}

func TestComponentFilterHandler_ClearLevel(t *testing.T) {
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	// Enable DEBUG for orchestrator.
	filter.SetLevel("orchestrator", slog.LevelDebug)

	// DEBUG should pass through.
	logger.Debug("debug message", "component", "orchestrator")
	if capture.count() != 1 {
		t.Errorf("expected 1 record, got %d", capture.count())
	}

	// Clear the level.
	filter.ClearLevel("orchestrator")

	// DEBUG should now be filtered again.
	logger.Debug("debug message", "component", "orchestrator")
	if capture.count() != 1 {
		t.Errorf("expected 1 record (debug filtered after clear), got %d", capture.count())
	}
}

func TestComponentFilterHandler_Level(t *testing.T) {
	filter := NewComponentFilterHandler(nil, slog.LevelInfo)

	// Default level for unknown component.
	if level := filter.Level("unknown"); level != slog.LevelInfo {
		t.Errorf("expected INFO, got %v", level)
	}

	// Set and check level.
	filter.SetLevel("orchestrator", slog.LevelDebug)
	if level := filter.Level("orchestrator"); level != slog.LevelDebug {
		t.Errorf("expected DEBUG, got %v", level)
	}

	// DefaultLevel should always return the configured default.
	if level := filter.DefaultLevel(); level != slog.LevelInfo {
		t.Errorf("expected INFO, got %v", level)
	}
}

func TestComponentFilterHandler_WithAttrs(t *testing.T) {
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)

	// Create a logger with component attribute pre-set.
	logger := slog.New(filter).With("component", "orchestrator")

	// Enable DEBUG for orchestrator.
	filter.SetLevel("orchestrator", slog.LevelDebug)

	// DEBUG should pass through because component is in preAttrs.
	logger.Debug("debug message")
	if capture.count() != 1 {
		t.Errorf("expected 1 record, got %d", capture.count())
	}
}

func TestComponentFilterHandler_NoComponent(t *testing.T) {
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	// Log without component attribute - should use default level.
	logger.Info("info message")
	if capture.count() != 1 {
		t.Errorf("expected 1 record, got %d", capture.count())
	}

	logger.Debug("debug message")
	if capture.count() != 1 {
		t.Errorf("expected 1 record (debug filtered), got %d", capture.count())
	}
}

func TestComponentFilterHandler_Concurrent(t *testing.T) {
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	var wg sync.WaitGroup
	const goroutines = 10
	const iterations = 100

	// Concurrent logging.
	for i := 0; i < goroutines; i++ {
		wg.Go(func() {
			for j := 0; j < iterations; j++ {
				logger.Info("message", "component", "test")
			}
		})
	}

	// Concurrent level changes.
	for i := 0; i < goroutines; i++ {
		wg.Go(func() {
			for j := 0; j < iterations; j++ {
				filter.SetLevel("test", slog.LevelDebug)
				filter.ClearLevel("test")
			}
		})
	}

	wg.Wait()

	// All INFO logs should have been captured.
	if count := capture.count(); count != goroutines*iterations {
		t.Errorf("expected %d records, got %d", goroutines*iterations, count)
	}
}

func TestComponentFilterHandler_Integration(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	filter := NewComponentFilterHandler(base, slog.LevelInfo)
	logger := slog.New(filter)

	// Create component-scoped loggers (as components would do).
	orchLogger := logger.With("component", "orchestrator")
	queryLogger := logger.With("component", "query-engine")

	// Initially both are at INFO level.
	orchLogger.Debug("orch debug 1")
	queryLogger.Debug("query debug 1")
	if buf.Len() != 0 {
		t.Errorf("expected no output, got: %s", buf.String())
	}

	// Enable DEBUG for orchestrator only.
	filter.SetLevel("orchestrator", slog.LevelDebug)

	orchLogger.Debug("orch debug 2")
	queryLogger.Debug("query debug 2")

	output := buf.String()
	if !strings.Contains(output, "orch debug 2") {
		t.Errorf("expected orchestrator debug log, got: %s", output)
	}
	if strings.Contains(output, "query debug") {
		t.Errorf("did not expect query-engine debug log, got: %s", output)
	}
}

func TestComponentFilterHandler_WithGroup(t *testing.T) {
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)

	// WithGroup should return a new handler that still filters.
	grouped := filter.WithGroup("mygroup")
	logger := slog.New(grouped)

	logger.Info("info message", "component", "test")
	if capture.count() != 1 {
		t.Errorf("expected 1 record, got %d", capture.count())
	}

	logger.Debug("debug message", "component", "test")
	if capture.count() != 1 {
		t.Errorf("expected 1 record (debug filtered), got %d", capture.count())
	}
}

func TestComponentFilterHandler_ClearLevelNonExistent(t *testing.T) {
	filter := NewComponentFilterHandler(nil, slog.LevelInfo)

	// Should not panic when clearing non-existent level.
	filter.ClearLevel("nonexistent")

	// Level should still be default.
	if level := filter.Level("nonexistent"); level != slog.LevelInfo {
		t.Errorf("expected INFO, got %v", level)
	}
}
