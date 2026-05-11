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
	t.Parallel()
	logger := Discard()
	if logger == nil {
		t.Fatal("Discard() returned nil")
	}
	// Should not panic when logging.
	logger.Info("test message")
	logger.Debug("debug message")
}

func TestDefault(t *testing.T) {
	t.Parallel()
	t.Run("nil returns discard", func(t *testing.T) {
		t.Parallel()
		logger := Default(nil)
		if logger == nil {
			t.Fatal("Default(nil) returned nil")
		}
		if logger.Enabled(context.Background(), slog.LevelInfo) {
			t.Error("Default(nil) should return a discard logger")
		}
	})

	t.Run("non-nil returns same logger", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		original := slog.New(slog.NewTextHandler(&buf, nil))
		result := Default(original)
		if result != original {
			t.Error("Default should return the same logger when non-nil")
		}
	})
}

// captureHandler records every received slog record for assertions.
// Uses a shared records pointer so WithAttrs clones share storage.
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
	return &captureHandler{mu: h.mu, records: h.records, attrs: newAttrs}
}

func (h *captureHandler) WithGroup(string) slog.Handler { return h }

func (h *captureHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(*h.records)
}

// setRules is a test helper that installs a fresh rule set with the
// next generation drawn from the production-shared NextGeneration counter.
// Sharing the counter is critical: if tests had their own counter
// starting at 1 and the constructor also started at 1, the first
// installed rule set would match the constructor's cached generation
// and the filter would silently keep its stale resolved level.
func setRules(filter *ComponentFilterHandler, defaultLevel slog.Level, rules ...LevelRule) {
	filter.SetRuleSet(NewRuleSet(defaultLevel, rules, NextGeneration()))
}

func TestComponentFilterHandler_DefaultLevel_FiltersBelow(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	logger.Info("info", "component", "test")
	if capture.count() != 1 {
		t.Errorf("INFO at default INFO: count = %d, want 1", capture.count())
	}
	logger.Debug("debug", "component", "test")
	if capture.count() != 1 {
		t.Errorf("DEBUG at default INFO: count = %d, want 1 (DEBUG filtered)", capture.count())
	}
	logger.Warn("warn", "component", "test")
	if capture.count() != 2 {
		t.Errorf("WARN at default INFO: count = %d, want 2", capture.count())
	}
}

func TestComponentFilterHandler_ExactRule(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator", Level: slog.LevelDebug},
	)

	logger.Debug("orch", "component", "orchestrator")
	if capture.count() != 1 {
		t.Errorf("exact rule should allow DEBUG: count = %d, want 1", capture.count())
	}
	logger.Debug("query", "component", "query-engine")
	if capture.count() != 1 {
		t.Errorf("non-matching component still filtered: count = %d, want 1", capture.count())
	}
}

func TestComponentFilterHandler_SubtreeGlob(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)

	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator.**", Level: slog.LevelDebug},
	)

	// ** matches the root itself plus any descendant.
	slog.New(filter).With("component", "orchestrator").Debug("root")
	if capture.count() != 1 {
		t.Errorf("** should match the root: count = %d, want 1", capture.count())
	}
	slog.New(filter).With("component", "orchestrator.replication.catchup").Debug("deep")
	if capture.count() != 2 {
		t.Errorf("** should cover deep descendants: count = %d, want 2", capture.count())
	}
	// Unrelated component still filtered.
	slog.New(filter).With("component", "cluster").Debug("nope")
	if capture.count() != 2 {
		t.Errorf("unrelated path: count = %d, want 2", capture.count())
	}
}

func TestComponentFilterHandler_SpecificityWins(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter).With("component", "orchestrator.replication.catchup")

	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator.*", Level: slog.LevelDebug},
		LevelRule{Pattern: "orchestrator.replication.catchup", Level: slog.LevelError},
	)

	logger.Warn("warn record under exact ERROR rule")
	if capture.count() != 0 {
		t.Errorf("exact ERROR should suppress WARN: count = %d, want 0", capture.count())
	}
	logger.Error("error record")
	if capture.count() != 1 {
		t.Errorf("ERROR at exact ERROR rule: count = %d, want 1", capture.count())
	}
}

func TestComponentFilterHandler_CacheInvalidatesOnRuleSwap(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter).With("component", "orchestrator")

	logger.Debug("first DEBUG, default INFO")
	if capture.count() != 0 {
		t.Errorf("DEBUG before rule: count = %d, want 0", capture.count())
	}

	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator", Level: slog.LevelDebug},
	)

	logger.Debug("second DEBUG, rule lifted to DEBUG")
	if capture.count() != 1 {
		t.Errorf("DEBUG after rule: count = %d, want 1 (cache should invalidate)", capture.count())
	}

	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator", Level: slog.LevelError},
	)

	logger.Debug("third DEBUG, rule raised to ERROR")
	if capture.count() != 1 {
		t.Errorf("DEBUG after rule tightened: count = %d, want 1", capture.count())
	}
}

func TestComponentFilterHandler_WithAttrsCapturesComponent(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)

	logger := slog.New(filter).With("component", "orchestrator")
	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator", Level: slog.LevelDebug},
	)

	logger.Debug("captured at WithAttrs time")
	if capture.count() != 1 {
		t.Errorf("captured component should resolve: count = %d, want 1", capture.count())
	}
}

func TestComponentFilterHandler_NoComponent_UsesDefault(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter)

	logger.Info("info, no component")
	if capture.count() != 1 {
		t.Errorf("INFO with no component: count = %d, want 1", capture.count())
	}
	logger.Debug("debug, no component")
	if capture.count() != 1 {
		t.Errorf("DEBUG with no component at default INFO: count = %d, want 1", capture.count())
	}
}

func TestComponentFilterHandler_RecordLevelComponent(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter) // no captured componentPath

	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator", Level: slog.LevelDebug},
	)

	// Inline component attr on the call. The handler has no captured
	// componentPath, so Enabled is permissive (lvl >= MinLevel) and
	// Handle does the final per-record resolution.
	logger.Debug("debug", "component", "orchestrator")
	if capture.count() != 1 {
		t.Errorf("inline component=orchestrator at DEBUG rule: count = %d, want 1", capture.count())
	}
	logger.Debug("debug", "component", "cluster")
	if capture.count() != 1 {
		t.Errorf("inline component=cluster (no rule, default INFO): count = %d, want 1", capture.count())
	}
}

func TestComponentFilterHandler_Concurrent(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)
	logger := slog.New(filter).With("component", "test")

	const goroutines = 10
	const iterations = 100

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				logger.Info("message")
			}
		})
	}
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				setRules(filter, slog.LevelInfo,
					LevelRule{Pattern: "test", Level: slog.LevelDebug},
				)
				setRules(filter, slog.LevelInfo)
			}
		})
	}
	wg.Wait()

	if count := capture.count(); count != goroutines*iterations {
		t.Errorf("INFO records emitted: count = %d, want %d", count, goroutines*iterations)
	}
}

func TestComponentFilterHandler_Integration(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	base := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	filter := NewComponentFilterHandler(base, slog.LevelInfo)
	root := slog.New(filter)
	orchLogger := root.With("component", "orchestrator")
	queryLogger := root.With("component", "query-engine")

	orchLogger.Debug("orch debug 1")
	queryLogger.Debug("query debug 1")
	if buf.Len() != 0 {
		t.Errorf("baseline INFO should drop DEBUG, got: %s", buf.String())
	}

	setRules(filter, slog.LevelInfo,
		LevelRule{Pattern: "orchestrator", Level: slog.LevelDebug},
	)
	orchLogger.Debug("orch debug 2")
	queryLogger.Debug("query debug 2")

	output := buf.String()
	if !strings.Contains(output, "orch debug 2") {
		t.Errorf("orchestrator DEBUG missing, got: %s", output)
	}
	if strings.Contains(output, "query debug") {
		t.Errorf("query-engine DEBUG leaked, got: %s", output)
	}
}

func TestComponentFilterHandler_WithGroup(t *testing.T) {
	t.Parallel()
	capture := newCaptureHandler()
	filter := NewComponentFilterHandler(capture, slog.LevelInfo)

	grouped := filter.WithGroup("mygroup")
	logger := slog.New(grouped)

	logger.Info("info", "component", "test")
	if capture.count() != 1 {
		t.Errorf("INFO under WithGroup: count = %d, want 1", capture.count())
	}
	logger.Debug("debug", "component", "test")
	if capture.count() != 1 {
		t.Errorf("DEBUG under WithGroup at default INFO: count = %d, want 1", capture.count())
	}
}

func TestComponentFilterHandler_RuleSetRoundTrip(t *testing.T) {
	t.Parallel()
	filter := NewComponentFilterHandler(nil, slog.LevelInfo)
	rs := NewRuleSet(slog.LevelWarn, []LevelRule{
		{Pattern: "x", Level: slog.LevelDebug},
	}, 42)
	filter.SetRuleSet(rs)

	got := filter.RuleSet()
	if got.Default != slog.LevelWarn {
		t.Errorf("RuleSet().Default = %v, want WARN", got.Default)
	}
	if got.Generation != 42 {
		t.Errorf("RuleSet().Generation = %d, want 42", got.Generation)
	}
	if len(got.Rules) != 1 || got.Rules[0].Pattern != "x" {
		t.Errorf("RuleSet().Rules = %v, want [{x DEBUG}]", got.Rules)
	}
}
