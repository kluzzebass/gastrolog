// Package logging provides utilities for structured logging across the system.
//
// Design principles:
//   - Logging is dependency-injected, never global
//   - Each component owns its own scoped logger
//   - Logger scoping happens once at construction time
//   - slog.With() is used to attach default attributes
//   - If no logger is provided, a discard logger is used
//
// Global configuration (output format, level, destination) belongs only in main().
// Components must never call slog.SetDefault or access global loggers.
//
// Logging is intentionally sparse:
//   - No logging inside tight loops (tokenization, scanning, indexing inner loops)
//   - Lifecycle boundaries are the intended log points
package logging

import (
	"context"
	"log/slog"
	"maps"
	"sync/atomic"
)

// discardHandler is a handler that discards all log records.
type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (d discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return d }
func (d discardHandler) WithGroup(string) slog.Handler           { return d }

// Discard returns a logger that discards all output.
// Use this as a default when no logger is provided.
func Discard() *slog.Logger {
	return slog.New(discardHandler{})
}

// Default returns the provided logger if non-nil, otherwise returns a discard logger.
// This is the standard pattern for optional logger parameters:
//
//	func NewComponent(logger *slog.Logger) *Component {
//	    logger = logging.Default(logger)
//	    return &Component{logger: logger.With("component", "name")}
//	}
func Default(logger *slog.Logger) *slog.Logger {
	if logger != nil {
		return logger
	}
	return Discard()
}

// ComponentFilterHandler wraps an slog.Handler and filters log records based on
// component-specific log levels. This enables dynamic, attribute-based logging control
// without components needing to know about or manage log levels.
//
// Design:
//   - Each log record is inspected for a "component" attribute
//   - A per-component minimum level map determines visibility
//   - Records below the minimum level for their component are dropped
//   - Components without explicit levels fall back to the default level
//
// Thread-safety:
//   - Handle() uses lock-free atomic read of levels map
//   - SetLevel()/ClearLevel() use copy-on-write pattern
//   - No allocations or heavy work in the hot path
//
// Usage:
//
//	base := slog.NewTextHandler(os.Stderr, nil)
//	filter := logging.NewComponentFilterHandler(base, slog.LevelInfo)
//	logger := slog.New(filter)
//
//	// Later, enable debug for specific component:
//	filter.SetLevel("orchestrator", slog.LevelDebug)
type ComponentFilterHandler struct {
	next         slog.Handler
	defaultLevel slog.Level

	// preAttrs holds attributes added via WithAttrs before any group context.
	// These are checked for "component" in Handle().
	preAttrs []slog.Attr

	// levelSnapshot is a pointer to an atomic that holds the current levels map.
	// This is a pointer so that handlers created via WithAttrs/WithGroup share
	// the same atomic, allowing SetLevel changes to affect all derived loggers.
	// Uses copy-on-write pattern: writes create a new map, reads see a snapshot.
	levelSnapshot *atomic.Pointer[map[string]slog.Level]
}

// NewComponentFilterHandler creates a handler that filters log records based on
// component-specific log levels.
//
// Parameters:
//   - next: the wrapped handler that receives filtered records
//   - defaultLevel: minimum level for components without explicit configuration
//
// The handler inspects each record for a "component" attribute (string value)
// and compares the record's level against the configured minimum for that component.
func NewComponentFilterHandler(next slog.Handler, defaultLevel slog.Level) *ComponentFilterHandler {
	// Initialize with empty map snapshot.
	snapshot := &atomic.Pointer[map[string]slog.Level]{}
	empty := make(map[string]slog.Level)
	snapshot.Store(&empty)

	return &ComponentFilterHandler{
		next:          next,
		defaultLevel:  defaultLevel,
		levelSnapshot: snapshot,
	}
}

// Enabled returns true to defer filtering to Handle().
// We cannot filter here because we don't have access to the record's attributes yet.
func (h *ComponentFilterHandler) Enabled(ctx context.Context, level slog.Level) bool {
	// Always return true - actual filtering happens in Handle() where we can
	// inspect the "component" attribute. The wrapped handler's Enabled() will
	// be checked after we decide to pass the record through.
	return true
}

// Handle filters the record based on its component attribute and configured levels.
func (h *ComponentFilterHandler) Handle(ctx context.Context, r slog.Record) error {
	// Fast path: get the current levels snapshot (lock-free read).
	levels := *h.levelSnapshot.Load()

	// Find the component attribute.
	component := h.findComponent(r)

	// Determine minimum level for this component.
	minLevel := h.defaultLevel
	if component != "" {
		if level, ok := levels[component]; ok {
			minLevel = level
		}
	}

	// Filter: drop records below minimum level.
	if r.Level < minLevel {
		return nil
	}

	// Check if the wrapped handler is enabled for this level.
	if !h.next.Enabled(ctx, r.Level) {
		return nil
	}

	return h.next.Handle(ctx, r)
}

// findComponent extracts the "component" attribute value from preAttrs and record.
// Returns empty string if not found.
func (h *ComponentFilterHandler) findComponent(r slog.Record) string {
	// Check preAttrs first (attributes added via WithAttrs on this handler).
	for _, attr := range h.preAttrs {
		if attr.Key == "component" {
			if s, ok := attr.Value.Resolve().Any().(string); ok {
				return s
			}
		}
	}

	// Check record attributes.
	var component string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "component" {
			if s, ok := a.Value.Resolve().Any().(string); ok {
				component = s
				return false // stop iteration
			}
		}
		return true // continue
	})
	return component
}

// WithAttrs returns a new handler with the given attributes.
// If attrs contains "component", it will be used for filtering.
func (h *ComponentFilterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	// Create new handler with combined preAttrs.
	newPreAttrs := make([]slog.Attr, len(h.preAttrs), len(h.preAttrs)+len(attrs))
	copy(newPreAttrs, h.preAttrs)
	newPreAttrs = append(newPreAttrs, attrs...)

	return &ComponentFilterHandler{
		next:          h.next.WithAttrs(attrs),
		defaultLevel:  h.defaultLevel,
		preAttrs:      newPreAttrs,
		levelSnapshot: h.levelSnapshot, // Share the same atomic pointer.
	}
}

// WithGroup returns a new handler with the given group name.
func (h *ComponentFilterHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &ComponentFilterHandler{
		next:          h.next.WithGroup(name),
		defaultLevel:  h.defaultLevel,
		preAttrs:      h.preAttrs,
		levelSnapshot: h.levelSnapshot, // Share the same atomic pointer.
	}
}

// SetLevel sets the minimum log level for a specific component.
// This can be called at runtime to dynamically adjust verbosity.
// Thread-safe: uses copy-on-write for lock-free reads in Handle().
func (h *ComponentFilterHandler) SetLevel(component string, level slog.Level) {
	// Copy-on-write: create new map with updated value.
	oldLevels := *h.levelSnapshot.Load()
	newLevels := make(map[string]slog.Level, len(oldLevels)+1)
	maps.Copy(newLevels, oldLevels)
	newLevels[component] = level
	h.levelSnapshot.Store(&newLevels)
}

// ClearLevel removes the component-specific log level, reverting to the default.
// Thread-safe: uses copy-on-write for lock-free reads in Handle().
func (h *ComponentFilterHandler) ClearLevel(component string) {
	oldLevels := *h.levelSnapshot.Load()
	if _, ok := oldLevels[component]; !ok {
		return // Nothing to clear.
	}

	// Copy-on-write: create new map without the component.
	newLevels := make(map[string]slog.Level, len(oldLevels))
	for k, v := range oldLevels {
		if k != component {
			newLevels[k] = v
		}
	}
	h.levelSnapshot.Store(&newLevels)
}

// Level returns the current minimum level for a component.
// Returns the default level if no component-specific level is set.
// Thread-safe.
func (h *ComponentFilterHandler) Level(component string) slog.Level {
	levels := *h.levelSnapshot.Load()
	if level, ok := levels[component]; ok {
		return level
	}
	return h.defaultLevel
}

// DefaultLevel returns the default minimum level for components without explicit configuration.
func (h *ComponentFilterHandler) DefaultLevel() slog.Level {
	return h.defaultLevel
}
