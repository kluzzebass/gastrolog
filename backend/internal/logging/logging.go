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

// ComponentFilterHandler wraps an slog.Handler and gates records by the
// component path attached to their logger.
//
// Design:
//   - A single immutable RuleSet (default level + pattern→level overrides)
//     lives behind an atomic.Pointer shared by every derived handler.
//   - Mutation = build a new RuleSet, atomically swap the pointer. Bumps
//     the generation counter so every derived handler invalidates its
//     resolved-level cache on the next call.
//   - Each derived handler captures its component path once (when
//     comp.Path.Apply or any WithAttrs containing "component" is used)
//     and caches the resolved level keyed on the RuleSet generation —
//     so the hot path is two atomic loads and a compare.
//
// Components SHOULD set their "component" attribute via comp.Path.Apply
// rather than passing it as a record-level attribute. Record-level
// "component" attributes are still supported (handlers whose captured
// path is empty inspect the record in Handle), but they cost an
// attribute scan per call and miss the per-handler cache.
//
// Usage:
//
//	base := slog.NewTextHandler(os.Stderr, nil)
//	filter := logging.NewComponentFilterHandler(base, slog.LevelInfo)
//	logger := slog.New(filter)
//
//	// later, install a new rule set (e.g. from the config store):
//	filter.SetRuleSet(logging.NewRuleSet(slog.LevelInfo, []logging.LevelRule{
//	    {Pattern: "orchestrator.*", Level: slog.LevelDebug},
//	}, /* generation */ 7))
type ComponentFilterHandler struct {
	next slog.Handler

	// state is shared with every handler derived via WithAttrs/WithGroup.
	// Mutation is via SetRuleSet, which atomically replaces the pointer.
	state *atomic.Pointer[RuleSet]

	// componentPath is the dotted path captured the first time a
	// "component" attribute is set via WithAttrs (e.g. by comp.Path.Apply).
	// Empty for handlers that never had one assigned — those defer the
	// component decision to Handle (which inspects record attrs).
	componentPath string

	// Per-handler resolution cache. cachedGen records the RuleSet
	// generation that produced cachedLevel; when state.Load().Generation
	// no longer matches, Enabled re-resolves.
	cachedGen   atomic.Uint64
	cachedLevel atomic.Int64 // slog.Level stored as int64
}

// NewComponentFilterHandler creates a filter handler with the given
// fallback level and no overrides. Use SetRuleSet to install rules
// (typically driven by the system config store).
func NewComponentFilterHandler(next slog.Handler, defaultLevel slog.Level) *ComponentFilterHandler {
	state := &atomic.Pointer[RuleSet]{}
	rs := NewRuleSet(defaultLevel, nil, NextGeneration())
	state.Store(&rs)
	return &ComponentFilterHandler{next: next, state: state}
}

// generationCounter is a process-wide monotonic source used by every
// caller that builds a RuleSet (the constructor here and any code that
// derives a new RuleSet from the config store). Using a single source
// is what prevents the cache-invalidation hazard where a handler's
// constructor-supplied generation collides with a later caller's
// freshly-assigned generation, causing derived handlers to think their
// cached level is still valid.
var generationCounter atomic.Uint64

// NextGeneration returns the next monotonically-increasing generation
// value. Always use this when constructing a RuleSet that will be
// installed via SetRuleSet, including from the config-store wiring.
func NextGeneration() uint64 {
	return generationCounter.Add(1)
}

// SetRuleSet atomically replaces the active rule set across every
// handler derived from this one. The new RuleSet's Generation must
// differ from the previous one for derived handlers to notice; callers
// usually let the system config store assign monotonic generations.
func (h *ComponentFilterHandler) SetRuleSet(rs RuleSet) {
	h.state.Store(&rs)
}

// RuleSet returns the currently active rule set. The returned value is
// a snapshot — concurrent mutations don't affect it.
func (h *ComponentFilterHandler) RuleSet() RuleSet {
	return *h.state.Load()
}

// Enabled reports whether the given level passes the filter for this
// handler's component path.
//
// When componentPath is set, this is a definitive answer keyed on the
// cached resolution. When componentPath is empty, the handler defers to
// Handle (returning true if any rule could conceivably allow this
// level) so Handle can inspect record-level "component" attributes.
func (h *ComponentFilterHandler) Enabled(_ context.Context, level slog.Level) bool {
	s := h.state.Load()
	if h.componentPath == "" {
		return level >= s.MinLevel
	}
	if h.cachedGen.Load() != s.Generation {
		h.cachedLevel.Store(int64(s.Resolve(h.componentPath)))
		h.cachedGen.Store(s.Generation)
	}
	return level >= slog.Level(h.cachedLevel.Load())
}

// Handle emits the record after a final filter check. When the handler
// has a captured componentPath, Enabled already settled the decision
// and Handle just forwards. When componentPath is empty, Handle scans
// the record for a "component" attribute and applies the rule lookup
// before forwarding.
func (h *ComponentFilterHandler) Handle(ctx context.Context, r slog.Record) error {
	if h.componentPath == "" {
		s := h.state.Load()
		var minLevel slog.Level
		if component := findRecordComponent(r); component != "" {
			minLevel = s.Resolve(component)
		} else {
			minLevel = s.Default
		}
		if r.Level < minLevel {
			return nil
		}
	}
	if !h.next.Enabled(ctx, r.Level) {
		return nil
	}
	return h.next.Handle(ctx, r)
}

// WithAttrs returns a new handler with the given attributes. If the
// attributes contain "component", the new handler captures it as its
// componentPath — the discipline-enforcing channel for comp.Path.Apply.
func (h *ComponentFilterHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	componentPath := h.componentPath
	for _, a := range attrs {
		if a.Key == "component" {
			if s, ok := a.Value.Resolve().Any().(string); ok {
				componentPath = s
			}
		}
	}
	return &ComponentFilterHandler{
		next:          h.next.WithAttrs(attrs),
		state:         h.state,
		componentPath: componentPath,
	}
}

// WithGroup returns a new handler with the given group name. The
// captured component path is preserved — group nesting does not affect
// component filtering.
func (h *ComponentFilterHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	return &ComponentFilterHandler{
		next:          h.next.WithGroup(name),
		state:         h.state,
		componentPath: h.componentPath,
	}
}

// findRecordComponent returns the value of a record-level "component"
// attribute, or "" if not present.
func findRecordComponent(r slog.Record) string {
	var out string
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == "component" {
			if s, ok := a.Value.Resolve().Any().(string); ok {
				out = s
				return false
			}
		}
		return true
	})
	return out
}
