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
