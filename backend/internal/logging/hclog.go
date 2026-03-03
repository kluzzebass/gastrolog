// Package logging provides utilities for structured logging across the system.

package logging

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"strings"

	"github.com/hashicorp/go-hclog"
)

// HclogAdapter bridges hclog.Logger → slog.Logger so that libraries using
// hclog (e.g. hashicorp/raft) can emit structured logs through the
// application's slog pipeline. The adapter maps hclog levels to slog levels
// and forwards key-value pairs as slog attributes.
type HclogAdapter struct {
	logger *slog.Logger
}

// NewHclogAdapter creates an hclog.Logger backed by the given slog.Logger.
func NewHclogAdapter(logger *slog.Logger) hclog.Logger {
	return &HclogAdapter{logger: logger}
}

func (h *HclogAdapter) toAttrs(args ...any) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(args)/2)
	for i := 0; i+1 < len(args); i += 2 {
		key, ok := args[i].(string)
		if !ok {
			key = fmt.Sprint(args[i])
		}
		attrs = append(attrs, slog.Any(key, args[i+1]))
	}
	return attrs
}

func (h *HclogAdapter) log(level slog.Level, msg string, args ...any) {
	if !h.logger.Enabled(context.Background(), level) {
		return
	}
	attrs := h.toAttrs(args...)
	anyAttrs := make([]any, len(attrs))
	for i, a := range attrs {
		anyAttrs[i] = a
	}
	h.logger.Log(context.Background(), level, msg, anyAttrs...)
}

func (h *HclogAdapter) Log(level hclog.Level, msg string, args ...any) {
	h.log(hclogToSlog(level), msg, args...)
}

func (h *HclogAdapter) Trace(msg string, args ...any) {
	h.log(slog.LevelDebug-4, msg, args...) // slog has no Trace; use Debug-4
}

func (h *HclogAdapter) Debug(msg string, args ...any) {
	h.log(slog.LevelDebug, msg, args...)
}

func (h *HclogAdapter) Info(msg string, args ...any) {
	h.log(slog.LevelInfo, msg, args...)
}

func (h *HclogAdapter) Warn(msg string, args ...any) {
	h.log(slog.LevelWarn, msg, args...)
}

func (h *HclogAdapter) Error(msg string, args ...any) {
	h.log(slog.LevelError, msg, args...)
}

func (h *HclogAdapter) IsTrace() bool { return h.logger.Enabled(context.Background(), slog.LevelDebug-4) }
func (h *HclogAdapter) IsDebug() bool { return h.logger.Enabled(context.Background(), slog.LevelDebug) }
func (h *HclogAdapter) IsInfo() bool  { return h.logger.Enabled(context.Background(), slog.LevelInfo) }
func (h *HclogAdapter) IsWarn() bool  { return h.logger.Enabled(context.Background(), slog.LevelWarn) }
func (h *HclogAdapter) IsError() bool { return h.logger.Enabled(context.Background(), slog.LevelError) }

func (h *HclogAdapter) ImpliedArgs() []any { return nil }

func (h *HclogAdapter) With(args ...any) hclog.Logger {
	attrs := h.toAttrs(args...)
	anyAttrs := make([]any, len(attrs))
	for i, a := range attrs {
		anyAttrs[i] = a
	}
	return &HclogAdapter{logger: h.logger.With(anyAttrs...)}
}

func (h *HclogAdapter) Name() string { return "" }

func (h *HclogAdapter) Named(name string) hclog.Logger {
	return &HclogAdapter{logger: h.logger.With("subsystem", name)}
}

func (h *HclogAdapter) ResetNamed(name string) hclog.Logger {
	return h.Named(name)
}

func (h *HclogAdapter) SetLevel(level hclog.Level) {}

func (h *HclogAdapter) GetLevel() hclog.Level { return hclog.Trace }

func (h *HclogAdapter) StandardLogger(opts *hclog.StandardLoggerOptions) *log.Logger {
	return log.New(h.StandardWriter(opts), "", 0)
}

func (h *HclogAdapter) StandardWriter(opts *hclog.StandardLoggerOptions) io.Writer {
	return os.Stderr
}

func hclogToSlog(level hclog.Level) slog.Level {
	switch level {
	case hclog.NoLevel, hclog.Off:
		return slog.LevelInfo
	case hclog.Trace:
		return slog.LevelDebug - 4
	case hclog.Debug:
		return slog.LevelDebug
	case hclog.Info:
		return slog.LevelInfo
	case hclog.Warn:
		return slog.LevelWarn
	case hclog.Error:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SlogToHclogLevel converts an slog.Level to the closest hclog.Level.
func SlogToHclogLevel(level slog.Level) hclog.Level {
	switch {
	case level <= slog.LevelDebug-4:
		return hclog.Trace
	case level <= slog.LevelDebug:
		return hclog.Debug
	case level <= slog.LevelInfo:
		return hclog.Info
	case level <= slog.LevelWarn:
		return hclog.Warn
	default:
		return hclog.Error
	}
}

// FilterHclogMessages returns an hclog.Logger that drops messages containing
// any of the given substrings. Useful for suppressing noisy library logs.
func FilterHclogMessages(base hclog.Logger, suppress ...string) hclog.Logger {
	return &filteringHclog{base: base, suppress: suppress}
}

type filteringHclog struct {
	base     hclog.Logger
	suppress []string
}

func (f *filteringHclog) shouldSuppress(msg string) bool {
	for _, s := range f.suppress {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

func (f *filteringHclog) Log(level hclog.Level, msg string, args ...any) {
	if !f.shouldSuppress(msg) {
		f.base.Log(level, msg, args...)
	}
}
func (f *filteringHclog) Trace(msg string, args ...any) {
	if !f.shouldSuppress(msg) {
		f.base.Trace(msg, args...)
	}
}
func (f *filteringHclog) Debug(msg string, args ...any) {
	if !f.shouldSuppress(msg) {
		f.base.Debug(msg, args...)
	}
}
func (f *filteringHclog) Info(msg string, args ...any) {
	if !f.shouldSuppress(msg) {
		f.base.Info(msg, args...)
	}
}
func (f *filteringHclog) Warn(msg string, args ...any) {
	if !f.shouldSuppress(msg) {
		f.base.Warn(msg, args...)
	}
}
func (f *filteringHclog) Error(msg string, args ...any) {
	if !f.shouldSuppress(msg) {
		f.base.Error(msg, args...)
	}
}

func (f *filteringHclog) IsTrace() bool                          { return f.base.IsTrace() }
func (f *filteringHclog) IsDebug() bool                          { return f.base.IsDebug() }
func (f *filteringHclog) IsInfo() bool                           { return f.base.IsInfo() }
func (f *filteringHclog) IsWarn() bool                           { return f.base.IsWarn() }
func (f *filteringHclog) IsError() bool                          { return f.base.IsError() }
func (f *filteringHclog) ImpliedArgs() []any             { return f.base.ImpliedArgs() }
func (f *filteringHclog) Name() string                           { return f.base.Name() }
func (f *filteringHclog) SetLevel(level hclog.Level)             { f.base.SetLevel(level) }
func (f *filteringHclog) GetLevel() hclog.Level                  { return f.base.GetLevel() }
func (f *filteringHclog) StandardLogger(o *hclog.StandardLoggerOptions) *log.Logger { return f.base.StandardLogger(o) }
func (f *filteringHclog) StandardWriter(o *hclog.StandardLoggerOptions) io.Writer   { return f.base.StandardWriter(o) }

func (f *filteringHclog) With(args ...any) hclog.Logger {
	return &filteringHclog{base: f.base.With(args...), suppress: f.suppress}
}
func (f *filteringHclog) Named(name string) hclog.Logger {
	return &filteringHclog{base: f.base.Named(name), suppress: f.suppress}
}
func (f *filteringHclog) ResetNamed(name string) hclog.Logger {
	return &filteringHclog{base: f.base.ResetNamed(name), suppress: f.suppress}
}
