// Package self provides the "self" ingester type that captures the
// application's own slog output and feeds it into the ingest pipeline
// as structured JSON records.
package self

import (
	"gastrolog/internal/glid"
	"log/slog"
	"strings"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for the self ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"min_level": "warn",
	}
}

// NewFactory returns an IngesterFactory for the self ingester.
// The capture channel is created externally and shared with the CaptureHandler.
// The CaptureHandler reference is used to apply the min_level param and, via
// the drop monitor (gastrolog-5d5a3), to surface capture-channel overflow as
// an operator-visible alert through the AlertCollector. The alerts parameter
// may be nil for tests that don't exercise the monitor.
func NewFactory(
	ch <-chan logging.CapturedRecord,
	capture *logging.CaptureHandler,
	alerts orchestrator.AlertCollector,
) orchestrator.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		scopedLogger := logging.Default(logger).With(
			"component", "ingester",
			"type", "self",
			"instance", id.String(),
		)

		// Apply min_level param to the capture handler.
		baseLevel := slog.LevelWarn
		if lvl, ok := params["min_level"]; ok {
			baseLevel = parseLevel(lvl)
		}
		if capture != nil {
			capture.SetMinCaptureLevel(baseLevel)
		}

		return &ingester{
			id:        id.String(),
			ch:        ch,
			logger:    scopedLogger,
			capture:   capture,
			baseLevel: baseLevel,
			alerts:    alerts,
		}, nil
	}
}

// parseLevel converts a human-friendly level string to slog.Level.
func parseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelWarn
	}
}
