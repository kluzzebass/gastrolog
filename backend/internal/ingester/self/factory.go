// Package self provides the "self" ingester type that captures the
// application's own slog output and feeds it into the ingest pipeline
// as structured JSON records.
package self

import (
	"log/slog"
	"strings"

	"github.com/google/uuid"

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
// The CaptureHandler reference is used to apply the min_level param.
func NewFactory(ch <-chan logging.CapturedRecord, capture *logging.CaptureHandler) orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		scopedLogger := logging.Default(logger).With(
			"component", "ingester",
			"type", "self",
			"instance", id.String(),
		)

		// Apply min_level param to the capture handler.
		if capture != nil {
			if lvl, ok := params["min_level"]; ok {
				capture.SetMinCaptureLevel(parseLevel(lvl))
			}
		}

		return &ingester{
			id:     id.String(),
			ch:     ch,
			logger: scopedLogger,
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
