// Package self provides the "self" ingester type that captures the
// application's own slog output and feeds it into the ingest pipeline
// as structured JSON records.
package self

import (
	"gastrolog/internal/glid"
	"gastrolog/internal/logging/comp"
	"gastrolog/internal/pipeline/ingestion"
	"log/slog"
	"strings"

	"gastrolog/internal/logging"
)

// ParamDefaults returns the default parameter values for the self ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"min_level": "warn",
	}
}

// NewFactory returns an IngesterFactory for the self ingester.
// The capture channel is created externally and shared with the CaptureHandler.
// The CaptureHandler reference is used to apply the min_level param and to
// raise the capture filter level under pressure. Capture-channel overflow is
// NOT surfaced here: the drop count is a metric read by the stats collector
// (NodeStats.self_ingester_drops_total), not an alarm.
func NewFactory(
	ch <-chan logging.CapturedRecord,
	capture *logging.CaptureHandler,
) ingestion.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (ingestion.Ingester, error) {
		scopedLogger := comp.Ingester.Sub("self").Desc("Self ingester — captures slog records emitted by this binary into a vault, mirroring stderr.").Apply(logging.Default(logger))

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
