// Package self provides the "self" ingester type that captures the
// application's own slog output and feeds it into the ingest pipeline
// as structured JSON records.
package self

import (
	"log/slog"

	"github.com/google/uuid"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

// ParamDefaults returns the default parameter values for the self ingester.
// There are no user-configurable parameters.
func ParamDefaults() map[string]string {
	return nil
}

// NewFactory returns an IngesterFactory for the self ingester.
// The capture channel is created externally and shared with the CaptureHandler.
func NewFactory(ch <-chan logging.CapturedRecord) orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		scopedLogger := logging.Default(logger).With(
			"component", "ingester",
			"type", "self",
			"instance", id.String(),
		)

		return &ingester{
			id:     id.String(),
			ch:     ch,
			logger: scopedLogger,
		}, nil
	}
}
