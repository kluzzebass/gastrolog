package metrics

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

const defaultInterval = 30 * time.Second

// StatsSource provides ingest queue statistics.
type StatsSource interface {
	IngestQueueDepth() int
	IngestQueueCapacity() int
}

// ParamDefaults returns the default parameter values for a metrics ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"interval": defaultInterval.String(),
	}
}

// NewFactory returns an IngesterFactory for the self-monitoring metrics ingester.
// The StatsSource is captured by the returned closure (same pattern as docker's NewFactory).
func NewFactory(src StatsSource) orchestrator.IngesterFactory {
	return func(id uuid.UUID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		interval := defaultInterval
		if v := params["interval"]; v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("metrics ingester %q: invalid interval %q: %w", id, v, err)
			}
			if d <= 0 {
				return nil, fmt.Errorf("metrics ingester %q: interval must be positive", id)
			}
			interval = d
		}

		scopedLogger := logging.Default(logger).With(
			"component", "ingester",
			"type", "metrics",
			"instance", id.String(),
		)

		return &ingester{
			id:       id.String(),
			interval: interval,
			src:      src,
			logger:   scopedLogger,
		}, nil
	}
}
