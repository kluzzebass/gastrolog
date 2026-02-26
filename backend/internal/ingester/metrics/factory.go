package metrics

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

const (
	defaultInterval      = 30 * time.Second
	defaultStoreInterval = 10 * time.Second
)

// StatsSource provides ingest queue and per-store statistics.
type StatsSource interface {
	IngestQueueDepth() int
	IngestQueueCapacity() int
	StoreSnapshots() []orchestrator.StoreSnapshot
}

// ParamDefaults returns the default parameter values for a metrics ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"interval":       defaultInterval.String(),
		"store_interval": defaultStoreInterval.String(),
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

		storeInterval := defaultStoreInterval
		if v := params["store_interval"]; v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("metrics ingester %q: invalid store_interval %q: %w", id, v, err)
			}
			if d <= 0 {
				return nil, fmt.Errorf("metrics ingester %q: store_interval must be positive", id)
			}
			storeInterval = d
		}

		scopedLogger := logging.Default(logger).With(
			"component", "ingester",
			"type", "metrics",
			"instance", id.String(),
		)

		return &ingester{
			id:            id.String(),
			interval:      interval,
			storeInterval: storeInterval,
			src:           src,
			logger:        scopedLogger,
		}, nil
	}
}
