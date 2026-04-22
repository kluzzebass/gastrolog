package scatterbox

import (
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"strconv"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

const (
	defaultInterval = 100 * time.Millisecond
	defaultBurst    = 1
)

// ParamDefaults returns the default parameter values for a scatterbox ingester.
func ParamDefaults() map[string]string {
	return map[string]string{
		"interval": defaultInterval.String(),
		"burst":    strconv.Itoa(defaultBurst),
	}
}

// NewFactory returns an IngesterFactory that produces scatterbox ingesters
// tagged with nodeID. The node ID is embedded in every record's body and
// Attrs ("node" key), which lets a multi-node cluster running the same
// scatterbox config on every node produce records that are uniquely
// attributable to the originating node. Without this, the four nodes emit
// four JSON-identical records every interval and only the backend
// ingester_id attr tells them apart.
func NewFactory(nodeID string) orchestrator.IngesterFactory {
	return func(id glid.GLID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
		ing, err := NewIngester(id, params, logger)
		if err != nil {
			return nil, err
		}
		ing.(*Ingester).node = nodeID
		return ing, nil
	}
}

// NewIngester creates a scatterbox ingester from configuration parameters.
//
// Supported parameters:
//   - "interval": delay between emissions (default: "100ms")
//   - "burst": records per emission (default: 1)
//
// At the default settings, scatterbox emits 10 records/sec.
// Set interval=1ms burst=1 for 1000 records/sec.
//
// The returned ingester's node field is empty. Production code should use
// NewFactory(nodeID) instead so records embed the cluster node identifier.
func NewIngester(id glid.GLID, params map[string]string, logger *slog.Logger) (orchestrator.Ingester, error) {
	interval := defaultInterval
	burst := defaultBurst

	if v := params["interval"]; v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid interval %q: %w", v, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("interval must be non-negative, got %v", parsed)
		}
		interval = parsed
	}

	if v := params["burst"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid burst %q: %w", v, err)
		}
		if n <= 0 {
			return nil, fmt.Errorf("burst must be positive, got %d", n)
		}
		burst = n
	}

	_ = logging.Default(logger).With(
		"component", "ingester",
		"type", "scatterbox",
	)

	return &Ingester{
		id:       id.String(),
		interval: interval,
		burst:    burst,
		trigger:  make(chan struct{}, 1),
	}, nil
}
