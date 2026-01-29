package chatterbox

import (
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"gastrolog/internal/logging"
	"gastrolog/internal/orchestrator"
)

const (
	defaultMinInterval = 100 * time.Millisecond
	defaultMaxInterval = 1 * time.Second
	defaultInstance    = "default"
)

// NewReceiver creates a new chatterbox receiver from configuration parameters.
//
// Supported parameters:
//   - "min_interval": minimum delay between messages (default: "100ms")
//   - "max_interval": maximum delay between messages (default: "1s")
//   - "instance": instance identifier for source attribution (default: "default")
//
// Intervals use Go duration format: "100us", "1.5ms", "2s", etc.
//
// If logger is nil, logging is disabled.
//
// Returns an error if parameters are invalid (e.g., unparseable duration,
// min > max, negative values).
func NewReceiver(params map[string]string, logger *slog.Logger) (orchestrator.Receiver, error) {
	minInterval := defaultMinInterval
	maxInterval := defaultMaxInterval
	instance := defaultInstance

	if v, ok := params["min_interval"]; ok {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid min_interval %q: %w", v, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("min_interval must be non-negative, got %v", parsed)
		}
		minInterval = parsed
	}

	if v, ok := params["max_interval"]; ok {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid max_interval %q: %w", v, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("max_interval must be non-negative, got %v", parsed)
		}
		maxInterval = parsed
	}

	if minInterval > maxInterval {
		return nil, fmt.Errorf("min_interval (%v) must not exceed max_interval (%v)", minInterval, maxInterval)
	}

	if v, ok := params["instance"]; ok && v != "" {
		instance = v
	}

	// Scope logger with component identity.
	scopedLogger := logging.Default(logger).With(
		"component", "receiver",
		"type", "chatterbox",
		"instance", instance,
	)

	return &Receiver{
		minInterval: minInterval,
		maxInterval: maxInterval,
		instance:    instance,
		rng:         rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		logger:      scopedLogger,
	}, nil
}
