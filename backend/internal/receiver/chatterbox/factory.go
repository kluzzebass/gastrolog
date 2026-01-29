package chatterbox

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"gastrolog/internal/orchestrator"
)

const (
	defaultMinIntervalMs = 100
	defaultMaxIntervalMs = 1000
	defaultInstance      = "default"
)

// NewReceiver creates a new chatterbox receiver from configuration parameters.
//
// Supported parameters:
//   - "min_interval_ms": minimum delay between messages (default: 100)
//   - "max_interval_ms": maximum delay between messages (default: 1000)
//   - "instance": instance identifier for source attribution (default: "default")
//
// Returns an error if parameters are invalid (e.g., non-numeric intervals,
// min > max, negative values).
func NewReceiver(params map[string]string) (orchestrator.Receiver, error) {
	minMs := defaultMinIntervalMs
	maxMs := defaultMaxIntervalMs
	instance := defaultInstance

	if v, ok := params["min_interval_ms"]; ok {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid min_interval_ms %q: %w", v, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("min_interval_ms must be non-negative, got %d", parsed)
		}
		minMs = parsed
	}

	if v, ok := params["max_interval_ms"]; ok {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return nil, fmt.Errorf("invalid max_interval_ms %q: %w", v, err)
		}
		if parsed < 0 {
			return nil, fmt.Errorf("max_interval_ms must be non-negative, got %d", parsed)
		}
		maxMs = parsed
	}

	if minMs > maxMs {
		return nil, fmt.Errorf("min_interval_ms (%d) must not exceed max_interval_ms (%d)", minMs, maxMs)
	}

	if v, ok := params["instance"]; ok && v != "" {
		instance = v
	}

	return &Receiver{
		minInterval: time.Duration(minMs) * time.Millisecond,
		maxInterval: time.Duration(maxMs) * time.Millisecond,
		instance:    instance,
		rng:         rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
	}, nil
}
