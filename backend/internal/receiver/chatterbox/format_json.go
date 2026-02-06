package chatterbox

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"
)

// JSONFormat generates JSON-structured log messages.
type JSONFormat struct {
	pools *AttributePools
}

// NewJSONFormat creates a JSON format generator.
func NewJSONFormat(pools *AttributePools) *JSONFormat {
	return &JSONFormat{pools: pools}
}

func (f *JSONFormat) Generate(rng *rand.Rand) ([]byte, map[string]string, time.Time) {
	levels := []string{"debug", "info", "warn", "error"}
	messages := []string{
		"request handled",
		"database connection established",
		"cache invalidated",
		"user session expired",
		"rate limit applied",
		"circuit breaker opened",
		"retry succeeded",
		"fallback activated",
		"feature flag evaluated",
		"A/B test enrolled",
	}

	level := pick(rng, levels)
	msg := pick(rng, messages)

	now := time.Now()

	// Build JSON object with varied fields
	obj := map[string]any{
		"level": level,
		"msg":   msg,
		"ts":    now.UnixMilli(),
	}

	switch rng.IntN(5) {
	case 0:
		// HTTP metrics
		obj["method"] = pick(rng, []string{"GET", "POST", "PUT", "DELETE"})
		obj["path"] = pick(rng, []string{"/api/v1/users", "/api/v1/orders", "/graphql", "/ws"})
		obj["status"] = 200 + rng.IntN(300)
		obj["latency_ms"] = rng.Float64() * 500
		obj["bytes_in"] = rng.IntN(10000)
		obj["bytes_out"] = rng.IntN(100000)
	case 1:
		// Error details
		obj["error"] = pick(rng, []string{"connection refused", "timeout", "invalid input", "not found", "permission denied"})
		obj["stack"] = pick(rng, []string{"main.go:42", "handler.go:156", "service.go:89"})
		obj["retry_count"] = rng.IntN(5)
	case 2:
		// Business event
		obj["event_type"] = pick(rng, []string{"order.created", "payment.processed", "user.registered", "item.shipped"})
		obj["entity_id"] = fmt.Sprintf("%08x", rng.Uint32())
		obj["amount"] = rng.Float64() * 1000
		obj["currency"] = pick(rng, []string{"USD", "EUR", "GBP"})
	case 3:
		// System metrics
		obj["cpu_percent"] = rng.Float64() * 100
		obj["mem_mb"] = rng.IntN(8192)
		obj["goroutines"] = rng.IntN(1000)
		obj["gc_pause_ms"] = rng.Float64() * 10
	default:
		// Distributed tracing
		obj["trace_id"] = fmt.Sprintf("%032x", rng.Uint64())
		obj["span_id"] = fmt.Sprintf("%016x", rng.Uint64())
		obj["parent_id"] = fmt.Sprintf("%016x", rng.Uint64())
		obj["duration_us"] = rng.IntN(100000)
	}

	data, _ := json.Marshal(obj)

	attrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"env":     pick(rng, f.pools.Envs),
		"host":    pick(rng, f.pools.Hosts),
	}

	return data, attrs, now
}
