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

	switch rng.IntN(8) {
	case 0:
		// HTTP request with nested headers and response
		obj["request"] = map[string]any{
			"method": pick(rng, []string{"GET", "POST", "PUT", "DELETE"}),
			"path":   pick(rng, []string{"/api/v1/users", "/api/v1/orders", "/graphql", "/ws"}),
			"headers": map[string]any{
				"content_type":  pick(rng, []string{"application/json", "text/html", "multipart/form-data"}),
				"authorization": "Bearer ***",
				"x_request_id":  fmt.Sprintf("%016x", rng.Uint64()),
			},
		}
		obj["response"] = map[string]any{
			"status":     200 + rng.IntN(300),
			"latency_ms": rng.Float64() * 500,
			"bytes":      rng.IntN(100000),
		}
	case 1:
		// Error with nested context and cause chain
		level = pick(rng, []string{"error", "error", "error", "warn"})
		obj["level"] = level
		obj["error"] = map[string]any{
			"message": pick(rng, []string{"connection refused", "timeout", "invalid input", "not found", "permission denied"}),
			"code":    pick(rng, []string{"ECONNREFUSED", "ETIMEDOUT", "EINVAL", "ENOENT", "EACCES"}),
			"source": map[string]any{
				"file":     pick(rng, []string{"main.go", "handler.go", "service.go", "client.go"}),
				"line":     rng.IntN(500),
				"function": pick(rng, []string{"handleRequest", "connect", "validate", "process"}),
			},
		}
		obj["retry_count"] = rng.IntN(5)
	case 2:
		// Business event with nested entity
		obj["event_type"] = pick(rng, []string{"order.created", "payment.processed", "user.registered", "item.shipped"})
		obj["entity"] = map[string]any{
			"id":   fmt.Sprintf("%08x", rng.Uint32()),
			"type": pick(rng, []string{"order", "payment", "user", "item"}),
			"metadata": map[string]any{
				"region":   pick(rng, []string{"us-east-1", "eu-west-1", "ap-south-1"}),
				"priority": pick(rng, []string{"low", "medium", "high", "critical"}),
			},
		}
		obj["amount"] = rng.Float64() * 1000
		obj["currency"] = pick(rng, []string{"USD", "EUR", "GBP"})
	case 3:
		// System metrics with nested resource breakdown
		obj["resources"] = map[string]any{
			"cpu": map[string]any{
				"percent": rng.Float64() * 100,
				"cores":   1 + rng.IntN(16),
			},
			"memory": map[string]any{
				"used_mb":  rng.IntN(8192),
				"total_mb": 8192 + rng.IntN(8192),
				"percent":  rng.Float64() * 100,
			},
			"disk": map[string]any{
				"read_iops":  rng.IntN(10000),
				"write_iops": rng.IntN(5000),
			},
		}
		obj["goroutines"] = rng.IntN(1000)
		obj["gc_pause_ms"] = rng.Float64() * 10
	case 4:
		// Distributed trace with nested spans array
		spanCount := 1 + rng.IntN(4)
		spans := make([]any, spanCount)
		for i := range spanCount {
			spans[i] = map[string]any{
				"span_id":       fmt.Sprintf("%016x", rng.Uint64()),
				"operation":     pick(rng, []string{"db.query", "http.request", "cache.get", "grpc.call", "queue.publish"}),
				"duration_us":   rng.IntN(100000),
				"status":        pick(rng, []string{"ok", "error", "timeout"}),
			}
		}
		obj["trace_id"] = fmt.Sprintf("%032x", rng.Uint64())
		obj["spans"] = spans
	case 5:
		// Kubernetes event with deep nesting
		obj["kubernetes"] = map[string]any{
			"namespace": pick(rng, []string{"default", "production", "staging", "monitoring"}),
			"pod": map[string]any{
				"name":   fmt.Sprintf("app-%s-%08x", pick(rng, []string{"web", "api", "worker"}), rng.Uint32()),
				"status": pick(rng, []string{"Running", "Pending", "CrashLoopBackOff", "OOMKilled"}),
				"containers": []any{
					map[string]any{
						"name":          pick(rng, []string{"app", "sidecar", "init"}),
						"image":         fmt.Sprintf("registry.io/app:%d.%d.%d", rng.IntN(3), rng.IntN(10), rng.IntN(100)),
						"restart_count": rng.IntN(10),
						"ready":         rng.IntN(2) == 0,
					},
				},
			},
			"node": pick(rng, []string{"node-1", "node-2", "node-3"}),
		}
	case 6:
		// Database query with nested explain plan
		obj["query"] = map[string]any{
			"sql":         pick(rng, []string{"SELECT * FROM users WHERE id = ?", "INSERT INTO orders VALUES (?)", "UPDATE inventory SET count = count - 1"}),
			"duration_ms": rng.Float64() * 500,
			"rows":        rng.IntN(10000),
			"plan": map[string]any{
				"type":           pick(rng, []string{"index_scan", "seq_scan", "nested_loop", "hash_join"}),
				"cost":           rng.Float64() * 1000,
				"actual_rows":    rng.IntN(10000),
				"buffers_shared": rng.IntN(1000),
			},
		}
		obj["connection"] = map[string]any{
			"pool_size":    10 + rng.IntN(90),
			"active":       rng.IntN(50),
			"idle":         rng.IntN(50),
			"wait_time_ms": rng.Float64() * 100,
		}
	default:
		// Pipeline with array of stages
		stageNames := []string{"parse", "validate", "enrich", "transform", "route", "deliver"}
		stageCount := 2 + rng.IntN(len(stageNames)-1)
		stages := make([]any, stageCount)
		for i := range stageCount {
			stages[i] = map[string]any{
				"name":        stageNames[i],
				"duration_ms": rng.Float64() * 50,
				"records_in":  100 + rng.IntN(9900),
				"records_out": 100 + rng.IntN(9900),
				"errors":      rng.IntN(5),
			}
		}
		obj["pipeline"] = map[string]any{
			"id":     fmt.Sprintf("pipe-%08x", rng.Uint32()),
			"stages": stages,
		}
		obj["total_duration_ms"] = rng.Float64() * 300
	}

	data, _ := json.Marshal(obj)

	attrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"env":     pick(rng, f.pools.Envs),
		"host":    pick(rng, f.pools.Hosts),
	}

	return data, attrs, now
}
