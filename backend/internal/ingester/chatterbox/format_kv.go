package chatterbox

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// KeyValueFormat generates structured key=value log lines.
type KeyValueFormat struct {
	pools *AttributePools
}

// NewKeyValueFormat creates a key-value format generator.
func NewKeyValueFormat(pools *AttributePools) *KeyValueFormat {
	return &KeyValueFormat{pools: pools}
}

func (f *KeyValueFormat) Generate(rng *rand.Rand) ([]byte, map[string]string, time.Time) {
	levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	messages := []string{
		"request completed",
		"database query executed",
		"cache lookup",
		"authentication attempt",
		"authorization check",
		"file uploaded",
		"email sent",
		"webhook delivered",
		"task queued",
		"job processed",
		"metric recorded",
		"event published",
		"message consumed",
		"transaction committed",
		"session created",
	}
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	paths := []string{"/api/users", "/api/orders", "/api/products", "/health", "/metrics", "/api/auth/login", "/api/search"}
	userAgents := []string{"Mozilla/5.0", "curl/7.68.0", "Go-http-client/1.1", "python-requests/2.25.1", "okhttp/4.9.0"}

	level := pick(rng, levels)
	msg := pick(rng, messages)

	var line string
	switch rng.IntN(4) {
	case 0:
		// HTTP request style
		line = fmt.Sprintf(`level=%s msg=%q method=%s path=%s status=%d latency_ms=%d`,
			level, msg, pick(rng, methods), pick(rng, paths), 200+rng.IntN(300), rng.IntN(500))
	case 1:
		// Database query style
		line = fmt.Sprintf(`level=%s msg=%q table=%s rows=%d duration_ms=%d cached=%t`,
			level, msg, pick(rng, []string{"users", "orders", "products", "sessions", "events"}),
			rng.IntN(1000), rng.IntN(100), rng.IntN(2) == 1)
	case 2:
		// User action style
		line = fmt.Sprintf(`level=%s msg=%q user_id=%d action=%s ip=%s user_agent=%q`,
			level, msg, rng.IntN(100000),
			pick(rng, []string{"login", "logout", "view", "edit", "delete", "create"}),
			fmt.Sprintf("10.%d.%d.%d", rng.IntN(256), rng.IntN(256), rng.IntN(256)),
			pick(rng, userAgents))
	default:
		// Generic with trace context
		line = fmt.Sprintf(`level=%s msg=%q trace_id=%016x span_id=%08x duration_ms=%d`,
			level, msg, rng.Uint64(), rng.Uint32(), rng.IntN(1000))
	}

	attrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"env":     pick(rng, f.pools.Envs),
		"host":    pick(rng, f.pools.Hosts),
	}

	return []byte(line), attrs, time.Time{}
}
