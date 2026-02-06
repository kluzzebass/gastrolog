package chatterbox

import (
	"math/rand/v2"
	"time"
)

// PlainTextFormat generates simple unstructured log messages.
type PlainTextFormat struct {
	pools *AttributePools
}

// NewPlainTextFormat creates a plain text format generator.
func NewPlainTextFormat(pools *AttributePools) *PlainTextFormat {
	return &PlainTextFormat{pools: pools}
}

func (f *PlainTextFormat) Generate(rng *rand.Rand) ([]byte, map[string]string, time.Time) {
	messages := []string{
		"starting worker pool",
		"connection failed",
		"shutting down gracefully",
		"waiting for pending requests",
		"configuration reloaded",
		"health check passed",
		"memory pressure detected",
		"disk space low",
		"certificate expires soon",
		"rate limit exceeded",
		"connection pool exhausted",
		"cache warmed up",
		"leader election completed",
		"follower synced",
		"snapshot created",
		"compaction started",
		"index rebuilt",
		"migration completed",
		"backup finished",
		"restore in progress",
	}

	msg := pick(rng, messages)

	attrs := map[string]string{
		"service": pick(rng, f.pools.Services),
		"host":    pick(rng, f.pools.Hosts),
	}

	return []byte(msg), attrs, time.Time{}
}
