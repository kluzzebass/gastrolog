// Package chatterbox provides a receiver that emits random log messages
// at random intervals. It is used to exercise the full ingest pipeline.
package chatterbox

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"gastrolog/internal/orchestrator"
)

// Receiver emits random log-like messages at random intervals.
// It implements orchestrator.Receiver.
//
// Logging:
//   - Logger is dependency-injected via the factory
//   - Receiver owns its scoped logger (component="receiver", type="chatterbox")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in the message generation loop
type Receiver struct {
	minInterval time.Duration
	maxInterval time.Duration
	instance    string
	rng         *rand.Rand

	// Logger for this receiver instance.
	// Scoped with component="receiver", type="chatterbox" at construction time.
	logger *slog.Logger
}

// Run starts the receiver and emits messages to the output channel.
// Run blocks until ctx is cancelled. Returns nil on normal cancellation.
func (r *Receiver) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
	timer := time.NewTimer(r.randomInterval())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
		}

		msg := r.generateMessage()
		select {
		case out <- msg:
		case <-ctx.Done():
			return nil
		}

		timer.Reset(r.randomInterval())
	}
}

// randomInterval returns a random duration between minInterval and maxInterval.
func (r *Receiver) randomInterval() time.Duration {
	if r.minInterval >= r.maxInterval {
		return r.minInterval
	}
	delta := r.maxInterval - r.minInterval
	return r.minInterval + time.Duration(r.rng.Int64N(int64(delta)))
}

// generateMessage creates a random log-like message.
func (r *Receiver) generateMessage() orchestrator.IngestMessage {
	return orchestrator.IngestMessage{
		Attrs: map[string]string{
			"receiver": "chatterbox",
			"instance": r.instance,
		},
		Raw:      r.randomLogLine(),
		IngestTS: time.Now(),
	}
}

// randomLogLine generates a random log-like string.
func (r *Receiver) randomLogLine() []byte {
	levels := []string{"DEBUG", "INFO", "WARN", "ERROR"}
	messages := []string{
		"request processed",
		"connection established",
		"cache miss",
		"user authenticated",
		"job completed",
		"retry attempt",
		"resource allocated",
		"timeout occurred",
		"validation failed",
		"data synced",
	}

	level := levels[r.rng.IntN(len(levels))]
	msg := messages[r.rng.IntN(len(messages))]
	id := r.rng.IntN(100000)
	duration := r.rng.IntN(1000)

	line := fmt.Sprintf("level=%s msg=%q id=%d duration_ms=%d", level, msg, id, duration)
	return []byte(line)
}
