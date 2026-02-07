// Package chatterbox provides a ingester that emits random log messages
// at random intervals. It is used to exercise the full ingest pipeline.
package chatterbox

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"gastrolog/internal/orchestrator"
)

// Ingester emits random log-like messages at random intervals.
// It implements orchestrator.Ingester.
//
// Logging:
//   - Logger is dependency-injected via the factory
//   - Ingester owns its scoped logger (component="ingester", type="chatterbox")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in the message generation loop
type Ingester struct {
	id          string
	minInterval time.Duration
	maxInterval time.Duration
	rng         *rand.Rand

	// formats holds the available log format generators.
	formats []LogFormat
	// weights holds the cumulative weights for format selection.
	// weights[i] = sum of weights[0..i], used for weighted random selection.
	weights []int
	// totalWeight is the sum of all format weights.
	totalWeight int

	// Logger for this ingester instance.
	// Scoped with component="ingester", type="chatterbox" at construction time.
	logger *slog.Logger
}

// Run starts the ingester and emits messages to the output channel.
// Run blocks until ctx is cancelled. Returns nil on normal cancellation.
func (r *Ingester) Run(ctx context.Context, out chan<- orchestrator.IngestMessage) error {
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
func (r *Ingester) randomInterval() time.Duration {
	if r.minInterval >= r.maxInterval {
		return r.minInterval
	}
	delta := r.maxInterval - r.minInterval
	return r.minInterval + time.Duration(r.rng.Int64N(int64(delta)))
}

// generateMessage creates a random log-like message.
func (r *Ingester) generateMessage() orchestrator.IngestMessage {
	// Select a format using weighted random selection.
	format := r.selectFormat()

	// Generate raw bytes, format-specific attributes, and source timestamp.
	raw, formatAttrs, sourceTS := format.Generate(r.rng)

	// Merge base attrs with format attrs.
	attrs := make(map[string]string, len(formatAttrs)+2)
	for k, v := range formatAttrs {
		attrs[k] = v
	}
	attrs["ingester_type"] = "chatterbox"
	attrs["ingester_id"] = r.id

	return orchestrator.IngestMessage{
		Attrs:    attrs,
		Raw:      raw,
		SourceTS: sourceTS,
		IngestTS: time.Now(),
	}
}

// selectFormat returns a randomly selected format based on weights.
func (r *Ingester) selectFormat() LogFormat {
	if len(r.formats) == 1 {
		return r.formats[0]
	}

	n := r.rng.IntN(r.totalWeight)
	for i, w := range r.weights {
		if n < w {
			return r.formats[i]
		}
	}
	// Fallback (shouldn't happen if weights are set up correctly).
	return r.formats[len(r.formats)-1]
}
