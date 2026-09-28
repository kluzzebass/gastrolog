package chatterbox

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/pipeline/ingestion"
	"gastrolog/internal/waittest"
)

// TestEventIDIdentity pins the identity invariant: every IngestMessage
// emitted by the chatterbox ingester carries the configured IngesterID and
// a non-zero IngestTS — the orchestrator's digest path needs both to stamp
// a complete EventID downstream.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	id := glid.New()
	ing, err := NewIngester(id, map[string]string{
		// Tight interval so the test captures a message quickly. The keys are
		// camelCase because that is what this factory reads — snake_case here
		// is silently ignored and leaves the 100ms-1s defaults in force.
		"minInterval": "1ms",
		"maxInterval": "2ms",
	}, nil)
	if err != nil {
		t.Fatalf("NewIngester: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := make(chan ingestion.IngesterMessage, 4)
	go func() { _ = ing.Run(ctx, out) }()

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, id.String())
	case <-time.After(waittest.Failsafe):
		t.Fatal("timed out waiting for chatterbox message")
	}
}
