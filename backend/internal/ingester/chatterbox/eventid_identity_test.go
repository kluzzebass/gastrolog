package chatterbox

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/orchestrator"
)

// TestEventIDIdentity pins gastrolog-44b9r: every IngestMessage emitted
// by the chatterbox ingester carries the configured IngesterID and a
// non-zero IngestTS — the orchestrator's digest path needs both to
// stamp a complete EventID downstream.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	id := glid.New()
	ing, err := NewIngester(id, map[string]string{
		// Tight interval so the test captures a message quickly.
		"min_interval": "1ms",
		"max_interval": "2ms",
	}, nil)
	if err != nil {
		t.Fatalf("NewIngester: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	out := make(chan orchestrator.IngestMessage, 4)
	go func() { _ = ing.Run(ctx, out) }()

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, id.String())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for chatterbox message")
	}
}
