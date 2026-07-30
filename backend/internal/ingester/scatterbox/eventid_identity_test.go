package scatterbox

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/pipeline/ingestion"
)

// TestEventIDIdentity pins the identity invariant: every IngestMessage
// emitted by the scatterbox ingester carries the configured IngesterID and
// a non-zero IngestTS.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	id := glid.New()
	ing, err := NewIngester(id, map[string]string{
		"interval": "5ms",
		"burst":    "1",
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
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scatterbox message")
	}
}
