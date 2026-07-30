package metrics

import (
	"context"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/pipeline/ingestion"
)

// TestEventIDIdentity pins the identity invariant for the metrics ingester:
// the configured IngesterID and a non-zero IngestTS on every message.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	src := &fakeStats{depth: 1, capacity: 1000}
	factory := NewFactory(src)

	id := glid.New()
	ing, err := factory(id, map[string]string{
		"interval":       "10ms",
		"vault_interval": "1h",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	out := make(chan ingestion.IngesterMessage, 4)
	go func() { _ = ing.Run(ctx, out) }()

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, id.String())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for metrics message")
	}
}
