package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"gastrolog/internal/orchestrator"
)

type fakeStats struct {
	depth    int
	capacity int
}

func (f *fakeStats) IngestQueueDepth() int    { return f.depth }
func (f *fakeStats) IngestQueueCapacity() int { return f.capacity }

func TestNewFactory(t *testing.T) {
	src := &fakeStats{depth: 5, capacity: 1000}
	factory := NewFactory(src)

	t.Run("default interval", func(t *testing.T) {
		ing, err := factory(uuid.New(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		m := ing.(*ingester)
		if m.interval != 30*time.Second {
			t.Errorf("got interval %v, want 30s", m.interval)
		}
	})

	t.Run("custom interval", func(t *testing.T) {
		ing, err := factory(uuid.New(), map[string]string{"interval": "10s"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		m := ing.(*ingester)
		if m.interval != 10*time.Second {
			t.Errorf("got interval %v, want 10s", m.interval)
		}
	})

	t.Run("invalid interval", func(t *testing.T) {
		_, err := factory(uuid.New(), map[string]string{"interval": "bad"}, nil)
		if err == nil {
			t.Fatal("expected error for invalid interval")
		}
	})

	t.Run("non-positive interval", func(t *testing.T) {
		_, err := factory(uuid.New(), map[string]string{"interval": "0s"}, nil)
		if err == nil {
			t.Fatal("expected error for zero interval")
		}
	})
}

func TestIngesterEmitsRecord(t *testing.T) {
	src := &fakeStats{depth: 3, capacity: 1000}
	factory := NewFactory(src)

	ing, err := factory(uuid.New(), map[string]string{"interval": "10ms"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 10)

	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx, out) }()

	// Wait for at least one message.
	select {
	case msg := <-out:
		raw := string(msg.Raw)

		for _, key := range []string{
			"cpu_percent=",
			"heap_alloc_bytes=",
			"heap_inuse_bytes=",
			"num_goroutine=",
			"ingest_queue_depth=3",
			"ingest_queue_capacity=1000",
		} {
			if !strings.Contains(raw, key) {
				t.Errorf("missing %q in raw line: %s", key, raw)
			}
		}

		if msg.Attrs["ingester_type"] != "metrics" {
			t.Errorf("got ingester_type=%q, want metrics", msg.Attrs["ingester_type"])
		}
		if msg.Attrs["level"] != "info" {
			t.Errorf("got level=%q, want info", msg.Attrs["level"])
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for message")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}
