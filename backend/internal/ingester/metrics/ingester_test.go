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
	depth     int
	capacity  int
	snapshots []orchestrator.StoreSnapshot
}

func (f *fakeStats) IngestQueueDepth() int    { return f.depth }
func (f *fakeStats) IngestQueueCapacity() int { return f.capacity }
func (f *fakeStats) StoreSnapshots() []orchestrator.StoreSnapshot {
	return f.snapshots
}

func TestNewFactory(t *testing.T) {
	src := &fakeStats{depth: 5, capacity: 1000}
	factory := NewFactory(src)

	t.Run("default intervals", func(t *testing.T) {
		ing, err := factory(uuid.New(), nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		m := ing.(*ingester)
		if m.interval != 30*time.Second {
			t.Errorf("got interval %v, want 30s", m.interval)
		}
		if m.storeInterval != 10*time.Second {
			t.Errorf("got storeInterval %v, want 10s", m.storeInterval)
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

	t.Run("custom store_interval", func(t *testing.T) {
		ing, err := factory(uuid.New(), map[string]string{"store_interval": "5s"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		m := ing.(*ingester)
		if m.storeInterval != 5*time.Second {
			t.Errorf("got storeInterval %v, want 5s", m.storeInterval)
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

	t.Run("invalid store_interval", func(t *testing.T) {
		_, err := factory(uuid.New(), map[string]string{"store_interval": "bad"}, nil)
		if err == nil {
			t.Fatal("expected error for invalid store_interval")
		}
	})

	t.Run("non-positive store_interval", func(t *testing.T) {
		_, err := factory(uuid.New(), map[string]string{"store_interval": "-1s"}, nil)
		if err == nil {
			t.Fatal("expected error for negative store_interval")
		}
	})
}

func TestSystemMetrics(t *testing.T) {
	src := &fakeStats{depth: 3, capacity: 1000}
	factory := NewFactory(src)

	// Use a long store_interval so only system fires in time.
	ing, err := factory(uuid.New(), map[string]string{
		"interval":       "10ms",
		"store_interval": "1h",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 10)
	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx, out) }()

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
		if msg.Attrs["metric_type"] != "system" {
			t.Errorf("got metric_type=%q, want system", msg.Attrs["metric_type"])
		}
		if msg.Attrs["level"] != "info" {
			t.Errorf("got level=%q, want info", msg.Attrs["level"])
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for system message")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestStoreMetrics(t *testing.T) {
	storeID := uuid.New()
	src := &fakeStats{
		depth:    0,
		capacity: 100,
		snapshots: []orchestrator.StoreSnapshot{
			{
				ID:           storeID,
				RecordCount:  42,
				ChunkCount:   3,
				SealedChunks: 2,
				DataBytes:    1024,
				Enabled:      true,
			},
		},
	}
	factory := NewFactory(src)

	// Use a long system interval so only store fires in time.
	ing, err := factory(uuid.New(), map[string]string{
		"interval":       "1h",
		"store_interval": "10ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 10)
	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx, out) }()

	select {
	case msg := <-out:
		raw := string(msg.Raw)
		for _, key := range []string{
			"record_count=42",
			"chunk_count=3",
			"sealed_chunks=2",
			"data_bytes=1024",
			"enabled=true",
		} {
			if !strings.Contains(raw, key) {
				t.Errorf("missing %q in raw line: %s", key, raw)
			}
		}
		if msg.Attrs["metric_type"] != "store" {
			t.Errorf("got metric_type=%q, want store", msg.Attrs["metric_type"])
		}
		if msg.Attrs["store_id"] != storeID.String() {
			t.Errorf("got store_id=%q, want %s", msg.Attrs["store_id"], storeID)
		}
		if msg.Attrs["ingester_type"] != "metrics" {
			t.Errorf("got ingester_type=%q, want metrics", msg.Attrs["ingester_type"])
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for store message")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestDualTickerIndependence(t *testing.T) {
	src := &fakeStats{
		depth:    1,
		capacity: 100,
		snapshots: []orchestrator.StoreSnapshot{
			{ID: uuid.New(), RecordCount: 10, ChunkCount: 1, Enabled: true},
		},
	}
	factory := NewFactory(src)

	// Store interval is shorter, so store metrics should arrive first.
	ing, err := factory(uuid.New(), map[string]string{
		"interval":       "200ms",
		"store_interval": "10ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 20)
	done := make(chan error, 1)
	go func() { done <- ing.Run(ctx, out) }()

	// First message should be a store metric.
	select {
	case msg := <-out:
		if msg.Attrs["metric_type"] != "store" {
			t.Errorf("expected first message metric_type=store, got %q", msg.Attrs["metric_type"])
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for first message")
	}

	// Collect more messages until we see a system metric.
	sawSystem := false
	for range 50 {
		select {
		case msg := <-out:
			if msg.Attrs["metric_type"] == "system" {
				sawSystem = true
			}
		case <-ctx.Done():
		}
		if sawSystem {
			break
		}
	}
	if !sawSystem {
		t.Error("never received a system metric")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
}

func TestCollectStoresEmpty(t *testing.T) {
	src := &fakeStats{depth: 0, capacity: 100}
	m := &ingester{
		id:            "test",
		interval:      time.Second,
		storeInterval: time.Second,
		src:           src,
	}
	msgs := m.collectStores()
	if len(msgs) != 0 {
		t.Errorf("expected 0 messages for empty snapshots, got %d", len(msgs))
	}
}

func TestParamDefaults(t *testing.T) {
	defaults := ParamDefaults()
	if defaults["interval"] != "30s" {
		t.Errorf("got interval default %q, want 30s", defaults["interval"])
	}
	if defaults["store_interval"] != "10s" {
		t.Errorf("got store_interval default %q, want 10s", defaults["store_interval"])
	}
}
