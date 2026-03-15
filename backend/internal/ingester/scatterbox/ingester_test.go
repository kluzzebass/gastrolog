package scatterbox

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"gastrolog/internal/orchestrator"

	"github.com/google/uuid"
)

func TestEmitsSequentialRecords(t *testing.T) {
	ing, err := NewIngester(uuid.Must(uuid.NewV7()), map[string]string{
		"interval": "1ms",
		"burst":    "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 100)
	done := make(chan struct{})
	go func() {
		_ = ing.Run(ctx, out)
		close(done)
	}()

	<-done // Run exited — no more sends on out

	var lastSeq uint64
	count := 0
	for len(out) > 0 {
		msg := <-out
		count++
		var body struct {
			Seq         uint64 `json:"seq"`
			GeneratedAt string `json:"generated_at"`
			Ingester    string `json:"ingester"`
		}
		if err := json.Unmarshal(msg.Raw, &body); err != nil {
			t.Fatalf("record %d: invalid JSON: %v", count, err)
		}
		if body.Seq <= lastSeq && count > 1 {
			t.Errorf("record %d: seq %d <= previous %d", count, body.Seq, lastSeq)
		}
		lastSeq = body.Seq

		if msg.Attrs["ingester_type"] != "scatterbox" {
			t.Errorf("record %d: ingester_type = %q, want scatterbox", count, msg.Attrs["ingester_type"])
		}
		if msg.Attrs["seq"] == "" {
			t.Errorf("record %d: missing seq attr", count)
		}
	}

	if count == 0 {
		t.Error("no records emitted")
	}
}

func TestBurstMode(t *testing.T) {
	ing, err := NewIngester(uuid.Must(uuid.NewV7()), map[string]string{
		"interval": "10ms",
		"burst":    "5",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 100)
	done := make(chan struct{})
	go func() {
		_ = ing.Run(ctx, out)
		close(done)
	}()

	<-done // Run exited — no more sends on out

	count := len(out)

	// At 10ms interval with burst=5, should get at least 5 records in 30ms.
	if count < 5 {
		t.Errorf("expected at least 5 records, got %d", count)
	}
}
