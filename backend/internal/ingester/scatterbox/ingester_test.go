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
	go ing.Run(ctx, out) //nolint:errcheck // test

	<-ctx.Done()
	close(out)

	var lastSeq uint64
	count := 0
	for msg := range out {
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
	go ing.Run(ctx, out) //nolint:errcheck // test

	<-ctx.Done()
	close(out)

	count := 0
	for range out {
		count++
	}

	// At 10ms interval with burst=5, should get at least 5 records in 30ms.
	if count < 5 {
		t.Errorf("expected at least 5 records, got %d", count)
	}
}

func TestParamValidation(t *testing.T) {
	tests := []struct {
		name   string
		params map[string]string
		errMsg string
	}{
		{"negative interval", map[string]string{"interval": "-1s"}, "must be non-negative"},
		{"invalid interval", map[string]string{"interval": "abc"}, "invalid interval"},
		{"zero burst", map[string]string{"burst": "0"}, "must be positive"},
		{"negative burst", map[string]string{"burst": "-1"}, "must be positive"},
		{"invalid burst", map[string]string{"burst": "abc"}, "invalid burst"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewIngester(uuid.Must(uuid.NewV7()), tt.params, nil)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}
