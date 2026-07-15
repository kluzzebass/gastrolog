package scatterbox

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/orchestrator"
)

func TestNextEmissionAlignsToEpoch(t *testing.T) {
	t.Parallel()
	interval := 100 * time.Millisecond
	base := time.Unix(1_000, 37_000_000) // 37ms past a second boundary
	got := nextEmission(base, interval)
	want := time.Unix(1_000, 100_000_000)
	if !got.Equal(want) {
		t.Fatalf("nextEmission() = %v, want %v", got, want)
	}
}

func TestNextEmissionExactBoundaryAdvancesOneStep(t *testing.T) {
	t.Parallel()
	interval := 100 * time.Millisecond
	base := time.Unix(0, 200_000_000)
	got := nextEmission(base, interval)
	want := time.Unix(0, 300_000_000)
	if !got.Equal(want) {
		t.Fatalf("nextEmission() = %v, want %v", got, want)
	}
}

func TestScheduledEmissionUsesAlignedTimestamp(t *testing.T) {
	interval := 100 * time.Millisecond
	ing, err := NewIngester(glid.New(), map[string]string{
		"interval": interval.String(),
		"burst":    "1",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	out := make(chan orchestrator.IngestMessage, 1)
	done := make(chan struct{})
	go func() {
		_ = ing.Run(ctx, out)
		close(done)
	}()
	<-done

	if len(out) != 1 {
		t.Fatalf("records = %d, want 1", len(out))
	}
	msg := <-out
	var body struct {
		GeneratedAt string `json:"generated_at"`
	}
	if err := json.Unmarshal(msg.Raw, &body); err != nil {
		t.Fatal(err)
	}
	ts, err := time.Parse(time.RFC3339Nano, body.GeneratedAt)
	if err != nil {
		t.Fatal(err)
	}
	if ts.UnixNano()%interval.Nanoseconds() != 0 {
		t.Fatalf("generated_at %v not aligned to %v boundary", ts, interval)
	}
}
