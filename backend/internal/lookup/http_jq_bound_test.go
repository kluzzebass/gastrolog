package lookup

// A response-path expression reaches the jq engine from a lookup
// configuration, so it is bounded in both directions a jq program can run
// away: emitting without end, and never finishing.

import (
	"context"
	"testing"
	"time"

	"github.com/itchyny/gojq"
)

func TestJQSelectCapsWhatOneExpressionEmits(t *testing.T) {
	t.Parallel()
	// range emits until something stops it.
	unbounded, err := compileRaw("range(1000000000)")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	got := jqSelect(context.Background(), unbounded, map[string]any{})
	if len(got) != maxJQResults {
		t.Fatalf("collected %d values, want the cap of %d", len(got), maxJQResults)
	}
}

func TestJQSelectStopsWhenTheContextEnds(t *testing.T) {
	t.Parallel()
	// A program that neither finishes nor emits enough to hit the count cap
	// has to be stopped by the context the fetch already carries.
	slow, err := compileRaw("[range(100000000)] | length")
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan []any, 1)
	go func() { done <- jqSelect(ctx, slow, map[string]any{}) }()

	select {
	case got := <-done:
		if len(got) != 0 {
			t.Fatalf("a cancelled program returned %d values, want none", len(got))
		}
	case <-time.After(30 * time.Second):
		t.Fatal("a cancelled jq program kept running; the context does not bound it")
	}
}

// compileRaw compiles a jq program directly, bypassing the JSONPath
// translation, so a test can express a runaway program.
func compileRaw(expr string) (*gojq.Code, error) {
	parsed, err := gojq.Parse(expr)
	if err != nil {
		return nil, err
	}
	return gojq.Compile(parsed)
}
