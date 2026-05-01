package tail

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/glid"
	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/orchestrator"
)

// TestEventIDIdentity pins gastrolog-44b9r for the file-tail ingester.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logFile := filepath.Join(dir, "app.log")
	if err := os.WriteFile(logFile, nil, 0o644); err != nil {
		t.Fatal(err)
	}

	id := glid.New()
	factory := NewFactory()
	ing, err := factory(id, map[string]string{
		"paths":         `["` + filepath.Join(dir, "*.log") + `"]`,
		"poll_interval": "10ms",
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan orchestrator.IngestMessage, 4)
	go func() { _ = ing.Run(ctx, out) }()

	// Append a line so the tail emits.
	time.Sleep(50 * time.Millisecond)
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString("identity probe\n"); err != nil {
		t.Fatal(err)
	}
	f.Close()

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, id.String())
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for tail message")
	}
}
