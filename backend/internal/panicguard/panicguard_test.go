package panicguard

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

// syncBuffer collects log output written from another goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func testLogger() (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug})), buf
}

func panicInNamedFunction() {
	panic("frame marker")
}

func TestRecoverConfinesPanicToItsGoroutine(t *testing.T) {
	t.Parallel()
	logger, logs := testLogger()

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer Recover(logger, "test goroutine", "remote", "10.0.0.1")
		panicInNamedFunction()
	}()
	<-done

	out := logs.String()
	if !strings.Contains(out, "recovered panic") {
		t.Fatalf("panic was swallowed without a log line: %q", out)
	}
	if !strings.Contains(out, "frame marker") {
		t.Errorf("log does not carry the panic value: %q", out)
	}
	if !strings.Contains(out, "test goroutine") {
		t.Errorf("log does not name the goroutine class: %q", out)
	}
	if !strings.Contains(out, "10.0.0.1") {
		t.Errorf("log does not carry the caller's attrs: %q", out)
	}
	if !strings.Contains(out, "panicInNamedFunction") {
		t.Errorf("stack does not reach the panicking frame, so the log cannot be debugged: %q", out)
	}
}

func TestRecoverIsInertWithoutAPanic(t *testing.T) {
	t.Parallel()
	logger, logs := testLogger()

	func() {
		defer Recover(logger, "test goroutine")
	}()

	if logs.String() != "" {
		t.Fatalf("logged without a panic: %q", logs.String())
	}
}

func TestCallReportsPanicAsError(t *testing.T) {
	t.Parallel()
	logger, logs := testLogger()

	err := Call(logger, "test loop", func() error {
		panicInNamedFunction()
		return nil
	})

	if err == nil {
		t.Fatal("panic did not surface as an error, so the caller's failure path never runs")
	}
	if !strings.Contains(err.Error(), "frame marker") {
		t.Errorf("error does not carry the panic value: %v", err)
	}
	if !strings.Contains(logs.String(), "panicInNamedFunction") {
		t.Errorf("stack not logged: %q", logs.String())
	}
}

func TestCallPassesThroughNormalResults(t *testing.T) {
	t.Parallel()
	logger, logs := testLogger()

	sentinel := errors.New("ordinary failure")
	if err := Call(logger, "test loop", func() error { return sentinel }); !errors.Is(err, sentinel) {
		t.Fatalf("expected the callee's own error, got %v", err)
	}
	if err := Call(logger, "test loop", func() error { return nil }); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if logs.String() != "" {
		t.Fatalf("logged without a panic: %q", logs.String())
	}
}
