package http

import (
	"bytes"
	"context"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

func startBoundedHTTP(t *testing.T) (string, chan ingestion.IngesterMessage, *logtest.Recorder) {
	t.Helper()
	logger, logs := logtest.New()
	out := make(chan ingestion.IngesterMessage, 4)
	ing := New(Config{ID: "test-http", Addr: "127.0.0.1:0", Logger: logger})

	go func() { _ = ing.Run(t.Context(), out) }()

	addr := ing.Addr().String()
	deadline := time.Now().Add(2 * time.Second)
	client := &http.Client{Timeout: 50 * time.Millisecond}
	for {
		resp, err := client.Get("http://" + addr + "/ready")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the http listener")
		}
		runtime.Gosched()
	}
	return addr, out, logs
}

// TestPushBombIsRejectedAndReported proves a small compressed push body that
// expands past the request ceiling is refused with a 400 and reported in the
// node's log — a sender whose batch never lands must be able to find out
// why.
func TestPushBombIsRejectedAndReported(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedHTTP(t)

	enc, err := zstd.NewWriter(nil)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}
	bomb := enc.EncodeAll(make([]byte, maxPushBodyBytes*8), nil)
	_ = enc.Close()

	// The premise: on the wire this is well inside the limit, so a bound
	// on the compressed input would let it through.
	if int64(len(bomb)) > maxPushBodyBytes {
		t.Fatalf("the bomb is not a bomb: %d compressed bytes already exceed the limit", len(bomb))
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://"+addr+"/loki/api/v1/push", bytes.NewReader(bomb))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Encoding", "zstd")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for an oversize body, got %d", resp.StatusCode)
	}
	select {
	case msg := <-out:
		t.Fatalf("an oversize body reached the pipeline: %q", msg.Raw)
	default:
	}
	if !logs.Wait(5*time.Second, "push body rejected") {
		t.Errorf("the rejection was silent: %s", logs)
	}
}

// TestConformingPushIsUnaffected proves the bounds did not change what a
// normal Loki sender gets back.
func TestConformingPushIsUnaffected(t *testing.T) {
	t.Parallel()
	addr, out, _ := startBoundedHTTP(t)

	body := `{"streams":[{"stream":{"app":"api"},"values":[["1700000000000000000","hello"]]}]}`
	resp, err := http.Post("http://"+addr+"/loki/api/v1/push", "application/json", strings.NewReader(body)) //nolint:noctx // short-lived test request
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}
	select {
	case msg := <-out:
		if string(msg.Raw) != "hello" {
			t.Errorf("payload altered: %q", msg.Raw)
		}
		if msg.Attrs["app"] != "api" {
			t.Errorf("stream label lost: %v", msg.Attrs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a conforming push was not ingested")
	}
}
