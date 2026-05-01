package http

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/orchestrator"
)

// TestEventIDIdentity pins gastrolog-44b9r for the HTTP/Loki ingester.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	const ingesterID = "test-http-ingester"
	out := make(chan orchestrator.IngestMessage, 4)
	recv := New(Config{ID: ingesterID, Addr: "127.0.0.1:0"})
	go func() { _ = recv.Run(t.Context(), out) }()

	deadline := time.Now().Add(time.Second)
	for recv.Addr() == nil {
		if time.Now().After(deadline) {
			t.Fatal("listener did not bind")
		}
		time.Sleep(time.Millisecond)
	}

	ts := strconv.FormatInt(time.Now().UnixNano(), 10)
	body := `{"streams":[{"stream":{"job":"probe"},"values":[["` + ts + `","probe"]]}]}`
	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, ingesterID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for HTTP message")
	}
}
