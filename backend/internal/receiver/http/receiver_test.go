package http

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/orchestrator"
)

func TestLokiPushSingleStream(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	body := `{
		"streams": [{
			"stream": {"host": "server1", "job": "app"},
			"values": [
				["` + strconv.FormatInt(ts, 10) + `", "hello world"]
			]
		}]
	}`

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		respBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 204, got %d: %s", resp.StatusCode, respBody)
	}

	select {
	case msg := <-out:
		if string(msg.Raw) != "hello world" {
			t.Errorf("expected 'hello world', got %q", msg.Raw)
		}
		if msg.Attrs["host"] != "server1" {
			t.Errorf("expected host=server1, got %q", msg.Attrs["host"])
		}
		if msg.Attrs["job"] != "app" {
			t.Errorf("expected job=app, got %q", msg.Attrs["job"])
		}
		// Check source timestamp was parsed from the Loki payload.
		if msg.SourceTS.UnixNano() != ts {
			t.Errorf("expected SourceTS %d, got %d", ts, msg.SourceTS.UnixNano())
		}
		// IngestTS should be set to current time (approximately).
		if time.Since(msg.IngestTS) > time.Second {
			t.Errorf("IngestTS should be recent, got %v", msg.IngestTS)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestLokiPushMultipleValues(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	body := `{
		"streams": [{
			"stream": {"host": "server1"},
			"values": [
				["` + strconv.FormatInt(ts, 10) + `", "line1"],
				["` + strconv.FormatInt(ts+1, 10) + `", "line2"],
				["` + strconv.FormatInt(ts+2, 10) + `", "line3"]
			]
		}]
	}`

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	// Check we got 3 messages.
	expected := []string{"line1", "line2", "line3"}
	for i, exp := range expected {
		select {
		case msg := <-out:
			if string(msg.Raw) != exp {
				t.Errorf("message %d: expected %q, got %q", i, exp, msg.Raw)
			}
			if msg.Attrs["host"] != "server1" {
				t.Errorf("message %d: expected host=server1, got %q", i, msg.Attrs["host"])
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for message %d", i)
		}
	}
}

func TestLokiPushMultipleStreams(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	body := `{
		"streams": [
			{
				"stream": {"host": "server1"},
				"values": [["` + strconv.FormatInt(ts, 10) + `", "from server1"]]
			},
			{
				"stream": {"host": "server2"},
				"values": [["` + strconv.FormatInt(ts, 10) + `", "from server2"]]
			}
		]
	}`

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	// Check we got 2 messages with different hosts.
	hosts := make(map[string]string)
	for i := 0; i < 2; i++ {
		select {
		case msg := <-out:
			hosts[msg.Attrs["host"]] = string(msg.Raw)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for message")
		}
	}

	if hosts["server1"] != "from server1" {
		t.Errorf("expected 'from server1' for server1, got %q", hosts["server1"])
	}
	if hosts["server2"] != "from server2" {
		t.Errorf("expected 'from server2' for server2, got %q", hosts["server2"])
	}
}

func TestLokiPushStructuredMetadata(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	// Third element is structured metadata.
	body := `{
		"streams": [{
			"stream": {"host": "server1"},
			"values": [
				["` + strconv.FormatInt(ts, 10) + `", "log with metadata", {"trace_id": "abc123", "user_id": "user42"}]
			]
		}]
	}`

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	select {
	case msg := <-out:
		if string(msg.Raw) != "log with metadata" {
			t.Errorf("expected 'log with metadata', got %q", msg.Raw)
		}
		// Stream label.
		if msg.Attrs["host"] != "server1" {
			t.Errorf("expected host=server1, got %q", msg.Attrs["host"])
		}
		// Structured metadata.
		if msg.Attrs["trace_id"] != "abc123" {
			t.Errorf("expected trace_id=abc123, got %q", msg.Attrs["trace_id"])
		}
		if msg.Attrs["user_id"] != "user42" {
			t.Errorf("expected user_id=user42, got %q", msg.Attrs["user_id"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestLokiPushGzipCompression(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	body := `{"streams": [{"stream": {"host": "server1"}, "values": [["` + strconv.FormatInt(ts, 10) + `", "gzipped message"]]}]}`

	// Gzip compress the body.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write([]byte(body))
	gz.Close()

	req, _ := http.NewRequest("POST", "http://"+recv.Addr().String()+"/loki/api/v1/push", &buf)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "gzip")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	select {
	case msg := <-out:
		if string(msg.Raw) != "gzipped message" {
			t.Errorf("expected 'gzipped message', got %q", msg.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestLokiPushWaitAck(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	body := `{"streams": [{"stream": {}, "values": [["` + strconv.FormatInt(ts, 10) + `", "ack test"]]}]}`

	// Start request with X-Wait-Ack header in background.
	respCh := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequest("POST", "http://"+recv.Addr().String()+"/loki/api/v1/push", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Wait-Ack", "true")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("POST failed: %v", err)
			return
		}
		respCh <- resp
	}()

	// Receive message and send ack.
	select {
	case msg := <-out:
		if string(msg.Raw) != "ack test" {
			t.Errorf("expected 'ack test', got %q", msg.Raw)
		}
		if msg.Ack == nil {
			t.Fatal("expected Ack channel to be set")
		}
		// Send success ack.
		msg.Ack <- nil
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}

	// Check response.
	select {
	case resp := <-respCh:
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusNoContent {
			respBody, _ := io.ReadAll(resp.Body)
			t.Errorf("expected 204, got %d: %s", resp.StatusCode, respBody)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for response")
	}
}

func TestLokiPushWaitAckError(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	body := `{"streams": [{"stream": {}, "values": [["` + strconv.FormatInt(ts, 10) + `", "ack error test"]]}]}`

	// Start request with X-Wait-Ack header in background.
	respCh := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequest("POST", "http://"+recv.Addr().String()+"/loki/api/v1/push", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Wait-Ack", "true")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Errorf("POST failed: %v", err)
			return
		}
		respCh <- resp
	}()

	// Receive message and send error ack.
	select {
	case msg := <-out:
		msg.Ack <- io.ErrUnexpectedEOF // Simulate an error.
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}

	// Check response indicates error.
	select {
	case resp := <-respCh:
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("expected 500, got %d", resp.StatusCode)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for response")
	}
}

func TestLokiPushLegacyEndpoint(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	ts := time.Now().UnixNano()
	body := `{"streams": [{"stream": {}, "values": [["` + strconv.FormatInt(ts, 10) + `", "legacy endpoint"]]}]}`

	// Use legacy /api/prom/push endpoint.
	resp, err := http.Post("http://"+recv.Addr().String()+"/api/prom/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", resp.StatusCode)
	}

	select {
	case msg := <-out:
		if string(msg.Raw) != "legacy endpoint" {
			t.Errorf("expected 'legacy endpoint', got %q", msg.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestLokiPushEmptyStreams(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Empty streams array - Loki returns 204.
	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(`{"streams": []}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("expected 204, got %d", resp.StatusCode)
	}
}

func TestLokiPushInvalidJSON(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(`{invalid json`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestLokiPushInvalidTimestamp(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Timestamp as number instead of string (Loki requires string).
	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json",
		strings.NewReader(`{"streams": [{"stream": {}, "values": [[1234567890, "test"]]}]}`))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}

func TestReadyEndpoint(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Get("http://" + recv.Addr().String() + "/ready")
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
}

func TestLokiPushTooManyAttrs(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Build stream with too many labels.
	labels := make(map[string]string)
	for i := 0; i < 50; i++ {
		labels[fmt.Sprintf("key%d", i)] = fmt.Sprintf("value%d", i)
	}
	labelsJSON, _ := json.Marshal(labels)

	ts := time.Now().UnixNano()
	body := fmt.Sprintf(`{"streams": [{"stream": %s, "values": [["%d", "test"]]}]}`, labelsJSON, ts)

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for too many attrs, got %d", resp.StatusCode)
	}
}

func TestLokiPushAttrKeyTooLong(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Key longer than 64 chars.
	longKey := strings.Repeat("x", 100)
	ts := time.Now().UnixNano()
	body := fmt.Sprintf(`{"streams": [{"stream": {"%s": "value"}, "values": [["%d", "test"]]}]}`, longKey, ts)

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for key too long, got %d", resp.StatusCode)
	}
}

func TestLokiPushAttrValueTooLong(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Value longer than 256 chars.
	longValue := strings.Repeat("x", 300)
	ts := time.Now().UnixNano()
	body := fmt.Sprintf(`{"streams": [{"stream": {"key": "%s"}, "values": [["%d", "test"]]}]}`, longValue, ts)

	resp, err := http.Post("http://"+recv.Addr().String()+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 for value too long, got %d", resp.StatusCode)
	}
}
