package http

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/orchestrator"
)

func TestHTTPReceiverPlainText(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start receiver in background.
	errCh := make(chan error, 1)
	go func() {
		errCh <- recv.Run(ctx, out)
	}()

	// Wait for server to start.
	time.Sleep(50 * time.Millisecond)

	// Send plain text message.
	resp, err := http.Post("http://"+recv.Addr().String()+"/ingest", "text/plain", strings.NewReader("hello world"))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 202, got %d: %s", resp.StatusCode, body)
	}

	// Check message was received.
	select {
	case msg := <-out:
		if string(msg.Raw) != "hello world" {
			t.Errorf("expected 'hello world', got %q", msg.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestHTTPReceiverPlainTextMultiline(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Send multi-line message.
	resp, err := http.Post("http://"+recv.Addr().String()+"/ingest", "text/plain", strings.NewReader("line1\nline2\nline3"))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	// Check we got 3 messages.
	var msgs []string
	for i := 0; i < 3; i++ {
		select {
		case msg := <-out:
			msgs = append(msgs, string(msg.Raw))
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for message")
		}
	}

	expected := []string{"line1", "line2", "line3"}
	for i, exp := range expected {
		if msgs[i] != exp {
			t.Errorf("message %d: expected %q, got %q", i, exp, msgs[i])
		}
	}
}

func TestHTTPReceiverJSON(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Send JSON message with raw and attrs.
	body := `{"raw": "test message", "attrs": {"host": "server1", "level": "error"}}`
	resp, err := http.Post("http://"+recv.Addr().String()+"/ingest", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	select {
	case msg := <-out:
		if string(msg.Raw) != "test message" {
			t.Errorf("expected 'test message', got %q", msg.Raw)
		}
		if msg.Attrs["host"] != "server1" {
			t.Errorf("expected host=server1, got %q", msg.Attrs["host"])
		}
		if msg.Attrs["level"] != "error" {
			t.Errorf("expected level=error, got %q", msg.Attrs["level"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestHTTPReceiverJSONArray(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Send array of messages.
	body := `[{"raw": "msg1"}, {"raw": "msg2"}, {"raw": "msg3"}]`
	resp, err := http.Post("http://"+recv.Addr().String()+"/ingest", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	// Check we got 3 messages.
	for i := 1; i <= 3; i++ {
		select {
		case msg := <-out:
			expected := "msg" + string(rune('0'+i))
			if string(msg.Raw) != expected {
				t.Errorf("message %d: expected %q, got %q", i, expected, msg.Raw)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for message")
		}
	}
}

func TestHTTPReceiverJSONRawObject(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Send arbitrary JSON object (no "raw" field) - should be stored as-is.
	body := `{"level": "error", "message": "something went wrong", "code": 500}`
	resp, err := http.Post("http://"+recv.Addr().String()+"/ingest", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	select {
	case msg := <-out:
		// Raw should be the entire JSON object.
		if !bytes.Contains(msg.Raw, []byte(`"level": "error"`)) {
			t.Errorf("expected raw to contain original JSON, got %q", msg.Raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestHTTPReceiverHeaderAttrs(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Send message with X-Attrs-* headers.
	req, _ := http.NewRequest("POST", "http://"+recv.Addr().String()+"/ingest", strings.NewReader("test"))
	req.Header.Set("Content-Type", "text/plain")
	req.Header.Set("X-Attrs-Host", "server1")
	req.Header.Set("X-Attrs-Env", "prod")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	select {
	case msg := <-out:
		if msg.Attrs["Host"] != "server1" {
			t.Errorf("expected Host=server1, got %q", msg.Attrs["Host"])
		}
		if msg.Attrs["Env"] != "prod" {
			t.Errorf("expected Env=prod, got %q", msg.Attrs["Env"])
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for message")
	}
}

func TestHTTPReceiverWaitAck(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Start request with X-Wait-Ack header in background.
	respCh := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequest("POST", "http://"+recv.Addr().String()+"/ingest", strings.NewReader("ack test"))
		req.Header.Set("Content-Type", "text/plain")
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
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			t.Errorf("expected 200, got %d: %s", resp.StatusCode, body)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for response")
	}
}

func TestHTTPReceiverWaitAckError(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	// Start request with X-Wait-Ack header in background.
	respCh := make(chan *http.Response, 1)
	go func() {
		req, _ := http.NewRequest("POST", "http://"+recv.Addr().String()+"/ingest", strings.NewReader("ack error test"))
		req.Header.Set("Content-Type", "text/plain")
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

func TestHTTPReceiverEmptyBody(t *testing.T) {
	out := make(chan orchestrator.IngestMessage, 10)
	recv := New(Config{Addr: "127.0.0.1:0"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go recv.Run(ctx, out)
	time.Sleep(50 * time.Millisecond)

	resp, err := http.Post("http://"+recv.Addr().String()+"/ingest", "text/plain", strings.NewReader(""))
	if err != nil {
		t.Fatalf("POST failed: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", resp.StatusCode)
	}
}
