package fluentfwd

import (
	"bytes"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

// TestFluentFwdConnectionPanicCostsOnlyThatConnection proves a panic while
// handling one Fluent Forward connection drops that connection and leaves
// the listener accepting and ingesting. Without containment the panic ends
// the process, taking every vault and Raft group on the node with it.
//
// The panic is raised from the handler's own logger on the first malformed
// frame, which puts it on the real per-connection call path without touching
// state the handler shares with anything else.
func TestFluentFwdConnectionPanicCostsOnlyThatConnection(t *testing.T) {
	t.Parallel()

	logger, logs := logtest.NewPanicking("unexpected array length", 1)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	out := make(chan ingestion.IngesterMessage, 4)
	ing := New(Config{ID: "test-fwd", Addr: addr, Logger: logger})
	go func() { _ = ing.Run(t.Context(), out) }()

	waitDialable(t, addr)

	victim, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer victim.Close()

	// A one-element array is not a Forward frame; rejecting it logs — and
	// the injected panic fires from that log call.
	var short bytes.Buffer
	_ = msgpack.NewEncoder(&short).EncodeArrayLen(1)
	if _, err := victim.Write(short.Bytes()); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = victim.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := victim.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection stayed usable after a panic in its handler")
	} else if !connectionEnded(err) {
		t.Fatalf("connection was held open after a panic in its handler: %v", err)
	}

	survivor, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("listener stopped accepting after a connection panicked: %v", err)
	}
	defer survivor.Close()

	if _, err := survivor.Write(messageModeFrame("app.log", "still here")); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case m := <-out:
		if string(m.Raw) != "still here" {
			t.Errorf("expected raw %q, got %q", "still here", m.Raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ingest stopped after a connection panicked")
	}

	if !logs.Wait(5*time.Second, "recovered panic", "fluent forward connection", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
	}
}

func messageModeFrame(tag, message string) []byte {
	var buf bytes.Buffer
	enc := msgpack.NewEncoder(&buf)
	_ = enc.EncodeArrayLen(3)
	_ = enc.EncodeString(tag)
	_ = enc.EncodeInt(time.Now().Unix())
	_ = enc.EncodeMap(map[string]any{"message": message})
	return buf.Bytes()
}

func waitDialable(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for fluentfwd listener")
		}
		runtime.Gosched()
	}
}

// connectionEnded reports whether err means the peer closed the socket, as
// opposed to the read deadline expiring on a connection still held open.
func connectionEnded(err error) bool {
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	return true
}
