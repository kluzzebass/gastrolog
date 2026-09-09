package syslog

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

// TestSyslogTCPConnectionPanicCostsOnlyThatConnection proves a panic while
// handling one TCP connection drops that connection and leaves the listener
// accepting and ingesting. Without containment the panic ends the process,
// taking every vault and Raft group on the node with it.
//
// The panic is raised from the handler's own logger on the first malformed
// frame, which puts it on the real per-connection call path without touching
// state the handler shares with anything else.
func TestSyslogTCPConnectionPanicCostsOnlyThatConnection(t *testing.T) {
	logger, logs := logtest.NewPanicking("TCP read error", 1)
	out := make(chan ingestion.IngesterMessage, 4)
	recv := New(Config{TCPAddr: "127.0.0.1:0", Logger: logger})

	go func() { _ = recv.Run(t.Context(), out) }()
	waitAddr(t, recv.TCPAddr)
	addr := recv.TCPAddr().String()

	victim, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer victim.Close()

	// An octet count that is not a number fails framing, which logs — and
	// the injected panic fires from that log call.
	if _, err := victim.Write([]byte("12x boom\n")); err != nil {
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

	msg := "<34>Jan 15 10:22:15 host app: still here"
	if _, err := survivor.Write([]byte(msg + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}
	select {
	case m := <-out:
		if string(m.Raw) != msg {
			t.Errorf("expected raw %q, got %q", msg, m.Raw)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ingest stopped after a connection panicked")
	}

	if !logs.Wait(5*time.Second, "recovered panic", "syslog TCP connection", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
	}
}

// TestSyslogUDPListenerPanicFailsTheRun proves a panic in the UDP read loop
// is reported as a run failure rather than ending the process. The ingester
// manager retries a failed run, so the listener comes back; a panic that was
// swallowed instead would leave the ingester silently deaf.
func TestSyslogUDPListenerPanicFailsTheRun(t *testing.T) {
	logger, logs := logtest.NewPanicking("syslog UDP listener starting", 1)
	out := make(chan ingestion.IngesterMessage, 4)
	recv := New(Config{UDPAddr: "127.0.0.1:0", Logger: logger})

	runErr := make(chan error, 1)
	go func() { runErr <- recv.Run(t.Context(), out) }()

	select {
	case err := <-runErr:
		if err == nil {
			t.Fatal("panic in the UDP loop was swallowed; the run reported success")
		}
		if !strings.Contains(err.Error(), "panicked") {
			t.Errorf("run error does not identify the panic: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run never returned after the UDP loop panicked")
	}

	if !logs.Wait(5*time.Second, "recovered panic", "syslog UDP listener", "stack") {
		t.Errorf("panic was contained but not reported with a stack: %s", logs)
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
