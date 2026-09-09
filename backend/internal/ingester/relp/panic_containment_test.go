package relp

import (
	"bufio"
	"context"
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

func waitForAddr(t *testing.T, ing *Ingester) net.Addr {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if addr := ing.Addr(); addr != nil {
			return addr
		}
		if time.Now().After(deadline) {
			t.Fatal("listener did not start")
		}
		runtime.Gosched()
	}
}

func relpOpen(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	writeRELPFrame(conn, 1, "open", "relp_version=0\nrelp_software=test\ncommands=syslog")
	if _, cmd, _, err := readRELPResponse(reader); err != nil || cmd != "rsp" {
		t.Fatalf("open handshake failed: cmd=%q err=%v", cmd, err)
	}
}

// TestRELPConnectionPanicCostsOnlyThatConnection proves the blast radius of a
// panic inside a RELP connection handler is that connection: the panic is
// logged with a stack, the client's socket drops, and the listener keeps
// accepting. Without containment the panic ends the process — here the test
// binary, in production the node and every vault and Raft group on it.
//
// The panic is raised from the handler's own logger on the first connection,
// which puts it on the real per-connection call path without touching state
// the handler shares with anything else.
func TestRELPConnectionPanicCostsOnlyThatConnection(t *testing.T) {
	t.Parallel()

	logger, logs := logtest.NewPanicking("RELP session established", 1)
	out := make(chan ingestion.IngesterMessage, 4)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0", Logger: logger})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = ing.Run(ctx, out) }()

	addr := waitForAddr(t, ing)

	victim, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer victim.Close()

	// The connection must die rather than hang.
	_ = victim.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := bufio.NewReader(victim).ReadByte(); err == nil {
		t.Fatal("connection stayed usable after a panic in its handler")
	} else if !connectionEnded(err) {
		t.Fatalf("connection was held open after a panic in its handler: %v", err)
	}

	// The listener must still be serving, and the next connection must
	// work end to end.
	survivor, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("listener stopped accepting after a connection panicked: %v", err)
	}
	defer survivor.Close()
	survivorReader := bufio.NewReader(survivor)
	relpOpen(t, survivor, survivorReader)

	msg := "<34>Jan 15 10:22:15 router01 kernel: still here"
	writeRELPFrame(survivor, 2, "syslog", msg)
	select {
	case m := <-out:
		if string(m.Raw) != msg {
			t.Errorf("expected raw %q, got %q", msg, m.Raw)
		}
		m.Ack <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("ingest stopped after a connection panicked")
	}

	if !logs.Wait(5*time.Second, "recovered panic", "RELP connection", "stack") {
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
