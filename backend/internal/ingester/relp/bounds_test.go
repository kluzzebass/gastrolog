package relp

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/ingester/limits"
	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

// startBoundedRELP starts a RELP ingester and returns its address, output
// channel and log recorder.
func startBoundedRELP(t *testing.T) (string, chan ingestion.IngesterMessage, *logtest.Recorder) {
	t.Helper()
	logger, logs := logtest.New()
	out := make(chan ingestion.IngesterMessage, 4)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0", Logger: logger})
	go func() { _ = ing.Run(t.Context(), out) }()
	return waitForAddr(t, ing).String(), out, logs
}

// TestHugeDatalenIsRejectedWithoutAllocating proves a frame whose DATALEN
// claims more than the frame ceiling is refused before the allocation. The
// claim alone is the attack: the sender never has to transmit a byte of the
// payload it declares.
func TestHugeDatalenIsRejectedWithoutAllocating(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedRELP(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	relpOpen(t, conn, reader)

	// 9999999999 bytes claimed, one byte sent.
	if _, err := fmt.Fprint(conn, "2 syslog 9999999999 X\n"); err != nil {
		t.Fatalf("write: %v", err)
	}

	// Nothing may reach the pipeline, and the connection must drop.
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := reader.ReadByte(); err == nil {
		t.Fatal("connection stayed usable after an oversize frame claim")
	} else if !connectionEnded(err) {
		t.Fatalf("connection was held open after an oversize frame claim: %v", err)
	}
	select {
	case msg := <-out:
		t.Fatalf("an oversize frame reached the pipeline: %q", msg.Raw)
	default:
	}

	if !logs.Wait(5*time.Second, "RELP frame rejected", "max_frame_bytes") {
		t.Errorf("the rejection was silent: %s", logs)
	}
}

// TestUnterminatedHeaderTokenIsRejected proves a sender that never emits a
// delimiter cannot grow the token buffer indefinitely.
func TestUnterminatedHeaderTokenIsRejected(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedRELP(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// A transaction number that never ends.
	if _, err := conn.Write([]byte(strings.Repeat("1", limits.MaxTokenBytes*4))); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection stayed usable after an unterminated token")
	} else if !connectionEnded(err) {
		t.Fatalf("connection was held open after an unterminated token: %v", err)
	}
	select {
	case msg := <-out:
		t.Fatalf("an unterminated frame reached the pipeline: %q", msg.Raw)
	default:
	}

	if !logs.Wait(5*time.Second, "RELP frame rejected") {
		t.Errorf("the rejection was silent: %s", logs)
	}
}

// TestListenerAtCapacityRefusesNewConnections proves the accept loop stops
// admitting connections at its cap and says so, rather than letting one peer
// exhaust the node's file descriptors and goroutines. The cap is lowered
// here so the test does not need to open four thousand sockets.
func TestListenerAtCapacityRefusesNewConnections(t *testing.T) {
	t.Parallel()
	logger, logs := logtest.New()
	out := make(chan ingestion.IngesterMessage, 4)
	ing := New(Config{ID: "test-relp", Addr: "127.0.0.1:0", Logger: logger})
	ing.conns = limits.NewConnLimiter(1)

	go func() { _ = ing.Run(t.Context(), out) }()
	addr := waitForAddr(t, ing).String()

	held, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer held.Close()
	relpOpen(t, held, bufio.NewReader(held))

	refused, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer refused.Close()

	_ = refused.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := refused.Read(make([]byte, 1)); err == nil {
		t.Fatal("a connection past the cap was served")
	} else if !connectionEnded(err) {
		t.Fatalf("a connection past the cap was held open: %v", err)
	}

	if !logs.Wait(5*time.Second, "connection refused", "max_connections") {
		t.Errorf("the refusal was silent: %s", logs)
	}
}

// TestFramesUnderTheCeilingStillIngest proves the bound did not change what
// a conforming sender gets, including a message right at the largest size a
// real syslog relay would send.
func TestFramesUnderTheCeilingStillIngest(t *testing.T) {
	t.Parallel()
	addr, out, _ := startBoundedRELP(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	reader := bufio.NewReader(conn)
	relpOpen(t, conn, reader)

	big := "<34>Jan 15 10:22:15 host app: " + strings.Repeat("x", 64<<10)
	writeRELPFrame(conn, 2, "syslog", big)

	select {
	case msg := <-out:
		if string(msg.Raw) != big {
			t.Errorf("payload altered: got %d bytes, want %d", len(msg.Raw), len(big))
		}
		msg.Ack <- nil
	case <-time.After(5 * time.Second):
		t.Fatal("a conforming frame under the ceiling was not ingested")
	}
}
