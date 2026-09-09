package syslog

import (
	"net"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/ingester/limits"
	"gastrolog/internal/logging/logtest"
	"gastrolog/internal/pipeline/ingestion"
)

func startBoundedSyslog(t *testing.T) (string, chan ingestion.IngesterMessage, *logtest.Recorder) {
	t.Helper()
	logger, logs := logtest.New()
	out := make(chan ingestion.IngesterMessage, 4)
	recv := New(Config{TCPAddr: "127.0.0.1:0", Logger: logger})
	go func() { _ = recv.Run(t.Context(), out) }()
	waitAddr(t, recv.TCPAddr)
	return recv.TCPAddr().String(), out, logs
}

// TestOverlongLineIsRejected proves newline framing refuses a line that
// grows past the frame ceiling. Without the bound a sender that withholds
// the newline grows one buffer for as long as it keeps writing, limited only
// by the link speed.
func TestOverlongLineIsRejected(t *testing.T) {
	if testing.Short() {
		t.Skip("writes a megabyte over a socket")
	}
	t.Parallel()
	addr, out, logs := startBoundedSyslog(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// No newline anywhere in it.
	flood := strings.Repeat("x", limits.MaxFrameBytes+4096)
	_, _ = conn.Write([]byte(flood))

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection stayed usable after an over-long line")
	} else if !connectionEnded(err) {
		t.Fatalf("connection was held open after an over-long line: %v", err)
	}
	select {
	case msg := <-out:
		t.Fatalf("an over-long line reached the pipeline: %d bytes", len(msg.Raw))
	default:
	}

	if !logs.Wait(5*time.Second, "syslog message rejected", "max_frame_bytes") {
		t.Errorf("the rejection was silent: %s", logs)
	}
}

// TestOverlongOctetCountIsRejected proves the octet-counted path refuses a
// declared length past the ceiling — the claim alone, with no payload sent.
func TestOverlongOctetCountIsRejected(t *testing.T) {
	t.Parallel()
	addr, out, logs := startBoundedSyslog(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("999999999 X")); err != nil {
		t.Fatalf("write: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("connection stayed usable after an oversize octet count")
	} else if !connectionEnded(err) {
		t.Fatalf("connection was held open after an oversize octet count: %v", err)
	}
	select {
	case msg := <-out:
		t.Fatalf("an oversize frame reached the pipeline: %d bytes", len(msg.Raw))
	default:
	}

	if !logs.Wait(5*time.Second, "syslog message rejected") {
		t.Errorf("the rejection was silent: %s", logs)
	}
}

// TestLongLinesUnderTheCeilingStillIngest proves the bound did not change
// what a conforming sender gets, including a line far longer than one
// bufio buffer.
func TestLongLinesUnderTheCeilingStillIngest(t *testing.T) {
	t.Parallel()
	addr, out, _ := startBoundedSyslog(t)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	msg := "<34>Jan 15 10:22:15 host app: " + strings.Repeat("x", 64<<10)
	if _, err := conn.Write([]byte(msg + "\n")); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case got := <-out:
		if string(got.Raw) != msg {
			t.Errorf("payload altered: got %d bytes, want %d", len(got.Raw), len(msg))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a conforming line under the ceiling was not ingested")
	}
}
