package syslog

import (
	"net"
	"testing"
	"time"

	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/pipeline/ingestion"
)

// TestEventIDIdentity pins the identity invariant for the syslog ingester
// (UDP path).
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	const ingesterID = "test-syslog-ingester"
	out := make(chan ingestion.IngesterMessage, 4)
	recv := New(Config{ID: ingesterID, UDPAddr: "127.0.0.1:0"})
	go func() { _ = recv.Run(t.Context(), out) }()
	waitAddr(t, recv.UDPAddr)

	conn, err := net.Dial("udp", recv.UDPAddr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("<34>Jan 15 10:22:15 host probe: identity")); err != nil {
		t.Fatalf("write: %v", err)
	}

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, ingesterID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for syslog message")
	}
}
