package relp

import (
	"bufio"
	"net"
	"runtime"
	"testing"
	"time"

	"gastrolog/internal/ingester/identitytest"
	"gastrolog/internal/orchestrator"
)

// TestEventIDIdentity pins gastrolog-44b9r for the RELP ingester.
func TestEventIDIdentity(t *testing.T) {
	t.Parallel()
	const ingesterID = "test-relp-ingester"
	out := make(chan orchestrator.IngestMessage, 4)
	ing := New(Config{ID: ingesterID, Addr: "127.0.0.1:0"})

	go func() { _ = ing.Run(t.Context(), out) }()

	deadline := time.Now().Add(2 * time.Second)
	var addr net.Addr
	for {
		addr = ing.Addr()
		if addr != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("listener did not bind")
		}
		runtime.Gosched()
	}

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writeRELPFrame(conn, 1, "open", "relp_version=0\nrelp_software=test\ncommands=syslog")
	if _, _, _, err := readRELPResponse(reader); err != nil {
		t.Fatalf("open response: %v", err)
	}
	writeRELPFrame(conn, 2, "syslog", "<34>Jan 15 10:22:15 host probe: identity")

	select {
	case msg := <-out:
		identitytest.AssertHasIdentity(t, msg, ingesterID)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for RELP message")
	}
}
