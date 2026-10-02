package syslog

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/ingester/ingesttls/tlstest"
	"gastrolog/internal/pipeline/ingestion"
)

// The TCP listener serves TLS when configured (RFC 5425), with the
// certificate resolved from the cert store by name — and a plaintext client
// against it gets nothing: its bytes die in the failed handshake instead of
// being parsed as records.
func TestSyslogTCPServesTLS(t *testing.T) {
	ca := tlstest.NewCA(t, "test-ca")
	mgr := tlstest.Manager(t, map[string]tlstest.Pair{
		"srv": ca.Leaf(t, "syslog-srv", []string{"127.0.0.1"}),
	})
	tlsCfg, err := ingesttls.Server("syslog", map[string]string{"tls": "true", "tls_cert": "srv"}, mgr)
	if err != nil {
		t.Fatal(err)
	}

	out := make(chan ingestion.IngesterMessage, 10)
	recv := New(Config{TCPAddr: "127.0.0.1:0", TLSConfig: tlsCfg})
	go recv.Run(t.Context(), out)
	waitAddr(t, recv.TCPAddr)

	conn, err := tls.Dial("tcp", recv.TCPAddr().String(), &tls.Config{RootCAs: ca.Pool(t), MinVersion: tls.VersionTLS12})
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	if cn := conn.ConnectionState().PeerCertificates[0].Subject.CommonName; cn != "syslog-srv" {
		t.Fatalf("listener served certificate %q, want the named one", cn)
	}
	msg := "<34>Jan 15 10:22:15 host1 app1: over tls"
	if _, err := conn.Write([]byte(msg + "\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-out:
		if string(m.Raw) != msg {
			t.Fatalf("got %q", m.Raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("record sent over TLS never arrived")
	}
	_ = conn.Close()

	// Plaintext against the TLS listener: the handshake fails server-side
	// and nothing reaches the parser.
	plain, err := net.Dial("tcp", recv.TCPAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	_, _ = plain.Write([]byte(msg + "\n"))
	_ = plain.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 1)
	if _, err := plain.Read(buf); err == nil {
		t.Fatal("plaintext connection was served by a TLS listener")
	}
	select {
	case m := <-out:
		t.Fatalf("plaintext bytes reached the parser through a TLS listener: %q", m.Raw)
	case <-time.After(200 * time.Millisecond):
	}
}
