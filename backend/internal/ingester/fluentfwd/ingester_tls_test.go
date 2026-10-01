package fluentfwd

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/ingester/ingesttls/tlstest"
	"gastrolog/internal/pipeline/ingestion"
)

// The listener serves TLS when configured, with the certificate resolved
// from the cert store by name; a plaintext client's bytes die in the failed
// handshake instead of reaching the Forward decoder.
func TestFluentForwardServesTLS(t *testing.T) {
	ca := tlstest.NewCA(t, "test-ca")
	mgr := tlstest.Manager(t, map[string]tlstest.Pair{
		"srv": ca.Leaf(t, "fluentfwd-srv", []string{"127.0.0.1"}),
	})
	tlsCfg, err := ingesttls.Server("fluentfwd", map[string]string{"tls": "true", "tls_cert": "srv"}, mgr)
	if err != nil {
		t.Fatal(err)
	}

	// Reserve a port the ingester will rebind.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	out := make(chan ingestion.IngesterMessage, 10)
	ing := New(Config{ID: "t", Addr: addr, TLSConfig: tlsCfg})
	go ing.Run(t.Context(), out)

	var conn *tls.Conn
	deadline := time.Now().Add(3 * time.Second)
	for {
		conn, err = tls.Dial("tcp", addr, &tls.Config{RootCAs: ca.Pool(t), MinVersion: tls.VersionTLS12})
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("TLS dial: %v", err)
	}
	if cn := conn.ConnectionState().PeerCertificates[0].Subject.CommonName; cn != "fluentfwd-srv" {
		t.Fatalf("listener served certificate %q, want the named one", cn)
	}
	_ = conn.Close()

	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	_, _ = plain.Write([]byte("not a tls handshake"))
	_ = plain.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := plain.Read(make([]byte, 1)); err == nil {
		t.Fatal("plaintext connection was served by a TLS listener")
	}
}
