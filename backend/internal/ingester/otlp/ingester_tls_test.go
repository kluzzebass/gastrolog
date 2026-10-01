package otlp

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/ingester/ingesttls/tlstest"
	"gastrolog/internal/pipeline/ingestion"
)

// One TLS config covers both OTLP listeners: the HTTP port and the gRPC
// port each complete a handshake presenting the named certificate.
func TestOTLPServesTLSOnBothPorts(t *testing.T) {
	ca := tlstest.NewCA(t, "test-ca")
	mgr := tlstest.Manager(t, map[string]tlstest.Pair{
		"srv": ca.Leaf(t, "otlp-srv", []string{"127.0.0.1"}),
	})
	tlsCfg, err := ingesttls.Server("otlp", map[string]string{"tls": "true", "tls_cert": "srv"}, mgr)
	if err != nil {
		t.Fatal(err)
	}

	reserve := func() string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer ln.Close()
		return ln.Addr().String()
	}
	httpAddr, grpcAddr := reserve(), reserve()

	out := make(chan ingestion.IngesterMessage, 10)
	ing := New(Config{ID: "t", HTTPAddr: httpAddr, GRPCAddr: grpcAddr, TLSConfig: tlsCfg})
	go ing.Run(t.Context(), out)

	for _, addr := range []string{httpAddr, grpcAddr} {
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
			t.Fatalf("TLS dial %s: %v", addr, err)
		}
		if cn := conn.ConnectionState().PeerCertificates[0].Subject.CommonName; cn != "otlp-srv" {
			t.Fatalf("%s served certificate %q, want the named one", addr, cn)
		}
		_ = conn.Close()
	}
}
