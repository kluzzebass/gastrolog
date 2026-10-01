package http

import (
	"crypto/tls"
	"io"
	"net"
	gohttp "net/http"
	"strings"
	"testing"
	"time"

	"gastrolog/internal/ingester/ingesttls"
	"gastrolog/internal/ingester/ingesttls/tlstest"
	"gastrolog/internal/pipeline/ingestion"
)

// The push listener serves HTTPS when configured, end to end: a TLS client
// delivers a record through the Loki push API, and a plaintext client gets
// no HTTP at all.
func TestHTTPServesTLS(t *testing.T) {
	ca := tlstest.NewCA(t, "test-ca")
	mgr := tlstest.Manager(t, map[string]tlstest.Pair{
		"srv": ca.Leaf(t, "http-srv", []string{"127.0.0.1"}),
	})
	tlsCfg, err := ingesttls.Server("http", map[string]string{"tls": "true", "tls_cert": "srv"}, mgr)
	if err != nil {
		t.Fatal(err)
	}

	out := make(chan ingestion.IngesterMessage, 10)
	r := New(Config{ID: "t", Addr: "127.0.0.1:0", TLSConfig: tlsCfg})
	go r.Run(t.Context(), out)
	deadline := time.Now().Add(3 * time.Second)
	for r.Addr() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if r.Addr() == nil {
		t.Fatal("listener never bound")
	}
	addr := r.Addr().String()

	client := &gohttp.Client{Transport: &gohttp.Transport{
		TLSClientConfig: &tls.Config{RootCAs: ca.Pool(t), MinVersion: tls.VersionTLS12},
	}}
	body := `{"streams":[{"stream":{"app":"t"},"values":[["1700000000000000000","over tls"]]}]}`
	resp, err := client.Post("https://"+addr+"/loki/api/v1/push", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("HTTPS push: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("push status %d: %s", resp.StatusCode, b)
	}
	select {
	case m := <-out:
		if string(m.Raw) != "over tls" {
			t.Fatalf("got %q", m.Raw)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("record pushed over TLS never arrived")
	}

	plain, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	_, _ = plain.Write([]byte("POST /loki/api/v1/push HTTP/1.1\r\nHost: x\r\n\r\n"))
	_ = plain.SetReadDeadline(time.Now().Add(2 * time.Second))
	// Go's http.Server answers a plaintext request on a TLS port with a
	// courtesy 400. The property that matters: the push is refused, the
	// record never lands.
	buf := make([]byte, 64)
	n, _ := plain.Read(buf)
	if strings.Contains(string(buf[:n]), "200") {
		t.Fatalf("plaintext push succeeded against a TLS listener: %q", buf[:n])
	}
	select {
	case m := <-out:
		t.Fatalf("plaintext push reached the parser through a TLS listener: %q", m.Raw)
	case <-time.After(200 * time.Millisecond):
	}
}
