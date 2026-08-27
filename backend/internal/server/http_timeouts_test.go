package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"gastrolog/internal/cert"
	"gastrolog/internal/orchestrator"
	sysmem "gastrolog/internal/system/memory"
)

// TestSlowHeaderTimeout_DisconnectsClient proves that a client dribbling
// request headers slower than ReadHeaderTimeout gets its connection closed
// by the server rather than held open indefinitely — the slowloris vector
// this change closes. It uses a short-timeout server instance (via
// newTimedServer, the same constructor production code uses) so the
// assertion is driven by the timeout value itself, not a sleep.
func TestSlowHeaderTimeout_DisconnectsClient(t *testing.T) {
	t.Parallel()

	const headerTimeout = 50 * time.Millisecond
	handlerCalled := make(chan struct{}, 1)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handlerCalled <- struct{}{}
		w.WriteHeader(http.StatusOK)
	})
	srv := newTimedServer(handler, headerTimeout, time.Second, time.Second)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		_ = srv.Serve(ln)
	}()
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Send a request line but never finish the headers — no terminating
	// blank line. A real slowloris client would drip this a byte at a time;
	// sending it once and then going silent is equivalent from the
	// server's perspective, since ReadHeaderTimeout bounds the whole
	// header read regardless of how it's paced.
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\n")); err != nil {
		t.Fatalf("write partial request: %v", err)
	}

	// Block on Read until the server closes the connection. Bound the wait
	// generously above headerTimeout so a hang (bug: no timeout applied)
	// fails the test instead of blocking forever, without using the bound
	// as the pass/fail signal itself — the signal is the read returning.
	if err := conn.SetReadDeadline(time.Now().Add(20 * headerTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 16)
	n, err := conn.Read(buf)
	if err == nil {
		t.Fatalf("expected connection to be closed by ReadHeaderTimeout, got %d bytes with no error", n)
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatalf("server did not close the connection within %v of ReadHeaderTimeout=%v; slowloris is not mitigated", 20*headerTimeout, headerTimeout)
	}
	if err != io.EOF {
		t.Logf("connection closed with %v (expected EOF or reset, both indicate the server dropped it)", err)
	}

	select {
	case <-handlerCalled:
		t.Fatal("handler ran despite headers never completing")
	default:
	}
}

// streamingHandler writes chunkCount chunks, chunkDelay apart, flushing
// after each one. It simulates the shape of the API's real streaming RPCs
// (Follow, WatchSystem, WatchChunks, ...): a single response held open and
// written to slowly over time.
func streamingHandler(t *testing.T, chunkCount int, chunkDelay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("ResponseWriter does not support flushing")
			return
		}
		w.WriteHeader(http.StatusOK)
		for i := 0; i < chunkCount; i++ {
			fmt.Fprintf(w, "chunk-%d\n", i)
			flusher.Flush()
			time.Sleep(chunkDelay)
		}
	}
}

// readChunkedLines reads a chunked HTTP/1.1 response over conn and returns
// the decoded body lines, or an error if the connection closes early.
func readChunkedLines(conn net.Conn, overallDeadline time.Time, count int) ([]string, error) {
	if err := conn.SetReadDeadline(overallDeadline); err != nil {
		return nil, fmt.Errorf("set read deadline: %w", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	defer resp.Body.Close()

	body := bufio.NewReader(resp.Body)
	lines := make([]string, 0, count)
	for i := 0; i < count; i++ {
		line, err := body.ReadString('\n')
		if err != nil {
			return lines, fmt.Errorf("chunk %d: %w", i, err)
		}
		lines = append(lines, line)
	}
	return lines, nil
}

// TestStreamTimeout_HTTP1WriteTimeoutZero_NotKilled pins the regression
// this change could introduce on the HTTPS and Unix socket listeners, both
// of which speak plain HTTP/1.1 (see the comment on the timeout constants
// in server.go): with WriteTimeout left at 0, a single active response
// that keeps writing chunks slower than ReadTimeout/IdleTimeout is not cut
// off. On HTTP/1.1 this is somewhat unsurprising by construction — the
// stdlib clears the request's read deadline once the request is fully
// read, and IdleTimeout only applies between requests — but it is exactly
// the guarantee a future reader might accidentally break by "tightening"
// WriteTimeout, so it is pinned directly, paired with the companion test
// below that proves the same setup DOES fail once WriteTimeout is nonzero
// (i.e. this test is sensitive to the failure it guards against).
func TestStreamTimeout_HTTP1WriteTimeoutZero_NotKilled(t *testing.T) {
	t.Parallel()

	const (
		headerTimeout = 50 * time.Millisecond
		readTimeout   = 200 * time.Millisecond
		idleTimeout   = 200 * time.Millisecond
		chunkDelay    = 80 * time.Millisecond
		chunkCount    = 5 // total streaming time > readTimeout and > idleTimeout
	)

	srv := newTimedServer(streamingHandler(t, chunkCount, chunkDelay), headerTimeout, readTimeout, idleTimeout)
	if srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout must stay unset (0) or streaming responses get killed mid-stream; got %v", srv.WriteTimeout)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	// The full stream takes chunkCount*chunkDelay (400ms), well past
	// readTimeout/idleTimeout (200ms each). The overall deadline is a hang
	// guard, not the pass/fail signal — the signal is "did every chunk
	// arrive intact".
	lines, err := readChunkedLines(conn, time.Now().Add(5*time.Second), chunkCount)
	if err != nil {
		t.Fatalf("stream cut short (readTimeout/idleTimeout killed an active response): %v", err)
	}
	for i, line := range lines {
		if want := fmt.Sprintf("chunk-%d\n", i); line != want {
			t.Fatalf("chunk %d: got %q, want %q", i, line, want)
		}
	}
}

// TestStreamTimeout_HTTP1WriteTimeoutNonzero_Killed is the premise check
// for the test above: the identical streaming handler and timing, but with
// WriteTimeout set below the total stream duration, must fail. This proves
// the "not killed" assertion is actually sensitive to the failure mode it
// guards against — that it isn't vacuously true regardless of what the
// server is configured with. Builds the server directly rather than via
// newTimedServer, since production code never sets WriteTimeout there.
func TestStreamTimeout_HTTP1WriteTimeoutNonzero_Killed(t *testing.T) {
	t.Parallel()

	const (
		writeTimeout = 150 * time.Millisecond
		chunkDelay   = 80 * time.Millisecond
		chunkCount   = 5 // total streaming time (400ms) > writeTimeout (150ms)
	)

	srv := &http.Server{
		Handler:      streamingHandler(t, chunkCount, chunkDelay),
		WriteTimeout: writeTimeout,
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	_, err = readChunkedLines(conn, time.Now().Add(5*time.Second), chunkCount)
	if err == nil {
		t.Fatal("expected the stream to be cut short by a nonzero WriteTimeout, but all chunks arrived intact")
	}
}

// TestIdleTimeout_H2C_ClosesIdleConnection pins the h2c-specific wiring:
// h2c.NewHandler never calls http2.ConfigureServer, so golang.org/x/net/
// http2 reads IdleTimeout from the *http2.Server passed to it, not from
// the *http.Server. A client that completes the HTTP/2 connection preface
// (the same shape a real h2c client or slow/idle proxy would produce) and
// then opens zero streams must be disconnected once IdleTimeout elapses.
func TestIdleTimeout_H2C_ClosesIdleConnection(t *testing.T) {
	t.Parallel()

	const (
		headerTimeout = 50 * time.Millisecond
		readTimeout   = time.Second
		idleTimeout   = 100 * time.Millisecond
	)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := newTimedServer(h2c.NewHandler(handler, &http2.Server{IdleTimeout: idleTimeout}), headerTimeout, readTimeout, idleTimeout)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = srv.Serve(ln) }()
	defer srv.Close()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	// Complete the HTTP/2 "prior knowledge" connection preface (magic
	// string + an initial, empty SETTINGS frame) and then go silent
	// without ever opening a stream.
	if _, err := conn.Write([]byte(http2.ClientPreface)); err != nil {
		t.Fatalf("write client preface: %v", err)
	}
	framer := http2.NewFramer(conn, conn)
	if err := framer.WriteSettings(); err != nil {
		t.Fatalf("write settings frame: %v", err)
	}

	if err := conn.SetReadDeadline(time.Now().Add(20 * idleTimeout)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 64)
	for {
		_, err := conn.Read(buf)
		if err == nil {
			continue // server's own SETTINGS frame; keep waiting for the close
		}
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			t.Fatalf("server did not close the idle h2c connection within %v of IdleTimeout=%v", 20*idleTimeout, idleTimeout)
		}
		break // connection closed, whatever the exact error shape
	}
}

// genSelfSignedCert returns a minimal self-signed cert/key PEM pair for
// exercising the HTTPS listener in tests, valid for a day.
func genSelfSignedCert(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes}))
	return certPEM, keyPEM
}

// requireTimeouts asserts an *http.Server carries exactly the production
// timeout wiring: ReadHeaderTimeout/ReadTimeout/IdleTimeout set to the
// shared constants, WriteTimeout left unset. Catches a listener regressing
// to a bare &http.Server{} (or gaining a WriteTimeout it shouldn't have).
func requireTimeouts(t *testing.T, name string, srv *http.Server) {
	t.Helper()
	if srv == nil {
		t.Fatalf("%s: server was never constructed", name)
	}
	if srv.ReadHeaderTimeout != readHeaderTimeout {
		t.Errorf("%s: ReadHeaderTimeout = %v, want %v", name, srv.ReadHeaderTimeout, readHeaderTimeout)
	}
	if srv.ReadTimeout != readTimeout {
		t.Errorf("%s: ReadTimeout = %v, want %v", name, srv.ReadTimeout, readTimeout)
	}
	if srv.IdleTimeout != idleTimeout {
		t.Errorf("%s: IdleTimeout = %v, want %v", name, srv.IdleTimeout, idleTimeout)
	}
	if srv.WriteTimeout != 0 {
		t.Errorf("%s: WriteTimeout = %v, want 0 (unset)", name, srv.WriteTimeout)
	}
}

// TestServerTimeouts_WiringMatchesProductionConstants exercises the real
// Serve/reconfigureTLS/ListenUnix call sites end to end and asserts each
// resulting *http.Server (main HTTP, HTTPS, Unix socket) carries the
// production timeout constants — catching a listener that regresses to a
// bare &http.Server{} or gains a WriteTimeout it shouldn't have. It checks
// only *http.Server fields; it would not catch the main listener's
// http2.Server-level IdleTimeout regressing (see
// TestIdleTimeout_H2C_ClosesIdleConnection for that).
func TestServerTimeouts_WiringMatchesProductionConstants(t *testing.T) {
	t.Parallel()

	certPEM, keyPEM := genSelfSignedCert(t)
	mgr := cert.New(cert.Config{})
	if err := mgr.LoadFromConfig("server", map[string]cert.CertSource{
		"server": {CertPEM: certPEM, KeyPEM: keyPEM},
	}); err != nil {
		t.Fatalf("load cert: %v", err)
	}

	store := sysmem.NewStore()
	ctx := context.Background()
	ss, err := store.LoadServerSettings(ctx)
	if err != nil {
		t.Fatalf("load server settings: %v", err)
	}
	ss.TLS.TLSEnabled = true
	ss.TLS.DefaultCert = "server"
	ss.TLS.HTTPSPort = "0"
	if err := store.SaveServerSettings(ctx, ss); err != nil {
		t.Fatalf("save server settings: %v", err)
	}

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}

	srv := New(orch, store, orchestrator.Factories{}, nil, Config{CertManager: mgr})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Stop(stopCtx)
	}()

	// Serve's setup (including reconfigureTLS, since TLS was already
	// enabled in the store before Serve was called) runs synchronously
	// before it blocks in the accept loop; poll for that to land instead
	// of sleeping a fixed amount.
	deadline := time.Now().Add(5 * time.Second)
	var httpSrv, httpsSrv *http.Server
	for time.Now().Before(deadline) {
		srv.mu.Lock()
		httpSrv = srv.server
		httpsSrv = srv.httpsServer
		srv.mu.Unlock()
		if httpSrv != nil && httpsSrv != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	requireTimeouts(t, "http", httpSrv)
	requireTimeouts(t, "https", httpsSrv)

	// A short-lived dir outside t.TempDir(): the unix socket path length
	// limit (~104 bytes on macOS) doesn't leave room for this test's name.
	sockDir, err := os.MkdirTemp("", "glsock")
	if err != nil {
		t.Fatalf("mkdir temp: %v", err)
	}
	defer os.RemoveAll(sockDir)
	sockPath := filepath.Join(sockDir, "gastrolog.sock")
	if err := srv.ListenUnix(sockPath); err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	srv.mu.Lock()
	unixSrv := srv.unixServer
	srv.mu.Unlock()
	requireTimeouts(t, "unix", unixSrv)
}
