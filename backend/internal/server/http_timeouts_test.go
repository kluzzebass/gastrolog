package server

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestNewTimedServer_SlowHeaderClientDisconnected proves that a client
// dribbling request headers slower than ReadHeaderTimeout gets its
// connection closed by the server rather than held open indefinitely —
// the slowloris vector this change closes. It uses a short-timeout server
// instance (via newTimedServer, the same constructor production code uses)
// so the assertion is driven by the timeout value itself, not a sleep.
func TestNewTimedServer_SlowHeaderClientDisconnected(t *testing.T) {
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

// TestNewTimedServer_LongLivedStreamNotKilled pins the regression this
// change could introduce: ReadTimeout and IdleTimeout, once nonzero, must
// not cut off a single active response that keeps writing chunks slower
// than either value. This is the shape of the API's real streaming RPCs
// (Follow, WatchSystem, WatchChunks, ...). WriteTimeout is asserted to be
// unset — the actual mechanism that keeps streams alive in production,
// documented where the constants are defined.
func TestNewTimedServer_LongLivedStreamNotKilled(t *testing.T) {
	t.Parallel()

	const (
		headerTimeout = 50 * time.Millisecond
		readTimeout   = 200 * time.Millisecond
		idleTimeout   = 200 * time.Millisecond
		chunkDelay    = 80 * time.Millisecond
		chunkCount    = 5 // total streaming time > readTimeout and > idleTimeout
	)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
	})
	srv := newTimedServer(handler, headerTimeout, readTimeout, idleTimeout)
	if srv.WriteTimeout != 0 {
		t.Fatalf("WriteTimeout must stay unset (0) or HTTP/2 streaming RPCs get killed mid-stream; got %v", srv.WriteTimeout)
	}

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

	if _, err := conn.Write([]byte("GET / HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n")); err != nil {
		t.Fatalf("write request: %v", err)
	}

	// The full stream takes chunkCount*chunkDelay (400ms), well past
	// readTimeout/idleTimeout (200ms each). A generous overall deadline
	// bounds the test against a genuine hang without using it as the
	// decision gate: the decision is "did every chunk arrive", not "did
	// time pass".
	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	defer resp.Body.Close()

	body := bufio.NewReader(resp.Body)
	for i := 0; i < chunkCount; i++ {
		line, err := body.ReadString('\n')
		if err != nil {
			t.Fatalf("chunk %d: connection closed early (readTimeout/idleTimeout killed an active stream): %v", i, err)
		}
		want := fmt.Sprintf("chunk-%d\n", i)
		if line != want {
			t.Fatalf("chunk %d: got %q, want %q", i, line, want)
		}
	}
}
