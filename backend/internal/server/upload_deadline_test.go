package server

import (
	"bufio"
	"bytes"
	"mime/multipart"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	sysmem "gastrolog/internal/system/memory"
)

// TestUploadHandler_SurvivesReadTimeoutThroughCompressMiddleware pins the
// fix for the managed-file upload handler: it must clear its own read
// deadline even though the real request goes through compressMiddleware's
// compressWriter wrapper first. compressWriter embeds http.ResponseWriter
// as an interface (no method promotion) and needs its own Unwrap method
// for http.ResponseController to reach the underlying writer at all — every
// real client hits this path, since both a browser fetch (Accept-Encoding
// is a forbidden header, always sent) and Go's http.DefaultClient
// (transport auto-adds Accept-Encoding: gzip) trigger compressWriter.
//
// The test drives the endpoint through the exact production middleware
// chain (tracking → CORS → securityHeaders → rateLimit → compress → mux),
// with Accept-Encoding: gzip set, over a real listener whose ReadTimeout
// is deliberately short. The multipart body is trickled in slowly enough
// to blow through that ReadTimeout many times over; the upload must still
// succeed, because the handler clears its own deadline before reading the
// body — MaxBytesReader is the only bound left on this request, as the
// comment in upload.go states.
func TestUploadHandler_SurvivesReadTimeoutThroughCompressMiddleware(t *testing.T) {
	t.Parallel()

	const (
		headerTimeout = 50 * time.Millisecond
		readTimeout   = 150 * time.Millisecond // deliberately short; production uses 30s
		idleTimeout   = time.Second
		chunkDelay    = 40 * time.Millisecond
		chunkCount    = 6 // total trickle time (240ms) > readTimeout (150ms)
	)

	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatalf("new orchestrator: %v", err)
	}
	srv := New(orch, sysmem.NewStore(), orchestrator.Factories{}, nil, Config{
		NoAuth:       true,
		HomeDir:      t.TempDir(),
		ConfigSignal: notify.NewSignal(),
	})

	mux := srv.buildMux()
	handler := srv.trackingMiddleware(srv.corsMiddleware(securityHeadersMiddleware(rateLimitMiddleware(srv.rl)(compressMiddleware(srv.logger, mux)))))
	httpSrv := newTimedServer(handler, headerTimeout, readTimeout, idleTimeout)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() { _ = httpSrv.Serve(ln) }()
	defer httpSrv.Close()

	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("file", "upload.txt")
	if err != nil {
		t.Fatalf("create form file: %v", err)
	}
	if _, err := part.Write([]byte("content trickled in slower than the server's ReadTimeout")); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart writer: %v", err)
	}
	bodyBytes := body.Bytes()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()

	reqHead := "POST /api/v1/managed-files/upload HTTP/1.1\r\n" +
		"Host: example.com\r\n" +
		"Content-Type: " + mw.FormDataContentType() + "\r\n" +
		"Content-Length: " + strconv.Itoa(len(bodyBytes)) + "\r\n" +
		"Accept-Encoding: gzip\r\n" +
		"Connection: close\r\n\r\n"
	if _, err := conn.Write([]byte(reqHead)); err != nil {
		t.Fatalf("write request head: %v", err)
	}

	// Trickle the body across chunkCount writes, chunkDelay apart — well
	// past readTimeout in total — simulating a legitimately slow upload
	// rather than a slowloris attack against some other endpoint.
	chunks := splitInto(bodyBytes, chunkCount)
	for _, c := range chunks {
		if _, err := conn.Write(c); err != nil {
			t.Fatalf("write body chunk: %v", err)
		}
		time.Sleep(chunkDelay)
	}

	if err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("read response: %v (readTimeout likely killed the slow upload)", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("upload status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	if enc := resp.Header.Get("Content-Encoding"); enc != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip (request never reached compressWriter)", enc)
	}
}

// splitInto divides b into n roughly-equal, non-empty chunks in order.
func splitInto(b []byte, n int) [][]byte {
	if n <= 0 || n > len(b) {
		n = len(b)
	}
	chunks := make([][]byte, 0, n)
	size := (len(b) + n - 1) / n
	for i := 0; i < len(b); i += size {
		end := min(i+size, len(b))
		chunks = append(chunks, b[i:end])
	}
	return chunks
}
