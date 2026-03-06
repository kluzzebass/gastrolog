package server

import (
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
)

func TestCompressMiddleware_Brotli(t *testing.T) {
	body := "hello world from the server"
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	})

	h := compressMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip, br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding = %q, want %q", got, "br")
	}

	plain, err := io.ReadAll(brotli.NewReader(rec.Body))
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != body {
		t.Fatalf("body = %q, want %q", plain, body)
	}
}

func TestCompressMiddleware_Gzip(t *testing.T) {
	body := "hello world from the server"
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	})

	h := compressMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want %q", got, "gzip")
	}

	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != body {
		t.Fatalf("body = %q, want %q", plain, body)
	}
}

func TestCompressMiddleware_PrefersBrotli(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("data"))
	})

	h := compressMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding = %q, want %q (brotli preferred over gzip)", got, "br")
	}
}

func TestCompressMiddleware_NoAcceptEncoding(t *testing.T) {
	body := "uncompressed response"
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(body))
	})

	h := compressMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := rec.Body.String(); got != body {
		t.Fatalf("body = %q, want %q", got, body)
	}
}

func TestCompressMiddleware_SkipsPreCompressed(t *testing.T) {
	// Simulates the frontend handler which sets Content-Encoding itself.
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Encoding", "br")
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte("pre-compressed-brotli-data"))
	})

	h := compressMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip, br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Should pass through the existing Content-Encoding, not double-compress.
	if got := rec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding = %q, want %q", got, "br")
	}
	if got := rec.Body.String(); got != "pre-compressed-brotli-data" {
		t.Fatalf("body = %q, want %q", got, "pre-compressed-brotli-data")
	}
}

func TestCompressMiddleware_NoContent(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	h := compressMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty for 204", got)
	}
}

func TestCompressMiddleware_Flush(t *testing.T) {
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("chunk1"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		w.Write([]byte("chunk2"))
	})

	h := compressMiddleware(inner)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	gz, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := io.ReadAll(gz)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(plain), "chunk1") || !strings.Contains(string(plain), "chunk2") {
		t.Fatalf("body = %q, want both chunks", plain)
	}
}

// ─── Benchmarks ──────────────────────────────────────────────────────────────

// makeJSONPayload generates a realistic JSON log response payload of approximately n bytes.
func makeJSONPayload(n int) []byte {
	line := []byte(`{"ts":"2024-01-15T10:22:15.123Z","level":"ERROR","msg":"authentication failed","host":"app-01"}` + "\n")
	buf := make([]byte, 0, n)
	for len(buf) < n {
		buf = append(buf, line...)
	}
	return buf[:n]
}

// BenchmarkCompressBrotli measures brotli compression throughput via the middleware.
func BenchmarkCompressBrotli(b *testing.B) {
	payload := makeJSONPayload(32 * 1024) // 32 KB
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	})
	h := compressMiddleware(inner)
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Accept-Encoding", "br")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}

// BenchmarkCompressGzip measures gzip compression throughput via the middleware.
func BenchmarkCompressGzip(b *testing.B) {
	payload := makeJSONPayload(32 * 1024) // 32 KB
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(payload)
	})
	h := compressMiddleware(inner)
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
	}
}

// BenchmarkBrotliWriterPool measures the channel-based brotli writer pool get/put cycle.
func BenchmarkBrotliWriterPool(b *testing.B) {
	for b.Loop() {
		w := getBrotliWriter(io.Discard)
		w.Write([]byte("benchmark data for pool recycling test"))
		w.Close()
		putBrotliWriter(w)
	}
}

// BenchmarkAcceptsEncoding measures Accept-Encoding header parsing.
func BenchmarkAcceptsEncoding(b *testing.B) {
	header := "gzip, deflate, br, zstd"
	for b.Loop() {
		_ = acceptsEncoding(header, "br")
		_ = acceptsEncoding(header, "gzip")
		_ = acceptsEncoding(header, "identity")
	}
}
