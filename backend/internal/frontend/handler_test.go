package frontend

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/andybalholm/brotli"
)

// brCompress returns brotli-compressed data.
func brCompress(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := brotli.NewWriterLevel(&buf, brotli.BestCompression)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func testFS(t *testing.T) fstest.MapFS {
	t.Helper()
	return fstest.MapFS{
		"index.html.br":                 {Data: brCompress(t, []byte("<html>hello</html>"))},
		"assets/app-abc123.js.br":       {Data: brCompress(t, []byte("console.log('app')"))},
		"assets/style-def456.css.br":    {Data: brCompress(t, []byte("body{color:red}"))},
		"favicon.svg.br":                {Data: brCompress(t, []byte("<svg/>"))},
	}
}

func TestBrotliServing(t *testing.T) {
	h := newStaticHandler(testFS(t))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding = %q, want %q", got, "br")
	}
	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want %q", got, "text/html; charset=utf-8")
	}

	// Verify the body is valid brotli that decompresses to our original.
	plain, err := io.ReadAll(brotli.NewReader(rec.Body))
	if err != nil {
		t.Fatal(err)
	}
	if string(plain) != "<html>hello</html>" {
		t.Fatalf("body = %q, want %q", plain, "<html>hello</html>")
	}
}

func TestGzipFallback(t *testing.T) {
	h := newStaticHandler(testFS(t))

	req := httptest.NewRequest(http.MethodGet, "/assets/app-abc123.js", nil)
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
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
	if string(plain) != "console.log('app')" {
		t.Fatalf("body = %q, want %q", plain, "console.log('app')")
	}
}

func TestPlainFallback(t *testing.T) {
	h := newStaticHandler(testFS(t))

	req := httptest.NewRequest(http.MethodGet, "/assets/style-def456.css", nil)
	// No Accept-Encoding header.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	if got := rec.Body.String(); got != "body{color:red}" {
		t.Fatalf("body = %q, want %q", got, "body{color:red}")
	}
}

func TestCacheHeaders(t *testing.T) {
	h := newStaticHandler(testFS(t))

	tests := []struct {
		path  string
		cache string
	}{
		{"/", "no-cache"},
		{"/assets/app-abc123.js", "max-age=31536000, immutable"},
		{"/favicon.svg", ""},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, tt.path, nil)
		req.Header.Set("Accept-Encoding", "br")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if got := rec.Header().Get("Cache-Control"); got != tt.cache {
			t.Errorf("%s: Cache-Control = %q, want %q", tt.path, got, tt.cache)
		}
	}
}

func TestSPAFallback(t *testing.T) {
	h := newStaticHandler(testFS(t))

	// Request a path that doesn't exist — should serve index.html.
	req := httptest.NewRequest(http.MethodGet, "/stores", nil)
	req.Header.Set("Accept-Encoding", "br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("Content-Type = %q, want %q", got, "text/html; charset=utf-8")
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-cache" {
		t.Fatalf("Cache-Control = %q, want %q", got, "no-cache")
	}
}

func TestMIMETypes(t *testing.T) {
	h := newStaticHandler(testFS(t))

	tests := []struct {
		path string
		ct   string
	}{
		{"/assets/app-abc123.js", "text/javascript; charset=utf-8"},
		{"/assets/style-def456.css", "text/css; charset=utf-8"},
		{"/favicon.svg", "image/svg+xml"},
	}

	for _, tt := range tests {
		req := httptest.NewRequest(http.MethodGet, tt.path, nil)
		req.Header.Set("Accept-Encoding", "br")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if got := rec.Header().Get("Content-Type"); got != tt.ct {
			t.Errorf("%s: Content-Type = %q, want %q", tt.path, got, tt.ct)
		}
	}
}

func TestMethodNotAllowed(t *testing.T) {
	h := newStaticHandler(testFS(t))

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusMethodNotAllowed)
	}
}

func TestHEADRequest(t *testing.T) {
	h := newStaticHandler(testFS(t))

	req := httptest.NewRequest(http.MethodHead, "/", nil)
	req.Header.Set("Accept-Encoding", "br")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD response has body of %d bytes", rec.Body.Len())
	}
	if got := rec.Header().Get("Content-Length"); got == "" || got == "0" {
		t.Fatalf("Content-Length = %q, want non-zero", got)
	}
}

func TestDevModeNilHandler(t *testing.T) {
	// When dist only has .gitignore, newStaticHandler builds an empty file set.
	emptyFS := fstest.MapFS{
		".gitignore": {Data: []byte("*\n!.gitignore\n")},
	}
	h := newStaticHandler(emptyFS)
	if len(h.files) != 0 {
		t.Fatalf("expected 0 files, got %d", len(h.files))
	}
}
