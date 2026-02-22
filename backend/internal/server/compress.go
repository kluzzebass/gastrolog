package server

import (
	"compress/gzip"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
)

const brotliDynamicQuality = 4 // fast enough for dynamic responses, ~15-20% smaller than gzip

var gzipWriterPool = sync.Pool{
	New: func() any {
		w, _ := gzip.NewWriterLevel(io.Discard, gzip.DefaultCompression)
		return w
	},
}

// brotliPool is a channel-based bounded pool that prevents GC eviction.
// sync.Pool evicts all entries every GC cycle, causing ~582 KB of ring buffer
// reallocation per writer. A channel-based pool holds strong references,
// keeping writers alive across GC cycles and eliminating the churn.
var brotliPool = func() chan *brotli.Writer {
	size := max(runtime.GOMAXPROCS(0), 4)
	ch := make(chan *brotli.Writer, size)
	return ch
}()

func getBrotliWriter(dst io.Writer) *brotli.Writer {
	select {
	case w := <-brotliPool:
		w.Reset(dst)
		return w
	default:
		return brotli.NewWriterLevel(dst, brotliDynamicQuality)
	}
}

func putBrotliWriter(w *brotli.Writer) {
	w.Reset(io.Discard) // release reference to the response writer
	select {
	case brotliPool <- w:
	default:
		// Pool full — discard the writer.
	}
}

// compressMiddleware applies brotli or gzip compression to responses when the
// client supports it. Prefers brotli over gzip. Skips responses that already
// have Content-Encoding set (e.g. pre-compressed static assets from the
// frontend handler).
func compressMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ae := r.Header.Get("Accept-Encoding")

		var encoding string
		switch {
		case acceptsEncoding(ae, "br"):
			encoding = "br"
		case acceptsEncoding(ae, "gzip"):
			encoding = "gzip"
		default:
			next.ServeHTTP(w, r)
			return
		}

		// Strip Accept-Encoding so downstream handlers (e.g. connect-go) don't
		// compress independently. This middleware owns response compression.
		r = r.Clone(r.Context())
		r.Header.Del("Accept-Encoding")

		cw := &compressWriter{
			ResponseWriter: w,
			encoding:       encoding,
		}
		defer cw.Close()

		next.ServeHTTP(cw, r)
	})
}

// acceptsEncoding checks whether the Accept-Encoding header includes the given encoding.
func acceptsEncoding(header, encoding string) bool {
	for _, part := range strings.Split(header, ",") {
		if enc, _, _ := strings.Cut(strings.TrimSpace(part), ";"); strings.TrimSpace(enc) == encoding {
			return true
		}
	}
	return false
}

// compressWriter wraps http.ResponseWriter to lazily apply compression.
// It defers the decision to compress until WriteHeader/Write, so that responses
// with an existing Content-Encoding (from the frontend handler) pass through untouched.
type compressWriter struct {
	http.ResponseWriter
	encoding    string // "br" or "gzip"
	writer      io.WriteCloser
	started     bool
	compressing bool
}

func (cw *compressWriter) WriteHeader(code int) {
	if cw.started {
		return
	}
	cw.started = true

	// Don't compress if the handler already set Content-Encoding (e.g. frontend handler).
	if cw.Header().Get("Content-Encoding") != "" {
		cw.ResponseWriter.WriteHeader(code)
		return
	}

	// Don't compress empty or tiny responses, or non-compressible status codes.
	if code == http.StatusNoContent || code == http.StatusNotModified {
		cw.ResponseWriter.WriteHeader(code)
		return
	}

	cw.compressing = true
	cw.Header().Set("Content-Encoding", cw.encoding)
	cw.Header().Del("Content-Length")
	cw.Header().Add("Vary", "Accept-Encoding")

	switch cw.encoding {
	case "br":
		cw.writer = getBrotliWriter(cw.ResponseWriter)
	case "gzip":
		gz := gzipWriterPool.Get().(*gzip.Writer)
		gz.Reset(cw.ResponseWriter)
		cw.writer = gz
	}

	cw.ResponseWriter.WriteHeader(code)
}

func (cw *compressWriter) Write(b []byte) (int, error) {
	if !cw.started {
		cw.WriteHeader(http.StatusOK)
	}
	if cw.compressing {
		return cw.writer.Write(b)
	}
	return cw.ResponseWriter.Write(b)
}

func (cw *compressWriter) Flush() {
	if cw.compressing {
		// brotli.Writer implements Flush(); gzip.Writer implements Flush().
		if f, ok := cw.writer.(interface{ Flush() error }); ok {
			f.Flush()
		}
	}
	if f, ok := cw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (cw *compressWriter) Close() {
	if !cw.compressing || cw.writer == nil {
		return
	}
	cw.writer.Close()

	// Return to pool.
	switch cw.encoding {
	case "br":
		putBrotliWriter(cw.writer.(*brotli.Writer))
	case "gzip":
		gzipWriterPool.Put(cw.writer)
	}
	cw.writer = nil
}
