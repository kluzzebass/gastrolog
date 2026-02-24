package frontend

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/andybalholm/brotli"
)

// staticHandler serves brotli-compressed frontend assets from an embedded fs.FS.
// All files in the FS are expected to have a .br extension. The handler strips .br
// to determine the original filename for MIME type detection and path matching.
type staticHandler struct {
	fs    fs.FS
	files map[string]bool // set of original paths (without .br) that exist
}

func newStaticHandler(fsys fs.FS) *staticHandler {
	h := &staticHandler{
		fs:    fsys,
		files: make(map[string]bool),
	}

	_ = fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if after, found := strings.CutSuffix(p, ".br"); found {
			h.files[after] = true
		}
		return nil
	})

	return h
}

func (h *staticHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Clean and normalize the path.
	p := strings.TrimPrefix(path.Clean(r.URL.Path), "/")
	if p == "" {
		p = "index.html"
	}

	// SPA fallback: if path doesn't match a real file, serve index.html.
	if !h.files[p] {
		p = "index.html"
	}

	brPath := p + ".br"

	f, err := h.fs.Open(brPath)
	if err != nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	defer func() { _ = f.Close() }()

	data, err := io.ReadAll(f)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// MIME type from the original filename (without .br).
	ct := mime.TypeByExtension(filepath.Ext(p))
	if ct == "" {
		ct = "application/octet-stream"
	}
	w.Header().Set("Content-Type", ct)

	// Cache headers: hashed assets are immutable, index.html is no-cache.
	if strings.HasPrefix(p, "assets/") {
		w.Header().Set("Cache-Control", "max-age=31536000, immutable")
	} else if p == "index.html" {
		w.Header().Set("Cache-Control", "no-cache")
	}

	// Content negotiation.
	ae := r.Header.Get("Accept-Encoding")
	switch {
	case acceptsEncoding(ae, "br"):
		w.Header().Set("Content-Encoding", "br")
		w.Header().Set("Content-Length", intToStr(len(data)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(data)
		}

	case acceptsEncoding(ae, "gzip"):
		plain, err := decompressBrotli(data)
		if err != nil {
			http.Error(w, "decompression error", http.StatusInternalServerError)
			return
		}
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		_, _ = gz.Write(plain)
		_ = gz.Close()
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Length", intToStr(buf.Len()))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(buf.Bytes())
		}

	default:
		plain, err := decompressBrotli(data)
		if err != nil {
			http.Error(w, "decompression error", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", intToStr(len(plain)))
		w.WriteHeader(http.StatusOK)
		if r.Method != http.MethodHead {
			_, _ = w.Write(plain)
		}
	}
}

func decompressBrotli(data []byte) ([]byte, error) {
	return io.ReadAll(brotli.NewReader(bytes.NewReader(data)))
}

// acceptsEncoding checks whether the Accept-Encoding header includes the given encoding.
func acceptsEncoding(header, encoding string) bool {
	for part := range strings.SplitSeq(header, ",") {
		// Strip quality value (e.g. "br;q=1.0" → "br").
		if enc, _, _ := strings.Cut(strings.TrimSpace(part), ";"); strings.TrimSpace(enc) == encoding {
			return true
		}
	}
	return false
}

func intToStr(n int) string {
	return strconv.Itoa(n)
}
