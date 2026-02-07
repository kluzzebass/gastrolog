package repl

import (
	"io"
	"net/http"
	"sync"

	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

// NewEmbeddedClient creates a client that connects to an in-process server.
// This provides the same gRPC interface as a remote connection but without
// any network overhead - ideal for embedded/standalone mode.
func NewEmbeddedClient(orch *orchestrator.Orchestrator) *GRPCClient {
	srv := server.New(orch, nil, orchestrator.Factories{}, server.Config{})
	handler := srv.Handler()

	// Create a custom HTTP client that routes requests directly to the handler
	transport := &embeddedTransport{handler: handler}
	httpClient := &http.Client{Transport: transport}

	return NewGRPCClientWithHTTP(httpClient, "http://embedded")
}

// embeddedTransport is an http.RoundTripper that routes requests
// directly to an http.Handler without going through the network.
// It uses a pipe-based response writer to support streaming RPCs.
type embeddedTransport struct {
	handler http.Handler
}

func (t *embeddedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Use a pipe-based response writer for streaming support.
	// This allows the handler to write incrementally while the client reads.
	pr, pw := io.Pipe()
	rw := &pipeResponseWriter{
		pw:       pw,
		header:   make(http.Header),
		headerCh: make(chan struct{}),
	}

	// Run the handler in a goroutine so it can write to the pipe
	// while the client reads from it.
	go func() {
		defer pw.Close()
		t.handler.ServeHTTP(rw, req)
		// Ensure header channel is closed even if WriteHeader was never called.
		rw.WriteHeader(http.StatusOK)
	}()

	// Wait for the header to be written before returning.
	<-rw.headerCh

	resp := &http.Response{
		StatusCode:    rw.statusCode,
		Status:        http.StatusText(rw.statusCode),
		Header:        rw.header,
		Body:          pr,
		ContentLength: -1, // Streaming, unknown length
		Request:       req,
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
	}

	return resp, nil
}

// pipeResponseWriter implements http.ResponseWriter using an io.PipeWriter.
// It supports streaming responses by writing directly to the pipe.
type pipeResponseWriter struct {
	pw         *pw
	header     http.Header
	statusCode int
	headerOnce sync.Once
	headerCh   chan struct{}
}

type pw = io.PipeWriter

func (w *pipeResponseWriter) Header() http.Header {
	return w.header
}

func (w *pipeResponseWriter) WriteHeader(code int) {
	w.headerOnce.Do(func() {
		w.statusCode = code
		if w.headerCh != nil {
			close(w.headerCh)
		}
	})
}

func (w *pipeResponseWriter) Write(data []byte) (int, error) {
	// Implicitly write header if not already done.
	w.WriteHeader(http.StatusOK)
	return w.pw.Write(data)
}

// Flush implements http.Flusher for streaming support.
func (w *pipeResponseWriter) Flush() {
	// Pipe automatically flushes on write, nothing needed here.
}
