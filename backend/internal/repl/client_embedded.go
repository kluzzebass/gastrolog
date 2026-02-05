package repl

import (
	"net/http"
	"net/http/httptest"

	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

// NewEmbeddedClient creates a client that connects to an in-process server.
// This provides the same gRPC interface as a remote connection but without
// any network overhead - ideal for embedded/standalone mode.
func NewEmbeddedClient(orch *orchestrator.Orchestrator) *GRPCClient {
	srv := server.New(orch, server.Config{})
	handler := srv.Handler()

	// Create a custom HTTP client that routes requests directly to the handler
	transport := &embeddedTransport{handler: handler}
	httpClient := &http.Client{Transport: transport}

	return NewGRPCClientWithHTTP(httpClient, "http://embedded")
}

// embeddedTransport is an http.RoundTripper that routes requests
// directly to an http.Handler without going through the network.
type embeddedTransport struct {
	handler http.Handler
}

func (t *embeddedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Use httptest.ResponseRecorder to capture the response
	rec := httptest.NewRecorder()

	// Serve the request directly
	t.handler.ServeHTTP(rec, req)

	// Convert the recorded response to an http.Response
	return rec.Result(), nil
}
