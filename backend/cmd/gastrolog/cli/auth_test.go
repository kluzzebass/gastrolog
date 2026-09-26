package cli

// The CLI attaches its bearer token with a Connect interceptor, and an
// interceptor has to opt into streaming separately from unary. Asserting the
// unary half says nothing about the streaming half, which is how every
// streaming command came to reach an authenticated cluster with no
// credentials at all. So this drives real calls at a real listener and reads
// the header off the wire.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	gastrologv1 "gastrolog/api/gen/gastrolog/v1"
	"gastrolog/internal/server"

	"connectrpc.com/connect"
)

// headerRecorder answers every request with an error and remembers the
// Authorization header it arrived with. The call failing is fine: the header
// is attached on the way out, so a refused call still proves what was sent.
type headerRecorder struct {
	mu   sync.Mutex
	seen map[string]string
}

func newHeaderRecorder() *headerRecorder {
	return &headerRecorder{seen: map[string]string{}}
}

func (h *headerRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	h.seen[r.URL.Path] = r.Header.Get("Authorization")
	h.mu.Unlock()
	w.WriteHeader(http.StatusInternalServerError)
}

func (h *headerRecorder) authFor(path string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.seen[path]
	return v, ok
}

func authedClientAgainst(t *testing.T, token string) (*server.Client, *headerRecorder) {
	t.Helper()
	rec := newHeaderRecorder()
	srv := httptest.NewServer(rec)
	t.Cleanup(srv.Close)
	return server.NewClient(srv.URL, connect.WithInterceptors(newAuthInterceptor(token))), rec
}

// Search is the CLI's streaming call — `gastrolog query`. Without the header
// an authenticated cluster answers "missing authorization header" and the
// command fails however valid the operator's token is.
func TestStreamingCallsCarryTheBearerToken(t *testing.T) {
	t.Parallel()
	client, rec := authedClientAgainst(t, "test-token")

	stream, err := client.Query.Search(context.Background(), connect.NewRequest(&gastrologv1.SearchRequest{}))
	if err == nil {
		// The recorder answers 500, so a stream may open and fail on receive.
		stream.Receive()
		_ = stream.Close()
	}

	got, ok := rec.authFor("/gastrolog.v1.QueryService/Search")
	if !ok {
		t.Fatal("the streaming call never reached the server")
	}
	if got != "Bearer test-token" {
		t.Fatalf("streaming Authorization = %q, want %q", got, "Bearer test-token")
	}
}

// The unary half is what already worked, and it has to keep working: a fix
// that moved the header onto streams and off unary calls would trade one
// broken set of commands for another.
func TestUnaryCallsCarryTheBearerToken(t *testing.T) {
	t.Parallel()
	client, rec := authedClientAgainst(t, "test-token")

	_, _ = client.Lifecycle.GetClusterStatus(context.Background(),
		connect.NewRequest(&gastrologv1.GetClusterStatusRequest{}))

	got, ok := rec.authFor("/gastrolog.v1.LifecycleService/GetClusterStatus")
	if !ok {
		t.Fatal("the unary call never reached the server")
	}
	if got != "Bearer test-token" {
		t.Fatalf("unary Authorization = %q, want %q", got, "Bearer test-token")
	}
}
