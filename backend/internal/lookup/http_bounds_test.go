package lookup

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gastrolog/internal/safefetch"
)

// jsonEcho serves a constant JSON object and counts the requests it answered.
func jsonEcho(counter *atomic.Int64, record func(*http.Request)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		counter.Add(1)
		if record != nil {
			record(r)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":"1"}`))
	})
}

func TestHTTPLookupRejectsUnsafeTemplate(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"http://{value}.internal/x", "file:///etc/{value}", ""} {
		if _, err := NewHTTP(HTTPConfig{URLTemplate: raw}); err == nil {
			t.Errorf("NewHTTP(%q) = nil error, want a rejection", raw)
		}
	}
}

// TestHTTPLookupBlocksPrivateDestination pins the default: the fetch fails
// with the destination policy's own error, not merely with "no result", which
// an unreachable address would also produce.
func TestHTTPLookupBlocksPrivateDestination(t *testing.T) {
	t.Parallel()

	for _, target := range []string{
		"http://169.254.169.254/latest/meta-data/",
		"http://127.0.0.1:9/x",
		"http://10.0.0.1/x",
		"http://100.100.100.200/latest/meta-data/",
		"http://[64:ff9b::a9fe:a9fe]/latest/meta-data/",
	} {
		h, err := NewHTTP(HTTPConfig{URLTemplate: target, Timeout: time.Second})
		if err != nil {
			t.Fatalf("NewHTTP(%q): %v", target, err)
		}
		_, err = h.fetch(context.Background(), target)
		if !errors.Is(err, safefetch.ErrBlockedDestination) {
			t.Errorf("fetch(%q) = %v, want ErrBlockedDestination", target, err)
		}
	}
}

// TestHTTPLookupEscapeHatchReachesPrivate is the premise for the test above:
// the same code path succeeds when the operator has opted in, so the refusals
// are the policy rather than an unreachable address.
func TestHTTPLookupEscapeHatchReachesPrivate(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	srv := httptest.NewServer(jsonEcho(&count, nil))
	defer srv.Close()

	denied, err := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}", Timeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("NewHTTP: %v", err)
	}
	if _, err := denied.fetch(context.Background(), srv.URL+"/x"); !errors.Is(err, safefetch.ErrBlockedDestination) {
		t.Fatalf("loopback fetch without the opt-in = %v, want ErrBlockedDestination", err)
	}
	if count.Load() != 0 {
		t.Fatalf("blocked fetch still reached the endpoint (%d requests)", count.Load())
	}

	allowed := newLoopbackHTTP(t, HTTPConfig{URLTemplate: srv.URL + "/{value}", Timeout: 5 * time.Second})
	got, err := allowed.fetch(context.Background(), srv.URL+"/x")
	if err != nil {
		t.Fatalf("permitted fetch: %v", err)
	}
	if got["ok"] != "1" {
		t.Fatalf("permitted fetch returned %v, want the served object", got)
	}
}

// TestHTTPLookupBoundsResponseSize keeps a hostile or misconfigured endpoint
// from pinning memory through a caller-chosen URL.
func TestHTTPLookupBoundsResponseSize(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pad":"`))
		chunk := make([]byte, 64<<10)
		for i := range chunk {
			chunk[i] = 'a'
		}
		for written := 0; written < maxResponseBytes+len(chunk); written += len(chunk) {
			if _, err := w.Write(chunk); err != nil {
				return
			}
		}
		_, _ = w.Write([]byte(`"}`))
	}))
	defer srv.Close()

	h := newLoopbackHTTP(t, HTTPConfig{URLTemplate: srv.URL + "/{value}", Timeout: 10 * time.Second})
	if _, err := h.fetch(context.Background(), srv.URL+"/x"); !errors.Is(err, errResponseTooLarge) {
		t.Fatalf("fetch of an oversized response = %v, want errResponseTooLarge", err)
	}
}

// TestHTTPLookupCapsTimeout keeps one lookup from holding a query open for as
// long as the configuration asks.
func TestHTTPLookupCapsTimeout(t *testing.T) {
	t.Parallel()

	h := newLoopbackHTTP(t, HTTPConfig{URLTemplate: "http://127.0.0.1:9/{value}", Timeout: time.Hour})
	if got := h.client.Timeout; got != maxHTTPTimeout {
		t.Fatalf("client timeout = %v, want it capped at %v", got, maxHTTPTimeout)
	}
}

// TestHTTPLookupRecordValueCannotChangeHost is the end-to-end form of the
// template guard: the request reaches the configured server whatever the
// record carried, and the value stays inside one path segment.
func TestHTTPLookupRecordValueCannotChangeHost(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	var mu sync.Mutex
	var paths []string
	srv := httptest.NewServer(jsonEcho(&count, func(r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.EscapedPath())
		mu.Unlock()
	}))
	defer srv.Close()

	h := newLoopbackHTTP(t, HTTPConfig{URLTemplate: srv.URL + "/lookup/{value}"})

	hostile := []string{
		"trusted.internal@evil.example.com",
		"http://evil.example.com/",
		"evil.example.com:8080",
		"../../admin",
		"a/b/c",
	}
	for _, v := range hostile {
		if got := h.LookupValues(context.Background(), map[string]string{"value": v}); got == nil {
			t.Fatalf("lookup with value %q did not reach the configured server", v)
		}
	}
	if int(count.Load()) != len(hostile) {
		t.Fatalf("server answered %d requests, want %d", count.Load(), len(hostile))
	}

	mu.Lock()
	defer mu.Unlock()
	for i, p := range paths {
		want := "/lookup/"
		if len(p) <= len(want) || p[:len(want)] != want {
			t.Errorf("value %q produced path %q, want it under %q", hostile[i], p, want)
		}
	}
}

// TestHTTPLookupOutboundBudget proves one query cannot chain unbounded outbound
// requests through a high-cardinality field.
func TestHTTPLookupOutboundBudget(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	srv := httptest.NewServer(jsonEcho(&count, nil))
	defer srv.Close()

	h := newLoopbackHTTP(t, HTTPConfig{URLTemplate: srv.URL + "/{value}"})

	const budget = 3
	ctx := WithOutboundBudget(context.Background(), budget)
	var served int
	for i := range budget + 5 {
		if h.LookupValues(ctx, map[string]string{"value": strconv.Itoa(i)}) != nil {
			served++
		}
	}
	if served != budget {
		t.Errorf("%d lookups enriched, want %d", served, budget)
	}
	if int(count.Load()) != budget {
		t.Errorf("server answered %d requests, want %d", count.Load(), budget)
	}
	if remaining, ok := OutboundRemaining(ctx); !ok || remaining != 0 {
		t.Errorf("remaining budget = %d (present %v), want 0", remaining, ok)
	}

	// A cache hit costs nothing, so a repeated value still enriches after the
	// budget is spent.
	if h.LookupValues(ctx, map[string]string{"value": "0"}) == nil {
		t.Error("cached value stopped resolving once the budget was spent")
	}
	if int(count.Load()) != budget {
		t.Errorf("cache hit issued a request: %d total, want %d", count.Load(), budget)
	}
}

func TestHTTPLookupWithoutBudgetIsUnbounded(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	srv := httptest.NewServer(jsonEcho(&count, nil))
	defer srv.Close()

	h := newLoopbackHTTP(t, HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	const n = 20
	for i := range n {
		if h.LookupValues(context.Background(), map[string]string{"value": strconv.Itoa(i)}) == nil {
			t.Fatalf("lookup %d returned nil", i)
		}
	}
	if int(count.Load()) != n {
		t.Errorf("server answered %d requests, want %d", count.Load(), n)
	}
}

// TestHTTPLookupBoundsConcurrency holds every request open until the cap is
// reached, so the observed peak is the cap itself rather than a timing artifact.
func TestHTTPLookupBoundsConcurrency(t *testing.T) {
	t.Parallel()

	var inFlight, peak atomic.Int64
	release := make(chan struct{})
	var once sync.Once

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := inFlight.Add(1)
		for {
			old := peak.Load()
			if n <= old || peak.CompareAndSwap(old, n) {
				break
			}
		}
		if n >= maxConcurrentFetches {
			once.Do(func() { close(release) })
		}
		select {
		case <-release:
		case <-time.After(5 * time.Second): // only reached when the cap is wrong
		}
		inFlight.Add(-1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":"1"}`))
	}))
	defer srv.Close()

	h := newLoopbackHTTP(t, HTTPConfig{URLTemplate: srv.URL + "/{value}", Timeout: 10 * time.Second})

	var wg sync.WaitGroup
	for i := range maxConcurrentFetches * 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h.LookupValues(context.Background(), map[string]string{"value": strconv.Itoa(i)})
		}()
	}
	wg.Wait()

	if got := peak.Load(); got != maxConcurrentFetches {
		t.Fatalf("peak concurrent requests = %d, want %d", got, maxConcurrentFetches)
	}
}

// TestHTTPLookupReportsBudgetExhaustion keeps the bound from being silent: an
// operator reading partial enrichment needs a line saying why, once per query
// rather than once per record.
func TestHTTPLookupReportsBudgetExhaustion(t *testing.T) {
	t.Parallel()

	var count atomic.Int64
	srv := httptest.NewServer(jsonEcho(&count, nil))
	defer srv.Close()

	var buf bytes.Buffer
	var mu sync.Mutex
	logger := slog.New(slog.NewTextHandler(&syncWriter{w: &buf, mu: &mu}, &slog.HandlerOptions{Level: slog.LevelDebug}))

	h := newLoopbackHTTP(t, HTTPConfig{URLTemplate: srv.URL + "/{value}", Logger: logger, Name: "probe"})

	const budget = 2
	ctx := WithOutboundBudget(context.Background(), budget)
	for i := range budget + 4 {
		h.LookupValues(ctx, map[string]string{"value": strconv.Itoa(i)})
	}

	mu.Lock()
	logged := buf.String()
	mu.Unlock()
	if n := strings.Count(logged, "outbound lookup budget exhausted"); n != 1 {
		t.Fatalf("budget exhaustion logged %d times, want exactly 1:\n%s", n, logged)
	}
	if !strings.Contains(logged, `table=probe`) {
		t.Errorf("log line does not name the table:\n%s", logged)
	}
}

// syncWriter serializes writes from the lookup table's concurrent callers.
type syncWriter struct {
	w  *bytes.Buffer
	mu *sync.Mutex
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}
