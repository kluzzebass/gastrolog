package lookup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
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

// TestHTTPLookupBlocksPrivateDestination pins the default: a lookup pointed at
// the instance metadata endpoint never connects.
func TestHTTPLookupBlocksPrivateDestination(t *testing.T) {
	t.Parallel()

	for _, target := range []string{
		"http://169.254.169.254/latest/meta-data/{value}",
		"http://127.0.0.1:9/{value}",
		"http://10.0.0.1/{value}",
	} {
		h, err := NewHTTP(HTTPConfig{URLTemplate: target, Timeout: time.Second})
		if err != nil {
			t.Fatalf("NewHTTP(%q): %v", target, err)
		}
		if got := h.LookupValues(context.Background(), map[string]string{"value": "x"}); got != nil {
			t.Errorf("lookup against %q returned %v, want nil", target, got)
		}
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
