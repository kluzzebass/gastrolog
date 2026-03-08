package lookup

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPLookup_Basic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"Alice","email":"alice@example.com"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "123")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %q, want Alice", result["name"])
	}
	if result["email"] != "alice@example.com" {
		t.Errorf("email = %q, want alice@example.com", result["email"])
	}
}

func TestHTTPLookup_CacheHit(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"Bob"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}", CacheTTL: time.Minute})

	// First call hits the server.
	r1 := h.Lookup(context.Background(), "456")
	if r1 == nil || r1["name"] != "Bob" {
		t.Fatal("first lookup failed")
	}

	// Second call should come from cache.
	r2 := h.Lookup(context.Background(), "456")
	if r2 == nil || r2["name"] != "Bob" {
		t.Fatal("second lookup failed")
	}

	if calls.Load() != 1 {
		t.Errorf("expected 1 HTTP call, got %d", calls.Load())
	}
}

func TestHTTPLookup_CacheTTLExpiry(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"Charlie"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}", CacheTTL: time.Millisecond})

	h.Lookup(context.Background(), "789")
	time.Sleep(5 * time.Millisecond) // Let TTL expire.
	h.Lookup(context.Background(), "789")

	if calls.Load() != 2 {
		t.Errorf("expected 2 HTTP calls after TTL expiry, got %d", calls.Load())
	}
}

func TestHTTPLookup_HTTPError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "err")

	if result != nil {
		t.Errorf("expected nil result on HTTP error, got %v", result)
	}
}

func TestHTTPLookup_Timeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Write([]byte(`{"name":"slow"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}", Timeout: 50 * time.Millisecond})
	result := h.Lookup(context.Background(), "slow")

	if result != nil {
		t.Errorf("expected nil result on timeout, got %v", result)
	}
}

func TestHTTPLookup_NonStringValues(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"age":42,"active":true,"score":3.14,"disabled":false}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["age"] != "42" {
		t.Errorf("age = %q, want 42", result["age"])
	}
	if result["active"] != "true" {
		t.Errorf("active = %q, want true", result["active"])
	}
	if result["score"] != "3.14" {
		t.Errorf("score = %q, want 3.14", result["score"])
	}
	if result["disabled"] != "false" {
		t.Errorf("disabled = %q, want false", result["disabled"])
	}
}

func TestHTTPLookup_NestedObjectsSerialized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"Alice","address":{"city":"NYC"},"tags":["a","b"],"nothing":null}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %q, want Alice", result["name"])
	}
	if result["address"] != `{"city":"NYC"}` {
		t.Errorf("address = %q, want JSON object string", result["address"])
	}
	if result["tags"] != `["a","b"]` {
		t.Errorf("tags = %q, want JSON array string", result["tags"])
	}
	if _, ok := result["nothing"]; ok {
		t.Error("null 'nothing' should be skipped")
	}
}

func TestHTTPLookup_URLTemplateSubstitution(t *testing.T) {
	var receivedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":"yes"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/users/{value}/info"})
	h.Lookup(context.Background(), "user42")

	if receivedPath != "/users/user42/info" {
		t.Errorf("path = %q, want /users/user42/info", receivedPath)
	}
}

func TestHTTPLookup_URLEncoding(t *testing.T) {
	var receivedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedPath = r.URL.RawPath
		if receivedPath == "" {
			receivedPath = r.URL.Path
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":"yes"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/lookup/{value}"})
	h.Lookup(context.Background(), "hello world/foo")

	if receivedPath != "/lookup/hello%20world%2Ffoo" {
		t.Errorf("path = %q, want /lookup/hello%%20world%%2Ffoo", receivedPath)
	}
}

func TestHTTPLookup_CustomHeaders(t *testing.T) {
	var gotAuth, gotCustom string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotCustom = r.Header.Get("X-Custom")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"ok":"yes"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate: srv.URL + "/{value}",
		Headers: map[string]string{
			"Authorization": "Bearer secret-token",
			"X-Custom":      "custom-value",
		},
	})
	h.Lookup(context.Background(), "x")

	if gotAuth != "Bearer secret-token" {
		t.Errorf("Authorization = %q, want Bearer secret-token", gotAuth)
	}
	if gotCustom != "custom-value" {
		t.Errorf("X-Custom = %q, want custom-value", gotCustom)
	}
}

func TestHTTPLookup_SuffixesDiscovery(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"name":"Alice","email":"a@b.com"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})

	// Before any lookup, suffixes should be nil/empty.
	if s := h.Suffixes(); len(s) != 0 {
		t.Errorf("suffixes before lookup = %v, want empty", s)
	}

	h.Lookup(context.Background(), "x")

	suffixes := h.Suffixes()
	if len(suffixes) != 2 {
		t.Fatalf("suffixes = %v, want 2 entries", suffixes)
	}

	// Check both keys are present (order doesn't matter).
	found := map[string]bool{}
	for _, s := range suffixes {
		found[s] = true
	}
	if !found["name"] || !found["email"] {
		t.Errorf("suffixes = %v, want [name, email]", suffixes)
	}
}

func TestHTTPLookup_EmptyResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "x")

	if result != nil {
		t.Errorf("expected nil for empty response, got %v", result)
	}
}

func TestHTTPLookup_EmptyValue(t *testing.T) {
	h := NewHTTP(HTTPConfig{URLTemplate: "http://localhost/{value}"})
	result := h.Lookup(context.Background(), "")

	if result != nil {
		t.Errorf("expected nil for empty value, got %v", result)
	}
}

func TestHTTPLookup_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "x")

	if result != nil {
		t.Errorf("expected nil for invalid JSON, got %v", result)
	}
}

func TestHTTPLookup_RejectsNonJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`{"name":"Alice"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "x")

	if result != nil {
		t.Errorf("expected nil for non-JSON content type, got %v", result)
	}
}

func TestHTTPLookup_RejectsMissingContentType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"name":"Alice"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "x")

	if result != nil {
		t.Errorf("expected nil for missing content type, got %v", result)
	}
}

func TestHTTPLookup_ResponsePath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":{"user":{"name":"Alice","email":"a@b.com"}}}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate:  srv.URL + "/{value}",
		ResponsePaths: []string{"$.data.user"},
	})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %q, want Alice", result["name"])
	}
	if result["email"] != "a@b.com" {
		t.Errorf("email = %q, want a@b.com", result["email"])
	}
}

func TestHTTPLookup_ResponsePathArray(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"results":[{"name":"First"},{"name":"Second"}]}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate:  srv.URL + "/{value}",
		ResponsePaths: []string{"$.results[0]"},
	})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "First" {
		t.Errorf("name = %q, want First", result["name"])
	}
}

func TestHTTPLookup_ResponsePathMiss(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":{"other":"value"}}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate:  srv.URL + "/{value}",
		ResponsePaths: []string{"$.data.user"},
	})
	result := h.Lookup(context.Background(), "x")

	if result != nil {
		t.Errorf("expected nil for missing path, got %v", result)
	}
}

func TestHTTPLookup_MultipleResponsePaths(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"user":{"name":"Alice","email":"a@b.com"},"account":{"plan":"pro","status":"active"}}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate:   srv.URL + "/{value}",
		ResponsePaths: []string{"$.user", "$.account"},
	})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %q, want Alice", result["name"])
	}
	if result["plan"] != "pro" {
		t.Errorf("plan = %q, want pro", result["plan"])
	}
	if result["status"] != "active" {
		t.Errorf("status = %q, want active", result["status"])
	}
}

func TestHTTPLookup_ResponsePathScalar(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"headers":{"host":"postman-echo.com","accept":"*/*"},"url":"https://example.com"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate:   srv.URL + "/{value}",
		ResponsePaths: []string{"$.headers.host"},
	})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["host"] != "postman-echo.com" {
		t.Errorf("host = %q, want postman-echo.com", result["host"])
	}
	if _, ok := result["url"]; ok {
		t.Error("should not contain root-level 'url' field")
	}
}

func TestHTTPLookup_ResponsePathArrayFlat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"data":{"tags":["alpha","beta","gamma"]}}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate:   srv.URL + "/{value}",
		ResponsePaths: []string{"$.data.tags"},
	})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["tags.0"] != "alpha" {
		t.Errorf("tags.0 = %q, want alpha", result["tags.0"])
	}
	if result["tags.1"] != "beta" {
		t.Errorf("tags.1 = %q, want beta", result["tags.1"])
	}
	if result["tags.2"] != "gamma" {
		t.Errorf("tags.2 = %q, want gamma", result["tags.2"])
	}
}

func TestHTTPLookup_ResponsePathArrayOfObjects(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"items":[{"id":"a","score":10},{"id":"b","score":20}]}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{
		URLTemplate:   srv.URL + "/{value}",
		ResponsePaths: []string{"$.items"},
	})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["items.0.id"] != "a" {
		t.Errorf("items.0.id = %q, want a", result["items.0.id"])
	}
	if result["items.1.score"] != "20" {
		t.Errorf("items.1.score = %q, want 20", result["items.1.score"])
	}
}

func TestHTTPLookup_AcceptsContentTypeWithCharset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write([]byte(`{"name":"Alice"}`))
	}))
	defer srv.Close()

	h := NewHTTP(HTTPConfig{URLTemplate: srv.URL + "/{value}"})
	result := h.Lookup(context.Background(), "x")

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result["name"] != "Alice" {
		t.Errorf("name = %q, want Alice", result["name"])
	}
}
