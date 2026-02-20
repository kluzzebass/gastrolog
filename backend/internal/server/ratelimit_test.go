package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/time/rate"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
)

func TestRateLimitMiddleware_NonAuthPaths(t *testing.T) {
	rl := newRateLimiter(rate.Limit(1), 1) // very restrictive
	handler := rateLimitMiddleware(rl)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	// Non-auth path should always pass through.
	for range 10 {
		req := httptest.NewRequest("POST", "/gastrolog.v1.QueryService/Search", nil)
		req.RemoteAddr = "1.2.3.4:5678"
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("non-auth path: expected 200, got %d", rr.Code)
		}
	}
}

func TestRateLimitMiddleware_AuthPathThrottled(t *testing.T) {
	rl := newRateLimiter(rate.Limit(1), 2) // 1 req/s, burst 2
	handler := rateLimitMiddleware(rl)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	path := gastrologv1connect.AuthServiceLoginProcedure
	ip := "10.0.0.1:1234"

	// First 2 requests (burst) should succeed.
	for i := range 2 {
		req := httptest.NewRequest("POST", path, nil)
		req.RemoteAddr = ip
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("request %d: expected 200, got %d", i, rr.Code)
		}
	}

	// 3rd request should be throttled.
	req := httptest.NewRequest("POST", path, nil)
	req.RemoteAddr = ip
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("expected 429, got %d", rr.Code)
	}

	// Verify Connect-compatible JSON error.
	var body connectError
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Code != "resource_exhausted" {
		t.Errorf("expected code %q, got %q", "resource_exhausted", body.Code)
	}
}

func TestRateLimitMiddleware_DifferentIPsIndependent(t *testing.T) {
	rl := newRateLimiter(rate.Limit(1), 1) // 1 req/s, burst 1
	handler := rateLimitMiddleware(rl)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	path := gastrologv1connect.AuthServiceRegisterProcedure

	// First IP: exhaust the burst.
	req1 := httptest.NewRequest("POST", path, nil)
	req1.RemoteAddr = "10.0.0.1:1000"
	rr1 := httptest.NewRecorder()
	handler.ServeHTTP(rr1, req1)
	if rr1.Code != http.StatusOK {
		t.Fatalf("ip1 first request: expected 200, got %d", rr1.Code)
	}

	// First IP: second request should be throttled.
	req1b := httptest.NewRequest("POST", path, nil)
	req1b.RemoteAddr = "10.0.0.1:1000"
	rr1b := httptest.NewRecorder()
	handler.ServeHTTP(rr1b, req1b)
	if rr1b.Code != http.StatusTooManyRequests {
		t.Fatalf("ip1 second request: expected 429, got %d", rr1b.Code)
	}

	// Second IP: should still succeed (independent limiter).
	req2 := httptest.NewRequest("POST", path, nil)
	req2.RemoteAddr = "10.0.0.2:2000"
	rr2 := httptest.NewRecorder()
	handler.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusOK {
		t.Fatalf("ip2 first request: expected 200, got %d", rr2.Code)
	}
}

func TestRateLimiterCleanup(t *testing.T) {
	rl := newRateLimiter(rate.Limit(1), 1)

	// Create an entry.
	rl.getLimiter("1.2.3.4")

	rl.mu.Lock()
	if len(rl.limiters) != 1 {
		t.Fatalf("expected 1 limiter, got %d", len(rl.limiters))
	}
	rl.mu.Unlock()

	// Cleanup with 0 stale duration should remove it.
	rl.cleanup(0)

	rl.mu.Lock()
	if len(rl.limiters) != 0 {
		t.Fatalf("expected 0 limiters after cleanup, got %d", len(rl.limiters))
	}
	rl.mu.Unlock()
}
