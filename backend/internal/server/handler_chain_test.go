package server_test

// Handler() is what tests drive. It has to be the chain the listeners
// serve, or a middleware can be dropped, reordered, or misconfigured and
// every handler-level test still passes. These assert the middleware is
// present through Handler(), one property per layer, so the parity itself
// is what breaks rather than some unrelated test that happened to rely on
// a header.

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"gastrolog/api/gen/gastrolog/v1/gastrologv1connect"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	sysmem "gastrolog/internal/system/memory"
)

func newChainTestHandler(t *testing.T) http.Handler {
	t.Helper()
	cfgStore := sysmem.NewStore()
	orch, err := orchestrator.New(orchestrator.Config{SystemLoader: cfgStore})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	srv := server.New(orch, cfgStore, orchestrator.Factories{VaultsDir: t.TempDir()}, nil,
		server.Config{NoAuth: true})
	return srv.Handler()
}

func TestHandlerCarriesSecurityHeaders(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	newChainTestHandler(t).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))

	for header, want := range map[string]string{
		"X-Content-Type-Options": "nosniff",
		"X-Frame-Options":        "DENY",
		"Referrer-Policy":        "strict-origin-when-cross-origin",
	} {
		if got := rec.Header().Get(header); got != want {
			t.Errorf("%s = %q, want %q", header, got, want)
		}
	}
	for _, header := range []string{"Content-Security-Policy", "Permissions-Policy"} {
		if rec.Header().Get(header) == "" {
			t.Errorf("%s is absent", header)
		}
	}
}

func TestHandlerAnswersCORSPreflight(t *testing.T) {
	t.Parallel()
	// The relaxed rule is loopback-only, so the request has to look like
	// the dev proxy it exists for: browser on one loopback port, API on
	// another.
	req := httptest.NewRequest(http.MethodOptions, gastrologv1connect.SystemServiceGetSystemProcedure, nil)
	req.Host = "localhost:4564"
	req.Header.Set("Origin", "http://localhost:3001")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)

	rec := httptest.NewRecorder()
	newChainTestHandler(t).ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Fatalf("preflight status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3001" {
		t.Errorf("Access-Control-Allow-Origin = %q, want the requesting origin", got)
	}
	if rec.Header().Get("Access-Control-Allow-Methods") == "" {
		t.Error("preflight names no allowed methods")
	}

	// A cross-origin caller that is not loopback gets nothing back, so the
	// assertion above is about the chain carrying CORS, not about it
	// reflecting whatever Origin arrives.
	foreign := httptest.NewRequest(http.MethodOptions, gastrologv1connect.SystemServiceGetSystemProcedure, nil)
	foreign.Host = "logs.example.com"
	foreign.Header.Set("Origin", "http://evil.example.net")
	foreignRec := httptest.NewRecorder()
	newChainTestHandler(t).ServeHTTP(foreignRec, foreign)
	if got := foreignRec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("Access-Control-Allow-Origin = %q for a foreign origin, want none", got)
	}
}

// Login is rate limited by IP. Enough attempts from one address must start
// being refused, which is only observable if the limiter is in the chain.
func TestHandlerRateLimitsLogin(t *testing.T) {
	t.Parallel()
	handler := newChainTestHandler(t)

	limited := false
	for range 200 {
		req := httptest.NewRequest(http.MethodPost, gastrologv1connect.AuthServiceLoginProcedure, nil)
		req.RemoteAddr = "203.0.113.7:5555"
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == http.StatusTooManyRequests {
			limited = true
			break
		}
	}
	if !limited {
		t.Fatal("login was never rate limited; the limiter is not in the chain Handler() returns")
	}

	// The limit is per address, so a different caller is unaffected.
	req := httptest.NewRequest(http.MethodPost, gastrologv1connect.AuthServiceLoginProcedure, nil)
	req.RemoteAddr = "203.0.113.8:5555"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusTooManyRequests {
		t.Fatal("a second address was refused; the limiter is not keyed by caller")
	}
}
