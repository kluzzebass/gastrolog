package app

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchBootstrapTokenWithRetry_HappyPath(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(bootstrapTokenSecretHeader) != "shh" {
			http.Error(w, "bad secret", http.StatusUnauthorized)
			return
		}
		_, _ = io.WriteString(w, "fetched-token")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := fetchBootstrapTokenWithRetry(ctx, srv.URL, "shh", slog.Default())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got != "fetched-token" {
		t.Errorf("token = %q, want %q", got, "fetched-token")
	}
}

func TestFetchBootstrapTokenWithRetry_FailsFastOn401(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no", http.StatusUnauthorized)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	_, err := fetchBootstrapTokenWithRetry(ctx, srv.URL, "wrong", slog.Default())
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	// 401 is fatal — must not retry, must not eat the full 30s timeout.
	if elapsed > 2*time.Second {
		t.Errorf("401 took %v to surface, expected <2s (no retry on auth failure)", elapsed)
	}
}

func TestFetchBootstrapTokenWithRetry_RetriesTransient(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// First two calls return 503 (endpoint not yet up); third
		// returns the token. Models the bootstrap-node startup race.
		if calls.Add(1) < 3 {
			http.Error(w, "warming up", http.StatusServiceUnavailable)
			return
		}
		_, _ = io.WriteString(w, "ready-now")
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got, err := fetchBootstrapTokenWithRetry(ctx, srv.URL, "any", slog.Default())
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if got != "ready-now" {
		t.Errorf("token = %q, want %q", got, "ready-now")
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("expected 3 calls (2 transient + 1 success), got %d", got)
	}
}

func TestResolveJoinTokenFromSources_LiteralWins(t *testing.T) {
	t.Parallel()
	cfg := RunConfig{
		JoinAddr:          "member:4566",
		JoinToken:         "literal",
		BootstrapTokenURL: "http://should-be-ignored/cluster/bootstrap-token",
	}
	if err := resolveJoinTokenFromSources(context.Background(), &cfg, slog.Default()); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cfg.JoinToken != "literal" {
		t.Errorf("JoinToken = %q, want literal", cfg.JoinToken)
	}
}

func TestResolveJoinTokenFromSources_NoSourceNoOp(t *testing.T) {
	t.Parallel()
	cfg := RunConfig{} // bootstrap node — no JoinAddr, no token sources
	if err := resolveJoinTokenFromSources(context.Background(), &cfg, slog.Default()); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cfg.JoinToken != "" {
		t.Errorf("JoinToken = %q, want empty", cfg.JoinToken)
	}
}
