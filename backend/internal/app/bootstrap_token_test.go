package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestWriteBootstrapTokenAtomic_CreatesDirAndChmods(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deeper", "token")
	if err := writeBootstrapTokenAtomic(path, "abc123"); err != nil {
		t.Fatalf("write: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := info.Mode().Perm(); got != bootstrapTokenFileMode {
		t.Errorf("file mode = %o, want %o", got, bootstrapTokenFileMode)
	}
	got, err := readTokenFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "abc123" {
		t.Errorf("contents = %q, want %q", got, "abc123")
	}
}

func TestWriteBootstrapTokenAtomic_OverwritesExisting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := writeBootstrapTokenAtomic(path, "first"); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if err := writeBootstrapTokenAtomic(path, "second"); err != nil {
		t.Fatalf("write 2: %v", err)
	}
	got, err := readTokenFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "second" {
		t.Errorf("contents after overwrite = %q, want %q", got, "second")
	}
}

func TestReadBootstrapTokenWithRetry_PollsUntilPresent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")

	// Write the file from another goroutine after a short delay so the
	// reader has to poll at least once.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = writeBootstrapTokenAtomic(path, "delivered")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	got, err := readBootstrapTokenWithRetry(ctx, path, slog.Default())
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "delivered" {
		t.Errorf("token = %q, want %q", got, "delivered")
	}
}

func TestReadBootstrapTokenWithRetry_TimesOut(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "never-exists")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := readBootstrapTokenWithRetry(ctx, path, slog.Default())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want context.DeadlineExceeded", err)
	}
}

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
		JoinAddr:           "leader:4566",
		JoinToken:          "literal",
		BootstrapTokenFile: "/should-be-ignored",
	}
	if err := resolveJoinTokenFromSources(context.Background(), &cfg, slog.Default()); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cfg.JoinToken != "literal" {
		t.Errorf("JoinToken = %q, want literal", cfg.JoinToken)
	}
}

func TestResolveJoinTokenFromSources_FromFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	if err := writeBootstrapTokenAtomic(path, "from-file"); err != nil {
		t.Fatal(err)
	}
	cfg := RunConfig{
		JoinAddr:           "leader:4566",
		BootstrapTokenFile: path,
	}
	if err := resolveJoinTokenFromSources(context.Background(), &cfg, slog.Default()); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cfg.JoinToken != "from-file" {
		t.Errorf("JoinToken = %q, want from-file", cfg.JoinToken)
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

func TestResolveJoinTokenFromSources_FileWithoutJoinAddrErrs(t *testing.T) {
	t.Parallel()
	cfg := RunConfig{
		BootstrapTokenFile: "/some/path",
	}
	err := resolveJoinTokenFromSources(context.Background(), &cfg, slog.Default())
	if err == nil {
		t.Fatal("expected error for token source without --join-addr, got nil")
	}
}
