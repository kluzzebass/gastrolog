package server_test

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
)

// embeddedTransport from lifecycle_test.go is reused via the shared
// package-test scope.

func newBootstrapTokenServer(t *testing.T, secret string, tokenFn func() (string, error)) *http.Client {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(orch, nil, orchestrator.Factories{}, nil, server.Config{
		BootstrapTokenServeSecret: secret,
		BootstrapTokenFn:          tokenFn,
	})
	return &http.Client{Transport: &embeddedTransport{handler: srv.Handler()}}
}

func TestBootstrapTokenEndpoint_HappyPath(t *testing.T) {
	t.Parallel()
	cli := newBootstrapTokenServer(t, "shared-secret", func() (string, error) {
		return "the-token", nil
	})

	req, _ := http.NewRequest("GET", "http://embedded/cluster/bootstrap-token", nil)
	req.Header.Set("X-Bootstrap-Token-Secret", "shared-secret")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if got := strings.TrimSpace(string(body)); got != "the-token" {
		t.Errorf("body = %q, want %q", got, "the-token")
	}
}

func TestBootstrapTokenEndpoint_BadSecretIs401(t *testing.T) {
	t.Parallel()
	cli := newBootstrapTokenServer(t, "shared-secret", func() (string, error) {
		return "the-token", nil
	})

	req, _ := http.NewRequest("GET", "http://embedded/cluster/bootstrap-token", nil)
	req.Header.Set("X-Bootstrap-Token-Secret", "wrong")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestBootstrapTokenEndpoint_MissingSecretIs401(t *testing.T) {
	t.Parallel()
	cli := newBootstrapTokenServer(t, "shared-secret", func() (string, error) {
		return "the-token", nil
	})

	req, _ := http.NewRequest("GET", "http://embedded/cluster/bootstrap-token", nil)
	// No X-Bootstrap-Token-Secret header at all.
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

func TestBootstrapTokenEndpoint_TokenFnErrorIs503(t *testing.T) {
	t.Parallel()
	cli := newBootstrapTokenServer(t, "shared-secret", func() (string, error) {
		return "", errors.New("cluster TLS not initialized")
	})

	req, _ := http.NewRequest("GET", "http://embedded/cluster/bootstrap-token", nil)
	req.Header.Set("X-Bootstrap-Token-Secret", "shared-secret")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", resp.StatusCode)
	}
}

func TestBootstrapTokenEndpoint_DisabledWhenSecretEmpty(t *testing.T) {
	t.Parallel()
	// No serve secret configured — endpoint must not be registered.
	// The mux falls through to the frontend handler (SPA index.html);
	// the contract we verify is that the response body does NOT contain
	// the token, regardless of the status code the fallback returns.
	cli := newBootstrapTokenServer(t, "", func() (string, error) {
		return "the-token", nil
	})

	req, _ := http.NewRequest("GET", "http://embedded/cluster/bootstrap-token", nil)
	req.Header.Set("X-Bootstrap-Token-Secret", "anything")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if strings.Contains(string(body), "the-token") {
		t.Errorf("response leaked token: %q (endpoint should be disabled)", string(body))
	}
}

func TestBootstrapTokenEndpoint_NonGETIs405(t *testing.T) {
	t.Parallel()
	cli := newBootstrapTokenServer(t, "shared-secret", func() (string, error) {
		return "the-token", nil
	})

	req, _ := http.NewRequest("POST", "http://embedded/cluster/bootstrap-token", nil)
	req.Header.Set("X-Bootstrap-Token-Secret", "shared-secret")
	resp, err := cli.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}
