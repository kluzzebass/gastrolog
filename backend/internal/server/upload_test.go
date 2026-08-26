package server_test

import (
	"bytes"
	"context"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gastrolog/internal/auth"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// uploadFixture is an upload endpoint backed by a memory store and a temp home.
type uploadFixture struct {
	url     string
	homeDir string
	store   system.Store
	tokens  *auth.TokenService
}

func newUploadFixture(t *testing.T) *uploadFixture {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	store := sysmem.NewStore()
	tokens := auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
	homeDir := t.TempDir()

	// ConfigSignal matches production wiring: RegisterFile signals config
	// watchers once the metadata is committed.
	srv := server.New(orch, store, orchestrator.Factories{}, tokens, server.Config{
		HomeDir:      homeDir,
		ConfigSignal: notify.NewSignal(),
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	return &uploadFixture{
		url:     ts.URL + "/api/v1/managed-files/upload",
		homeDir: homeDir,
		store:   store,
		tokens:  tokens,
	}
}

// upload posts a one-file multipart body, with token as the bearer credential
// when non-empty.
func (f *uploadFixture) upload(t *testing.T, token string) *http.Response {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	part, err := mw.CreateFormFile("file", "geoip-overrides.csv")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := part.Write([]byte("ip,country\n10.0.0.1,NO\n")); err != nil {
		t.Fatalf("write part: %v", err)
	}
	if err := mw.Close(); err != nil {
		t.Fatalf("close multipart: %v", err)
	}

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, f.url, &body)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

// managedFileEntries lists what landed under the home's managed-files
// directory, including any leftover temp part. A missing directory counts as
// empty.
func (f *uploadFixture) managedFileEntries(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(f.homeDir, "managed-files"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read managed-files dir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// TestManagedFileUpload_RequiresAdmin covers the role gate on a route the
// Connect auth interceptor never sees. Both cases post an identical body to the
// same endpoint with a valid token; only the role differs, so the 403 is the
// role check and nothing else, and the admin case is the premise proving the
// request would otherwise be accepted.
func TestManagedFileUpload_RequiresAdmin(t *testing.T) {
	t.Parallel()

	t.Run("non-admin is forbidden and writes nothing", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t)
		token, _, err := f.tokens.Issue("uid-alice", "alice", "user")
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}

		resp := f.upload(t, token)
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
		}

		files, err := f.store.ListManagedFiles(context.Background())
		if err != nil {
			t.Fatalf("ListManagedFiles: %v", err)
		}
		if len(files) != 0 {
			t.Errorf("registered %d managed files, want none", len(files))
		}
		if entries := f.managedFileEntries(t); len(entries) != 0 {
			t.Errorf("wrote %v to the managed files directory, want nothing", entries)
		}
	})

	t.Run("admin succeeds", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t)
		token, _, err := f.tokens.Issue("uid-admin", "admin", "admin")
		if err != nil {
			t.Fatalf("Issue: %v", err)
		}

		resp := f.upload(t, token)
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
		}

		files, err := f.store.ListManagedFiles(context.Background())
		if err != nil {
			t.Fatalf("ListManagedFiles: %v", err)
		}
		if len(files) != 1 {
			t.Fatalf("registered %d managed files, want 1", len(files))
		}
		if files[0].Name != "geoip-overrides.csv" {
			t.Errorf("name = %q, want %q", files[0].Name, "geoip-overrides.csv")
		}
	})

	t.Run("no token is unauthorized", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t)
		resp := f.upload(t, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
		}
	})
}
