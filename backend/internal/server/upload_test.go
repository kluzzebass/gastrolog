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
	"gastrolog/internal/glid"
	"gastrolog/internal/notify"
	"gastrolog/internal/orchestrator"
	"gastrolog/internal/server"
	"gastrolog/internal/system"
	sysmem "gastrolog/internal/system/memory"
)

// uploadFixture is an upload endpoint backed by a memory store and a temp home,
// with one admin and one ordinary user on file.
type uploadFixture struct {
	url     string
	homeDir string
	store   system.Store
	tokens  *auth.TokenService
	adminID glid.GLID
	userID  glid.GLID
}

// newUploadFixture builds the endpoint. tokens nil omits the token service,
// which is how a misconfigured node comes up.
func newUploadFixture(t *testing.T, tokens *auth.TokenService, noAuth bool) *uploadFixture {
	t.Helper()
	orch, err := orchestrator.New(orchestrator.Config{})
	if err != nil {
		t.Fatalf("orchestrator.New: %v", err)
	}
	store := sysmem.NewStore()
	homeDir := t.TempDir()

	f := &uploadFixture{
		homeDir: homeDir,
		store:   store,
		tokens:  tokens,
		adminID: glid.New(),
		userID:  glid.New(),
	}
	// The verifier rejects a token whose user is gone, so the callers below
	// have to exist for a role check to be reached at all.
	f.createUser(t, f.adminID, "admin", "admin")
	f.createUser(t, f.userID, "alice", "user")

	// ConfigSignal matches production wiring: RegisterFile signals config
	// watchers once the metadata is committed.
	srv := server.New(orch, store, orchestrator.Factories{}, tokens, server.Config{
		HomeDir:      homeDir,
		NoAuth:       noAuth,
		ConfigSignal: notify.NewSignal(),
	})
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	f.url = ts.URL + "/api/v1/managed-files/upload"
	return f
}

func (f *uploadFixture) createUser(t *testing.T, id glid.GLID, username, role string) {
	t.Helper()
	err := f.store.CreateUser(context.Background(), system.User{
		ID:        id,
		Username:  username,
		Role:      role,
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("CreateUser %q: %v", username, err)
	}
}

// issue mints an access token bound to a live session, the way login does:
// a token that names no session is rejected as one that could never be
// logged out.
func (f *uploadFixture) issue(t *testing.T, id glid.GLID, username, role string) string {
	t.Helper()
	session := glid.New()
	err := f.store.CreateRefreshToken(context.Background(), system.RefreshToken{
		ID:        session,
		UserID:    id,
		TokenHash: "test-" + session.String(),
		ExpiresAt: time.Now().Add(time.Hour),
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("CreateRefreshToken: %v", err)
	}
	token, _, err := f.tokens.Issue(id.String(), username, role, session.String())
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return token
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

// assertNothingStored fails when a rejected upload left a trace, in the
// manifest or on disk.
func (f *uploadFixture) assertNothingStored(t *testing.T) {
	t.Helper()
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
}

func newUploadTokens() *auth.TokenService {
	return auth.NewTokenService([]byte("test-secret-key-32-bytes-long!!"), 7*24*time.Hour)
}

// TestManagedFileUpload_RequiresAdmin covers the role gate on a route the
// Connect auth interceptor never sees. Every case posts an identical body to
// the same endpoint; only the caller's credential differs, so a rejection is
// the credential and nothing else, and the admin case is the premise proving
// the request would otherwise be accepted.
func TestManagedFileUpload_RequiresAdmin(t *testing.T) {
	t.Parallel()

	t.Run("non-admin is forbidden and writes nothing", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t, newUploadTokens(), false)
		resp := f.upload(t, f.issue(t, f.userID, "alice", "user"))
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
		}
		f.assertNothingStored(t)
	})

	t.Run("admin succeeds", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t, newUploadTokens(), false)
		resp := f.upload(t, f.issue(t, f.adminID, "admin", "admin"))
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
		f := newUploadFixture(t, newUploadTokens(), false)
		resp := f.upload(t, "")
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
		}
		f.assertNothingStored(t)
	})
}

// TestManagedFileUpload_RejectsInvalidatedToken covers the freshness half of
// the check: an admin token stays a valid signature after the user is demoted,
// logged out or deleted, and only server-side revocation says so. The Connect
// RPCs consult it, so this route must too.
func TestManagedFileUpload_RejectsInvalidatedToken(t *testing.T) {
	t.Parallel()

	t.Run("token invalidated after issue", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t, newUploadTokens(), false)
		token := f.issue(t, f.adminID, "admin", "admin")

		// Premise: the token works before invalidation, so the rejection below
		// is the revocation and not the token.
		if resp := f.upload(t, token); resp.StatusCode != http.StatusCreated {
			t.Fatalf("before invalidation: status = %d, want %d", resp.StatusCode, http.StatusCreated)
		}

		invalidatedAt := time.Now().UTC().Add(time.Hour)
		if err := f.store.InvalidateTokens(context.Background(), f.adminID, invalidatedAt); err != nil {
			t.Fatalf("InvalidateTokens: %v", err)
		}

		resp := f.upload(t, token)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
		}
	})

	t.Run("deleted user", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t, newUploadTokens(), false)
		token := f.issue(t, f.adminID, "admin", "admin")
		if err := f.store.DeleteUser(context.Background(), f.adminID); err != nil {
			t.Fatalf("DeleteUser: %v", err)
		}

		resp := f.upload(t, token)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
		}
		f.assertNothingStored(t)
	})
}

// TestManagedFileUpload_AuthModes covers the two configurations that skip the
// role check: noAuth serves an operator who turned authentication off, while a
// node with authentication on and no token service can authorize nobody.
func TestManagedFileUpload_AuthModes(t *testing.T) {
	t.Parallel()

	t.Run("noAuth accepts an unauthenticated upload", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t, nil, true)
		resp := f.upload(t, "")
		if resp.StatusCode != http.StatusCreated {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusCreated)
		}
	})

	t.Run("no token service refuses instead of opening up", func(t *testing.T) {
		t.Parallel()
		f := newUploadFixture(t, nil, false)
		resp := f.upload(t, "")
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", resp.StatusCode, http.StatusInternalServerError)
		}
		f.assertNothingStored(t)
	})
}
