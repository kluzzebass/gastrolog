package app

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/auth"
	sysmem "gastrolog/internal/system/memory"
)

func newTestStore(t *testing.T) *sysmem.Store {
	t.Helper()
	return sysmem.NewStore()
}

func TestProvisionInitialAdmin_NoOpWhenJoiner(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	cfg := RunConfig{
		JoinAddr:             "leader:4566",
		InitialAdminUser:     "admin",
		InitialAdminPassword: "password123",
	}
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("provision: %v", err)
	}
	count, _ := store.CountUsers(context.Background())
	if count != 0 {
		t.Errorf("joiners must not provision; got %d users", count)
	}
}

func TestProvisionInitialAdmin_NoOpWhenUnconfigured(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	cfg := RunConfig{} // bootstrap node, no provisioning configured
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("provision: %v", err)
	}
	count, _ := store.CountUsers(context.Background())
	if count != 0 {
		t.Errorf("unconfigured bootstrap must not provision; got %d users", count)
	}
}

func TestProvisionInitialAdmin_FromEnvVars(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	cfg := RunConfig{
		InitialAdminUser:     "admin",
		InitialAdminPassword: "password123",
	}
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("provision: %v", err)
	}
	users, _ := store.ListUsers(context.Background())
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}
	if users[0].Username != "admin" {
		t.Errorf("username = %q, want admin", users[0].Username)
	}
	if users[0].Role != "admin" {
		t.Errorf("role = %q, want admin", users[0].Role)
	}
	// Verify the password actually hashes correctly through the same
	// auth path the login flow uses.
	ok, err := auth.VerifyPassword("password123", users[0].PasswordHash)
	if err != nil {
		t.Errorf("VerifyPassword: %v", err)
	}
	if !ok {
		t.Error("password hash does not verify against the original password")
	}
}

func TestProvisionInitialAdmin_FromFileJSON(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "admin.json")
	if err := os.WriteFile(path, []byte(`{"username": "ops", "password": "secret-1234"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t)
	cfg := RunConfig{InitialAdminFile: path}
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("provision: %v", err)
	}
	users, _ := store.ListUsers(context.Background())
	if len(users) != 1 || users[0].Username != "ops" {
		t.Errorf("expected user 'ops', got %#v", users)
	}
}

func TestProvisionInitialAdmin_FromFileColon(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "admin.colon")
	if err := os.WriteFile(path, []byte("ops:secret-1234\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t)
	cfg := RunConfig{InitialAdminFile: path}
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("provision: %v", err)
	}
	users, _ := store.ListUsers(context.Background())
	if len(users) != 1 || users[0].Username != "ops" {
		t.Errorf("expected user 'ops', got %#v", users)
	}
}

func TestProvisionInitialAdmin_FileWinsOverEnv(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "admin.json")
	if err := os.WriteFile(path, []byte(`{"username": "from-file", "password": "filefile1234"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t)
	cfg := RunConfig{
		InitialAdminFile:     path,
		InitialAdminUser:     "from-env",
		InitialAdminPassword: "envenvenv1234",
	}
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("provision: %v", err)
	}
	users, _ := store.ListUsers(context.Background())
	if len(users) != 1 || users[0].Username != "from-file" {
		t.Errorf("file should win over env; got users=%#v", users)
	}
}

func TestProvisionInitialAdmin_IdempotentOnRestart(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	cfg := RunConfig{
		InitialAdminUser:     "admin",
		InitialAdminPassword: "password123",
	}
	// First call: provisions.
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Second call (simulating a restart): no-op.
	if err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default()); err != nil {
		t.Fatalf("second: %v", err)
	}
	users, _ := store.ListUsers(context.Background())
	if len(users) != 1 {
		t.Errorf("expected 1 user after restart-no-op, got %d", len(users))
	}
}

func TestProvisionInitialAdmin_RejectsInvalidUsername(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	cfg := RunConfig{
		InitialAdminUser:     "ad", // too short
		InitialAdminPassword: "password123",
	}
	err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default())
	if err == nil {
		t.Fatal("expected validation error for short username")
	}
	count, _ := store.CountUsers(context.Background())
	if count != 0 {
		t.Errorf("invalid creds must not create user; got %d users", count)
	}
}

func TestProvisionInitialAdmin_RejectsShortPassword(t *testing.T) {
	t.Parallel()
	store := newTestStore(t)
	cfg := RunConfig{
		InitialAdminUser:     "admin",
		InitialAdminPassword: "short", // <8 chars
	}
	err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default())
	if err == nil {
		t.Fatal("expected validation error for short password")
	}
}

func TestProvisionInitialAdmin_RejectsEmptyFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty")
	if err := os.WriteFile(path, []byte(""), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t)
	cfg := RunConfig{InitialAdminFile: path}
	err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default())
	if err == nil {
		t.Fatal("expected error for empty file")
	}
}

func TestProvisionInitialAdmin_RejectsMalformedFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "garbage")
	if err := os.WriteFile(path, []byte("no-colon-no-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	store := newTestStore(t)
	cfg := RunConfig{InitialAdminFile: path}
	err := provisionInitialAdmin(context.Background(), store, cfg, slog.Default())
	if err == nil {
		t.Fatal("expected error for malformed file")
	}
}
