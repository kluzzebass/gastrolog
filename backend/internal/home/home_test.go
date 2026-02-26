package home

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNew(t *testing.T) {
	d := New("/tmp/gastrolog-test")
	if d.Root() != "/tmp/gastrolog-test" {
		t.Errorf("expected root /tmp/gastrolog-test, got %s", d.Root())
	}
}

func TestDefault(t *testing.T) {
	d, err := Default()
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	if d.Root() == "" {
		t.Fatal("expected non-empty root")
	}
	// Should end with "gastrolog".
	if filepath.Base(d.Root()) != "gastrolog" {
		t.Errorf("expected root to end with 'gastrolog', got %s", d.Root())
	}
}

func TestUsersPath(t *testing.T) {
	d := New("/data")
	if got := d.UsersPath(); got != "/data/users.json" {
		t.Errorf("got %s", got)
	}
}

func TestVaultDir(t *testing.T) {
	d := New("/data")
	if got := d.VaultDir("default"); got != "/data/stores/default" {
		t.Errorf("got %s", got)
	}
	if got := d.VaultDir("prod"); got != "/data/stores/prod" {
		t.Errorf("got %s", got)
	}
}

func TestEnsureExists(t *testing.T) {
	root := filepath.Join(t.TempDir(), "nested", "gastrolog")
	d := New(root)
	if err := d.EnsureExists(); err != nil {
		t.Fatalf("EnsureExists: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if !info.IsDir() {
		t.Error("expected directory")
	}

	// Calling again should be idempotent.
	if err := d.EnsureExists(); err != nil {
		t.Fatalf("EnsureExists (idempotent): %v", err)
	}
}
