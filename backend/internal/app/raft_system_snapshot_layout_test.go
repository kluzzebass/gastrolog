package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMigrateLegacySystemRaftSnapshots_movesTermIndexDirs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "wal"), 0o750); err != nil {
		t.Fatal(err)
	}
	legacySnap := filepath.Join(root, "3-42")
	if err := os.Mkdir(legacySnap, 0o750); err != nil {
		t.Fatal(err)
	}
	systemSnap := filepath.Join(root, "groups", "system")
	if err := migrateLegacySystemRaftSnapshots(root, systemSnap); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := os.Stat(filepath.Join(systemSnap, "3-42")); err != nil {
		t.Fatalf("expected snapshot under groups/system: %v", err)
	}
	if _, err := os.Stat(legacySnap); !os.IsNotExist(err) {
		t.Fatalf("legacy path should be gone: stat err=%v", err)
	}
}

func TestMigrateLegacySystemRaftSnapshots_idempotent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	systemSnap := filepath.Join(root, "groups", "system")
	if err := os.MkdirAll(filepath.Join(systemSnap, "1-1"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := migrateLegacySystemRaftSnapshots(root, systemSnap); err != nil {
		t.Fatalf("migrate: %v", err)
	}
}
