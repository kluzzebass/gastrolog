package app

// A reinit renames the raft directory aside before rebuilding it. Each
// backup holds a full copy of this node's raft log and stable state,
// replicated config and its secrets included, so they cannot be left to
// accumulate one per rejoin.

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func TestPruneRaftBackupsKeepsOnlyTheNewest(t *testing.T) {
	t.Parallel()
	home := t.TempDir()
	raftDir := filepath.Join(home, "raft")

	// Older first; the newest is the one a rollback would restore.
	stamps := []string{"1700000000000", "1700000001000", "1700000002000"}
	for _, stamp := range stamps {
		dir := raftDir + ".bak." + stamp
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "raft.db"), []byte("state"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	pruneRaftBackups(raftDir, slog.New(slog.DiscardHandler))

	left, err := filepath.Glob(raftDir + ".bak.*")
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(left)
	if len(left) != 1 {
		t.Fatalf("%d backups left, want 1: %v", len(left), left)
	}
	if want := raftDir + ".bak." + stamps[len(stamps)-1]; left[0] != want {
		t.Errorf("kept %s, want the newest %s", left[0], want)
	}
}

// A single backup is the rollback candidate and must survive.
func TestPruneRaftBackupsKeepsALoneBackup(t *testing.T) {
	t.Parallel()
	raftDir := filepath.Join(t.TempDir(), "raft")
	only := raftDir + ".bak.1700000000000"
	if err := os.MkdirAll(only, 0o750); err != nil {
		t.Fatal(err)
	}

	pruneRaftBackups(raftDir, slog.New(slog.DiscardHandler))

	if _, err := os.Stat(only); err != nil {
		t.Fatalf("the only backup was removed: %v", err)
	}
}

// Neighbouring directories are not backups of this one and are left alone.
func TestPruneRaftBackupsLeavesUnrelatedDirectories(t *testing.T) {
	t.Parallel()
	home := t.TempDir()
	raftDir := filepath.Join(home, "raft")

	keep := []string{
		filepath.Join(home, "vaults"),
		filepath.Join(home, "raft-other.bak.1700000000000"),
		raftDir,
	}
	for _, dir := range keep {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	for _, stamp := range []string{"1700000000000", "1700000001000"} {
		if err := os.MkdirAll(raftDir+".bak."+stamp, 0o750); err != nil {
			t.Fatal(err)
		}
	}

	pruneRaftBackups(raftDir, slog.New(slog.DiscardHandler))

	for _, dir := range keep {
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("%s was removed: %v", dir, err)
		}
	}
}
