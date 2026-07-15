package home_test

import (
	"path/filepath"
	"testing"

	"gastrolog/internal/home"
)

func TestRaftGroupDir(t *testing.T) {
	t.Parallel()
	h := home.New("/tmp/gl-home-test")
	got := h.RaftGroupDir("system")
	want := filepath.Join("/tmp/gl-home-test", "raft", "groups", "system")
	if got != want {
		t.Fatalf("RaftGroupDir(system) = %q, want %q", got, want)
	}
}

func TestRaftWALDirs(t *testing.T) {
	t.Parallel()
	h := home.New("/tmp/gl-home-test")
	if got, want := h.ClusterCtlWALDir(), filepath.Join("/tmp/gl-home-test", "raft", "wal"); got != want {
		t.Fatalf("ClusterCtlWALDir() = %q, want %q", got, want)
	}
	if got, want := h.VaultCtlWALDir(), filepath.Join("/tmp/gl-home-test", "raft", "groups", "wal"); got != want {
		t.Fatalf("VaultCtlWALDir() = %q, want %q", got, want)
	}
}
