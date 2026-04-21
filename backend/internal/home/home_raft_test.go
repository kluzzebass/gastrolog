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
