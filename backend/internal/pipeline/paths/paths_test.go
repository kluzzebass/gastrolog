package paths_test

import (
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
)

func TestDirsAndSegments(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()

	if got, want := paths.WorkingDir(root), filepath.Join(root, paths.Working); got != want {
		t.Fatalf("WorkingDir = %q, want %q", got, want)
	}
	if got, want := paths.CompletedSegment(root, segID), filepath.Join(root, paths.Completed, segID.String()); got != want {
		t.Fatalf("CompletedSegment = %q, want %q", got, want)
	}
	if got, want := paths.PreHeadSegment(root, segID), filepath.Join(root, paths.PreHead, segID.String()); got != want {
		t.Fatalf("PreHeadSegment = %q, want %q", got, want)
	}
	if got, want := paths.HeadSegment(root, segID), filepath.Join(root, paths.Head, segID.String()); got != want {
		t.Fatalf("HeadSegment = %q, want %q", got, want)
	}
}

func TestEnsureSegmentationDirs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{paths.Working, paths.Completed} {
		if _, err := os.Stat(filepath.Join(root, sub)); err != nil {
			t.Fatalf("%s: %v", sub, err)
		}
	}
}

func TestListSegmentIDs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	dir := paths.HeadDir(root)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	id := glid.New()
	if err := os.WriteFile(paths.HeadSegment(root, id), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "not-a-glid"), []byte("y"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := paths.ListSegmentIDs(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("ListSegmentIDs() = %d entries, want 1", len(got))
	}
	if _, ok := got[id]; !ok {
		t.Fatalf("missing %s in %v", id, got)
	}

	empty, err := paths.ListSegmentIDs(filepath.Join(root, "missing"))
	if err != nil {
		t.Fatal(err)
	}
	if len(empty) != 0 {
		t.Fatalf("missing dir = %v, want empty", empty)
	}
}
