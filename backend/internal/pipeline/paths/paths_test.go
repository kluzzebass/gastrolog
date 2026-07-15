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
	if got, want := paths.WorkingSegment(root, segID), filepath.Join(root, paths.Working, segID.String()); got != want {
		t.Fatalf("WorkingSegment = %q, want %q", got, want)
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

func TestEnsureHeadAndPreHeadDirs(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := paths.EnsurePreHeadDir(root); err != nil {
		t.Fatal(err)
	}
	if err := paths.EnsureHeadDir(root); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []string{paths.PreHead, paths.Head} {
		if _, err := os.Stat(filepath.Join(root, sub)); err != nil {
			t.Fatalf("%s: %v", sub, err)
		}
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

func writeSegmentFile(t *testing.T, root string, area paths.Area, segID glid.GLID) string {
	t.Helper()
	path := area.Segment(root, segID)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestFindSegmentEachArea(t *testing.T) {
	t.Parallel()
	for _, area := range []paths.Area{
		paths.AreaWorking, paths.AreaCompleted, paths.AreaPreHead, paths.AreaHead,
	} {
		t.Run(string(area), func(t *testing.T) {
			t.Parallel()
			root := t.TempDir()
			segID := glid.New()
			want := writeSegmentFile(t, root, area, segID)

			got, ok := paths.FindSegment(root, segID,
				paths.AreaHead, paths.AreaCompleted, paths.AreaPreHead, paths.AreaWorking)
			if !ok || got != want {
				t.Fatalf("FindSegment() = (%q, %v), want (%q, true)", got, ok, want)
			}
		})
	}
}

func TestFindSegmentPrecedence(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()
	headPath := writeSegmentFile(t, root, paths.AreaHead, segID)
	completedPath := writeSegmentFile(t, root, paths.AreaCompleted, segID)

	// Area order is search preference: first listed area wins.
	if got, ok := paths.FindSegment(root, segID, paths.AreaHead, paths.AreaCompleted); !ok || got != headPath {
		t.Fatalf("FindSegment(head, completed) = (%q, %v), want (%q, true)", got, ok, headPath)
	}
	if got, ok := paths.FindSegment(root, segID, paths.AreaCompleted, paths.AreaHead); !ok || got != completedPath {
		t.Fatalf("FindSegment(completed, head) = (%q, %v), want (%q, true)", got, ok, completedPath)
	}
}

func TestFindSegmentNotFound(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	segID := glid.New()

	if path, ok := paths.FindSegment(root, segID,
		paths.AreaHead, paths.AreaCompleted, paths.AreaPreHead); ok {
		t.Fatalf("FindSegment() = (%q, true), want not found", path)
	}

	// An unlisted area must not be probed: bytes in working/ are invisible to
	// a head/completed probe.
	writeSegmentFile(t, root, paths.AreaWorking, segID)
	if path, ok := paths.FindSegment(root, segID, paths.AreaHead, paths.AreaCompleted); ok {
		t.Fatalf("FindSegment() = (%q, true), want not found for unlisted area", path)
	}

	// No areas: never found.
	if path, ok := paths.FindSegment(root, segID); ok {
		t.Fatalf("FindSegment() with no areas = (%q, true), want not found", path)
	}
}

func TestSyncDir(t *testing.T) {
	dir := t.TempDir()
	if err := paths.SyncDir(dir); err != nil {
		t.Fatalf("SyncDir on real dir: %v", err)
	}
	if err := paths.SyncDir(filepath.Join(dir, "missing")); err == nil {
		t.Fatal("SyncDir on missing dir should error")
	}
}
