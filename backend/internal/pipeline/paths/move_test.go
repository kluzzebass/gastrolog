package paths

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func writeTreeFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readTreeFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// crossDevice refuses every rename between the two trees the way rename(2)
// does across filesystems, while renames inside one tree (the temp-name
// install) still work.
func crossDevice(src string) func(string, string) error {
	return func(oldpath, newpath string) error {
		if strings.HasPrefix(oldpath, src+string(filepath.Separator)) {
			return &os.LinkError{Op: "rename", Old: oldpath, New: newpath, Err: syscall.EXDEV}
		}
		return os.Rename(oldpath, newpath)
	}
}

func TestMoveTreeMovesEveryFile(t *testing.T) {
	t.Parallel()
	for name, rename := range map[string]func(string) func(string, string) error{
		"same filesystem":  func(string) func(string, string) error { return os.Rename },
		"cross filesystem": crossDevice,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			src := filepath.Join(t.TempDir(), "old")
			dst := filepath.Join(t.TempDir(), "new")
			writeTreeFile(t, filepath.Join(src, Completed, "seg-a"), "a")
			writeTreeFile(t, filepath.Join(src, Head, "seg-b"), "b")
			writeTreeFile(t, filepath.Join(src, "chunks", "c1", "data.glcb"), "glcb")
			if err := moveTree(src, dst, rename(src)); err != nil {
				t.Fatalf("moveTree: %v", err)
			}
			for rel, want := range map[string]string{
				filepath.Join(Completed, "seg-a"):          "a",
				filepath.Join(Head, "seg-b"):               "b",
				filepath.Join("chunks", "c1", "data.glcb"): "glcb",
			} {
				if got := readTreeFile(t, filepath.Join(dst, rel)); got != want {
					t.Errorf("%s = %q, want %q", rel, got, want)
				}
			}
			if _, err := os.Stat(src); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("source tree still present after move: %v", err)
			}
			if entries, _ := os.ReadDir(filepath.Join(dst, "chunks", "c1")); len(entries) != 1 {
				t.Errorf("chunk dir holds %d entries, want only data.glcb (no temp copy left)", len(entries))
			}
		})
	}
}

// A file already present at its destination is a finished move: the source
// copy is dropped and the destination is not overwritten.
func TestMoveTreeKeepsExistingDestination(t *testing.T) {
	t.Parallel()
	src := filepath.Join(t.TempDir(), "old")
	dst := filepath.Join(t.TempDir(), "new")
	writeTreeFile(t, filepath.Join(src, Completed, "seg"), "source")
	writeTreeFile(t, filepath.Join(dst, Completed, "seg"), "installed")
	if err := MoveTree(src, dst); err != nil {
		t.Fatalf("MoveTree: %v", err)
	}
	if got := readTreeFile(t, filepath.Join(dst, Completed, "seg")); got != "installed" {
		t.Fatalf("destination = %q, want the installed copy", got)
	}
	if _, err := os.Stat(filepath.Join(src, Completed, "seg")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("source copy of a finished move survives: %v", err)
	}
}

// An unfinished cross-filesystem copy in the source tree carries nothing its
// own source does not, so it is discarded rather than propagated.
func TestMoveTreeDropsUnfinishedCopies(t *testing.T) {
	t.Parallel()
	src := filepath.Join(t.TempDir(), "old")
	dst := filepath.Join(t.TempDir(), "new")
	writeTreeFile(t, filepath.Join(src, Completed, "seg"+moveTempSuffix), "partial")
	if err := MoveTree(src, dst); err != nil {
		t.Fatalf("MoveTree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, Completed, "seg"+moveTempSuffix)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unfinished copy propagated: %v", err)
	}
}

func TestMoveTreeMissingSourceIsEmpty(t *testing.T) {
	t.Parallel()
	dst := filepath.Join(t.TempDir(), "new")
	if err := MoveTree(filepath.Join(t.TempDir(), "absent"), dst); err != nil {
		t.Fatalf("MoveTree of a missing tree: %v", err)
	}
}

// A file that cannot be moved stays in the source tree, with its directory,
// and the error names it; nothing is lost.
func TestMoveTreeFailureKeepsSource(t *testing.T) {
	t.Parallel()
	src := filepath.Join(t.TempDir(), "old")
	dst := filepath.Join(t.TempDir(), "new")
	writeTreeFile(t, filepath.Join(src, Completed, "seg"), "bytes")
	failing := func(string, string) error { return syscall.EIO }
	if err := moveTree(src, dst, failing); err == nil {
		t.Fatal("moveTree succeeded with a failing rename")
	}
	if got := readTreeFile(t, filepath.Join(src, Completed, "seg")); got != "bytes" {
		t.Fatalf("source = %q after failed move", got)
	}
}
