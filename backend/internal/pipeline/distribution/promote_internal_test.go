package distribution

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"gastrolog/internal/pipeline/paths"
)

func writeSourceFile(t *testing.T, root, name string, content []byte) string {
	t.Helper()
	if err := paths.EnsureSegmentationDirs(root); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(paths.CompletedDir(root), name)
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func inode(t *testing.T, path string) (ino uint64, nlink uint64) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("stat %s: no unix Stat_t", path)
	}
	return st.Ino, uint64(st.Nlink)
}

// TestCopyToHeadFallback exercises the byte-copy fallback directly: the
// promoted file must hold the source bytes on its own inode, and the .promote
// temp name must not linger.
func TestCopyToHeadFallback(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	want := []byte("copied, not linked")
	src := writeSourceFile(t, root, "seg-copy", want)
	if err := paths.EnsureHeadDir(root); err != nil {
		t.Fatal(err)
	}
	dest := filepath.Join(paths.HeadDir(root), "seg-copy")

	got, err := copyToHead(src, dest, root)
	if err != nil {
		t.Fatal(err)
	}
	if got != dest {
		t.Fatalf("copyToHead dest = %q, want %q", got, dest)
	}
	data, err := os.ReadFile(dest)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, want) {
		t.Fatalf("copied bytes = %q, want %q", data, want)
	}
	srcIno, _ := inode(t, src)
	dstIno, dstNlink := inode(t, dest)
	if srcIno == dstIno {
		t.Fatal("fallback should copy, not link")
	}
	if dstNlink != 1 {
		t.Fatalf("copied file nlink = %d, want 1", dstNlink)
	}
	if _, err := os.Stat(dest + ".promote"); !os.IsNotExist(err) {
		t.Fatalf(".promote temp should be gone, stat err = %v", err)
	}
}

// TestCopyToHeadMissingSource asserts the fallback surfaces open errors.
func TestCopyToHeadMissingSource(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := paths.EnsureHeadDir(root); err != nil {
		t.Fatal(err)
	}
	_, err := copyToHead(filepath.Join(root, "missing"), filepath.Join(paths.HeadDir(root), "missing"), root)
	if err == nil {
		t.Fatal("expected error for missing source")
	}
}

// TestPromoteToHeadLinkRefusedFallsBack forces link failures a link-refusing
// filesystem would return (EPERM/EXDEV/ENOTSUP) and asserts PromoteToHead
// dispatches to the copy fallback. Not parallel: it swaps the linkHead hook.
func TestPromoteToHeadLinkRefusedFallsBack(t *testing.T) {
	orig := linkHead
	t.Cleanup(func() { linkHead = orig })

	for _, errno := range []syscall.Errno{syscall.EPERM, syscall.EXDEV, syscall.ENOTSUP} {
		linkHead = func(oldname, newname string) error {
			return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: errno}
		}
		root := t.TempDir()
		want := []byte("fallback for " + errno.Error())
		src := writeSourceFile(t, root, "seg-fallback", want)

		dest, err := PromoteToHead(src, root)
		if err != nil {
			t.Fatalf("%v: promote should fall back to copy: %v", errno, err)
		}
		data, err := os.ReadFile(dest)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(data, want) {
			t.Fatalf("%v: fallback bytes = %q, want %q", errno, data, want)
		}
		srcIno, _ := inode(t, src)
		dstIno, _ := inode(t, dest)
		if srcIno == dstIno {
			t.Fatalf("%v: fallback should copy, not link", errno)
		}
	}
}

// TestPromoteToHeadLinkHardErrorSurfaces asserts link failures outside the
// refused-by-filesystem set are returned, not papered over with a copy.
func TestPromoteToHeadLinkHardErrorSurfaces(t *testing.T) {
	orig := linkHead
	t.Cleanup(func() { linkHead = orig })
	linkHead = func(oldname, newname string) error {
		return &os.LinkError{Op: "link", Old: oldname, New: newname, Err: syscall.EIO}
	}

	root := t.TempDir()
	src := writeSourceFile(t, root, "seg-eio", []byte("io error"))
	if _, err := PromoteToHead(src, root); !errors.Is(err, syscall.EIO) {
		t.Fatalf("expected EIO to surface, got %v", err)
	}
}

func TestLinkUnsupported(t *testing.T) {
	t.Parallel()
	unsupported := []error{syscall.EPERM, syscall.EXDEV, syscall.ENOTSUP, syscall.EOPNOTSUPP}
	for _, errno := range unsupported {
		if !linkUnsupported(&os.LinkError{Op: "link", Err: errno}) {
			t.Errorf("linkUnsupported(%v) = false, want true", errno)
		}
	}
	hard := []error{syscall.EIO, syscall.ENOENT, syscall.EEXIST, nil}
	for _, err := range hard {
		if linkUnsupported(&os.LinkError{Op: "link", Err: err}) {
			t.Errorf("linkUnsupported(%v) = true, want false", err)
		}
	}
}
