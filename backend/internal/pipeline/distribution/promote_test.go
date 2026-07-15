package distribution_test

import (
	"bytes"
	"os"
	"syscall"
	"testing"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/distribution"
	"gastrolog/internal/pipeline/paths"
)

// statInode returns the inode number and link count for a path.
func statInode(t *testing.T, path string) (ino uint64, nlink uint64) {
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

// TestPromoteToHeadHardLinks asserts promote is O(1) I/O on link-capable
// filesystems: the head/ name is a hard link to the completed file, not a
// byte copy.
func TestPromoteToHeadHardLinks(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	seg := writeCompletedSegment(t, root, glid.New(), "link me")

	dest, err := distribution.PromoteToHead(seg.Path, root)
	if err != nil {
		t.Fatal(err)
	}
	srcIno, srcNlink := statInode(t, seg.Path)
	dstIno, dstNlink := statInode(t, dest)
	if srcIno != dstIno {
		t.Fatalf("head name should share the completed file's inode: src=%d dst=%d", srcIno, dstIno)
	}
	if srcNlink != 2 || dstNlink != 2 {
		t.Fatalf("expected 2 links to the segment inode, got src=%d dst=%d", srcNlink, dstNlink)
	}
}

// TestPromoteToHeadIdempotent replays a promote (rescan/restart) and asserts
// the existing head/ name is reused intact.
func TestPromoteToHeadIdempotent(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	seg := writeCompletedSegment(t, root, glid.New(), "promote twice")

	first, err := distribution.PromoteToHead(seg.Path, root)
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := distribution.PromoteToHead(seg.Path, root)
	if err != nil {
		t.Fatalf("replayed promote: %v", err)
	}
	if second != first {
		t.Fatalf("replayed promote dest = %q, want %q", second, first)
	}
	got, err := os.ReadFile(second)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("head bytes changed across replayed promote")
	}
	if _, nlink := statInode(t, second); nlink != 2 {
		t.Fatalf("expected 2 links after replayed promote, got %d", nlink)
	}
}

// TestPromoteToHeadPurgeOneNameKeepsOther purges each staging name in turn
// and asserts the surviving hard-linked name still reads the full segment
// bytes — the property that makes hard-link promotion safe.
func TestPromoteToHeadPurgeOneNameKeepsOther(t *testing.T) {
	t.Parallel()

	t.Run("purge completed, head survives", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		seg := writeCompletedSegment(t, root, glid.New(), "retire completed")
		want, err := os.ReadFile(seg.Path)
		if err != nil {
			t.Fatal(err)
		}
		dest, err := distribution.PromoteToHead(seg.Path, root)
		if err != nil {
			t.Fatal(err)
		}
		if err := paths.PurgeCompleted(root, seg.SegmentID); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(seg.Path); !os.IsNotExist(err) {
			t.Fatalf("completed name should be gone, stat err = %v", err)
		}
		got, err := os.ReadFile(dest)
		if err != nil {
			t.Fatalf("head bytes after completed purge: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("head bytes corrupted after completed purge: got %d bytes, want %d", len(got), len(want))
		}
	})

	t.Run("purge head, completed survives", func(t *testing.T) {
		t.Parallel()
		root := t.TempDir()
		seg := writeCompletedSegment(t, root, glid.New(), "retire head")
		want, err := os.ReadFile(seg.Path)
		if err != nil {
			t.Fatal(err)
		}
		dest, err := distribution.PromoteToHead(seg.Path, root)
		if err != nil {
			t.Fatal(err)
		}
		if err := paths.PurgeHeadStaging(root, seg.SegmentID); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(dest); !os.IsNotExist(err) {
			t.Fatalf("head name should be gone, stat err = %v", err)
		}
		got, err := os.ReadFile(seg.Path)
		if err != nil {
			t.Fatalf("completed bytes after head purge: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("completed bytes corrupted after head purge: got %d bytes, want %d", len(got), len(want))
		}
	})
}
