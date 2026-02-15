package file

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMoveDirSameFilesystem(t *testing.T) {
	root := t.TempDir()

	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")

	// Create source directory with files.
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "data.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := MoveDir(src, dst); err != nil {
		t.Fatalf("MoveDir: %v", err)
	}

	// Source should be gone.
	if _, err := os.Stat(src); !os.IsNotExist(err) {
		t.Errorf("source should not exist after move, got err=%v", err)
	}

	// Destination should have the file.
	data, err := os.ReadFile(filepath.Join(dst, "data.txt"))
	if err != nil {
		t.Fatalf("read moved file: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("expected %q, got %q", "hello", string(data))
	}
}

func TestMoveDirNonexistentSource(t *testing.T) {
	root := t.TempDir()

	src := filepath.Join(root, "nonexistent")
	dst := filepath.Join(root, "dst")

	err := MoveDir(src, dst)
	if err == nil {
		t.Fatal("expected error for nonexistent source")
	}
}

func TestMoveDirNestedContent(t *testing.T) {
	root := t.TempDir()

	src := filepath.Join(root, "src")
	dst := filepath.Join(root, "dst")

	// Create nested structure.
	subdir := filepath.Join(src, "sub")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "top.txt"), []byte("top"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(subdir, "nested.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}

	if err := MoveDir(src, dst); err != nil {
		t.Fatalf("MoveDir: %v", err)
	}

	// Verify nested content.
	data, err := os.ReadFile(filepath.Join(dst, "sub", "nested.txt"))
	if err != nil {
		t.Fatalf("read nested file: %v", err)
	}
	if string(data) != "nested" {
		t.Errorf("expected %q, got %q", "nested", string(data))
	}

	topData, err := os.ReadFile(filepath.Join(dst, "top.txt"))
	if err != nil {
		t.Fatalf("read top file: %v", err)
	}
	if string(topData) != "top" {
		t.Errorf("expected %q, got %q", "top", string(topData))
	}
}
