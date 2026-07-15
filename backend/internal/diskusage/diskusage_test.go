package diskusage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirBytesSumsFiles(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "a"), []byte("12345"), 0o644); err != nil {
		t.Fatal(err)
	}
	sub := filepath.Join(root, "nested")
	if err := os.MkdirAll(sub, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "b"), []byte("xy"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := DirBytes(root)
	if got != 7 {
		t.Fatalf("DirBytes() = %d, want 7", got)
	}
}

func TestDirBytesMissingRoot(t *testing.T) {
	t.Parallel()
	if got := DirBytes(filepath.Join(t.TempDir(), "missing")); got != 0 {
		t.Fatalf("DirBytes(missing) = %d, want 0", got)
	}
}
