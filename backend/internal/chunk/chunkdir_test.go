package chunk_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"gastrolog/internal/chunk"
)

// A write that outlives its vault's deletion must not recreate the vault's
// chunk-manager directory; while the directory exists, the chunk directory
// is created and creating it again is harmless.
func TestMkdirChunkDirNeverCreatesVaultDir(t *testing.T) {
	t.Parallel()
	vaultDir := filepath.Join(t.TempDir(), "vault")
	id := chunk.NewChunkID()

	if _, err := chunk.MkdirChunkDir(vaultDir, id); err == nil {
		t.Fatalf("created a chunk directory under a missing vault directory")
	}
	if _, err := os.Stat(vaultDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("vault directory recreated: stat err=%v", err)
	}

	if err := os.Mkdir(vaultDir, 0o750); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		path, err := chunk.MkdirChunkDir(vaultDir, id)
		if err != nil {
			t.Fatalf("MkdirChunkDir under an existing vault directory: %v", err)
		}
		if info, err := os.Stat(path); err != nil || !info.IsDir() || path != filepath.Join(vaultDir, id.String()) {
			t.Fatalf("chunk directory %s not created: %v", path, err)
		}
	}
}
