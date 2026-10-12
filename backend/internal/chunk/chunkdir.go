package chunk

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

// MkdirChunkDir creates the chunk's directory <dir>/<id> and returns its path,
// never creating dir itself: dir is a vault's chunk-manager directory, which
// exists while the vault does, so a write still in flight when the vault is
// deleted fails instead of recreating the removed directory.
func MkdirChunkDir(dir string, id ChunkID) (string, error) {
	path := filepath.Join(dir, id.String())
	if err := os.Mkdir(path, 0o750); err != nil && !errors.Is(err, fs.ErrExist) {
		return "", err
	}
	return path, nil
}
