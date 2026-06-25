package chunking

import (
	"errors"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
)

// OpenGLCBCursor opens a seekable record cursor over a local pipeline GLCB.
// The caller owns closing the returned cursor.
func OpenGLCBCursor(glcbPath string, chunkID chunk.ChunkID) (chunk.RecordCursor, error) {
	if glcbPath == "" {
		return nil, errors.New("GLCB path required")
	}
	f, err := os.Open(filepath.Clean(glcbPath))
	if err != nil {
		return nil, err
	}
	rd, err := chunkcloud.NewCacheReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return chunkcloud.NewSeekableCursorWithClose(rd, chunkID, func() {
		_ = rd.Close()
	}), nil
}
