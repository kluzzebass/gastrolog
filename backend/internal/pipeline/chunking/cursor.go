package chunking

import (
	"errors"
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
	blob, err := chunkcloud.OpenMappedBlob(filepath.Clean(glcbPath))
	if err != nil {
		return nil, err
	}
	rd, err := blob.Reader()
	if err != nil {
		_ = blob.Close()
		return nil, err
	}
	blob.Retain()
	return chunkcloud.NewSeekableCursorWithClose(rd, chunkID, func() {
		_ = rd.Close()
		blob.Release()
		_ = blob.Close()
	}), nil
}
