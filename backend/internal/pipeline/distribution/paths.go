package distribution

import (
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
)

const headDirName = "head"

func headDir(root string) string {
	return filepath.Join(root, headDirName)
}

func headPath(root string, segmentID glid.GLID) string {
	return filepath.Join(headDir(root), segmentID.String())
}

func ensureHeadDir(root string) error {
	return os.MkdirAll(headDir(root), 0o750)
}
