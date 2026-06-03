package segmentation

import (
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
)

const (
	workingDirName   = "working"
	completedDirName = "completed"
)

func workingDir(root string) string {
	return filepath.Join(root, workingDirName)
}

func completedDir(root string) string {
	return filepath.Join(root, completedDirName)
}

func workingPath(root string, segmentID glid.GLID) string {
	return filepath.Join(workingDir(root), segmentID.String())
}

func completedPath(root string, segmentID glid.GLID) string {
	return filepath.Join(completedDir(root), segmentID.String())
}

func ensureVaultDirs(root string) error {
	if err := os.MkdirAll(workingDir(root), 0o750); err != nil {
		return err
	}
	return os.MkdirAll(completedDir(root), 0o750)
}
