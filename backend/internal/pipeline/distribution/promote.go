package distribution

import (
	"os"
	"path/filepath"

	"gastrolog/internal/pipeline/paths"
)

// PromoteToHead atomically moves a completed segment file into the vault head.
func PromoteToHead(completedPath, vaultRoot string) (string, error) {
	if err := paths.EnsureHeadDir(vaultRoot); err != nil {
		return "", err
	}
	dest := filepath.Join(paths.HeadDir(vaultRoot), filepath.Base(completedPath))
	if err := os.Rename(filepath.Clean(completedPath), dest); err != nil {
		return "", err
	}
	return dest, nil
}
