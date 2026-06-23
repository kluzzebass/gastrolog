package distribution

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gastrolog/internal/pipeline/paths"
)

// PromoteToHead copies a completed segment into head/ for local chunking.
// The completed/ file remains so peer collectors can pull until ReleaseSegments
// purges it — a rename would destroy the only on-disk copy peers can reach.
func PromoteToHead(completedPath, vaultRoot string) (string, error) {
	if err := paths.EnsureHeadDir(vaultRoot); err != nil {
		return "", err
	}
	dest := filepath.Join(paths.HeadDir(vaultRoot), filepath.Base(completedPath))
	src, err := os.Open(filepath.Clean(completedPath))
	if err != nil {
		return "", err
	}
	defer func() { _ = src.Close() }()
	tmp := dest + ".promote"
	out, err := os.OpenFile(filepath.Clean(tmp), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, src); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	if err := out.Close(); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	if err := os.Rename(tmp, dest); err != nil {
		_ = os.Remove(tmp)
		return "", fmt.Errorf("install head copy: %w", err)
	}
	return dest, nil
}
