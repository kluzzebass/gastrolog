package collection

import (
	"errors"
	"io"
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/pipeline/paths"
)

var (
	// ErrCorruptSegment is returned when a pre-head file fails verification.
	ErrCorruptSegment = errors.New("segment checksum verification failed")
)

// ReceiveToPreHead writes pulled segment bytes into the vault pre-head area.
func ReceiveToPreHead(vaultRoot string, segmentID glid.GLID, src io.Reader) (string, error) {
	if err := paths.EnsurePreHeadDir(vaultRoot); err != nil {
		return "", err
	}
	path := paths.PreHeadSegment(vaultRoot, segmentID)
	f, err := os.OpenFile(filepath.Clean(path), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	_, copyErr := io.Copy(f, src)
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return "", copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return "", closeErr
	}
	return path, nil
}

// PromoteVerified opens the pre-head segment, verifies its checksum, and atomically
// renames it into head. A corrupt transfer is discarded from pre-head.
func PromoteVerified(preHeadPath, vaultRoot string) (string, error) {
	sf, err := segment.Open(preHeadPath)
	if err != nil {
		_ = os.Remove(preHeadPath)
		return "", errors.Join(ErrCorruptSegment, err)
	}
	_ = sf.Close()

	if err := paths.EnsureHeadDir(vaultRoot); err != nil {
		_ = os.Remove(preHeadPath)
		return "", err
	}
	dest := filepath.Join(paths.HeadDir(vaultRoot), filepath.Base(preHeadPath))
	if err := os.Rename(filepath.Clean(preHeadPath), dest); err != nil {
		_ = os.Remove(preHeadPath)
		return "", err
	}
	return dest, nil
}
