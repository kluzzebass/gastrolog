package collection

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
)

var (
	// ErrCorruptSegment is returned when a pre-head file fails verification.
	ErrCorruptSegment = errors.New("segment checksum verification failed")
)

// preHeadPullSuffix marks an in-flight collect temp file under pre-head/.
// The final segment filename must not exist until the pull completes: the
// production PullClient reads local head/completed/pre-head before RPC and
// would otherwise copy the empty truncated pre-head file to itself.
const preHeadPullSuffix = ".pulling"

// PullToPreHead streams a segment from pull directly into pre-head without
// holding the full payload in an intermediate memory buffer.
func PullToPreHead(ctx context.Context, vaultRoot string, vaultID, segmentID glid.GLID, pull PullClient) (string, error) {
	if err := paths.EnsurePreHeadDir(vaultRoot); err != nil {
		return "", err
	}
	finalPath := paths.PreHeadSegment(vaultRoot, segmentID)
	tmpPath := finalPath + preHeadPullSuffix
	f, err := os.OpenFile(filepath.Clean(tmpPath), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	pullErr := pull.Pull(ctx, vaultID, segmentID, f)
	if pullErr == nil {
		// Durability barrier: the holder receipt this pull leads to asserts
		// cluster-wide that a copy exists. Fsync before the receipt can
		// commit, or a crash leaves the cluster trusting a torn copy —
		// potentially the last one (gastrolog-4mqy06).
		pullErr = f.Sync()
	}
	closeErr := f.Close()
	if pullErr != nil {
		_ = os.Remove(tmpPath)
		return "", pullErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return "", closeErr
	}
	if err := os.Rename(filepath.Clean(tmpPath), filepath.Clean(finalPath)); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	if err := paths.SyncDir(paths.PreHeadDir(vaultRoot)); err != nil {
		return "", err
	}
	return finalPath, nil
}

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
	if copyErr == nil {
		copyErr = f.Sync()
	}
	closeErr := f.Close()
	if copyErr != nil {
		_ = os.Remove(path)
		return "", copyErr
	}
	if closeErr != nil {
		_ = os.Remove(path)
		return "", closeErr
	}
	if err := paths.SyncDir(paths.PreHeadDir(vaultRoot)); err != nil {
		return "", err
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
	if err := paths.SyncDir(paths.HeadDir(vaultRoot)); err != nil {
		return "", err
	}
	return dest, nil
}
