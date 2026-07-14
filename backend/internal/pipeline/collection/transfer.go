package collection

import (
	"context"
	"errors"
	"fmt"
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

// PromoteVerified opens the pre-head segment, verifies its checksum, and atomically
// renames it into head. A corrupt transfer is discarded from pre-head. The
// verified header is returned so callers can count arrivals without a
// re-read (gastrolog-10n6k8).
//
// publishedChecksum is the record checksum the origin published to the
// vault-ctl registry (CompletedSegmentEntry.Checksum). segment.Open only
// proves the file is consistent with its OWN header — a holder serving stale
// or wrong-but-internally-valid bytes would still pass, get receipted, and
// merge divergent bytes into this home's GLCB (gastrolog-5zotim). Zero means
// no published expectation is available (no registry entry — tests, targeted
// collects before the FSM caught up); internal verification alone then gates
// the promote.
//
// The record checksum is XXH64 over the record region — content-sensitive,
// including same-length substitution. Its rolling-CRC32 predecessor folded
// each frame's trailing CRC32 into itself, and CRC(M ++ CRC(M)) cancels the
// content contribution, pinning only frame-length structure
// (gastrolog-1vepg0).
func PromoteVerified(preHeadPath, vaultRoot string, publishedChecksum uint64) (string, segment.Header, error) {
	sf, err := segment.Open(preHeadPath)
	if err != nil {
		_ = os.Remove(preHeadPath)
		return "", segment.Header{}, errors.Join(ErrCorruptSegment, err)
	}
	hdr := sf.Header()
	_ = sf.Close()

	if publishedChecksum != 0 && hdr.SegmentChecksum != publishedChecksum {
		_ = os.Remove(preHeadPath)
		return "", segment.Header{}, fmt.Errorf("%w: segment checksum %016x does not match published checksum %016x",
			ErrCorruptSegment, hdr.SegmentChecksum, publishedChecksum)
	}

	if err := paths.EnsureHeadDir(vaultRoot); err != nil {
		_ = os.Remove(preHeadPath)
		return "", segment.Header{}, err
	}
	dest := filepath.Join(paths.HeadDir(vaultRoot), filepath.Base(preHeadPath))
	if err := os.Rename(filepath.Clean(preHeadPath), dest); err != nil {
		_ = os.Remove(preHeadPath)
		return "", segment.Header{}, err
	}
	if err := paths.SyncDir(paths.HeadDir(vaultRoot)); err != nil {
		return "", segment.Header{}, err
	}
	return dest, hdr, nil
}
