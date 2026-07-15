package collection

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"gastrolog/internal/glid"
	"gastrolog/internal/pipeline/paths"
	"gastrolog/internal/pipeline/segment"
)

var (
	// ErrCorruptSegment is returned when a pre-head file fails verification.
	ErrCorruptSegment = errors.New("segment checksum verification failed")

	// ErrPreHeadPurged is returned when the pre-head file vanishes between
	// the pull's rename-in and the promote — a concurrent purge
	// (paths.PurgeHeadStaging on segment release, via the supervisor's
	// OnReleaseSegments hook or chunking's release/stale purges) deleted it
	// because the registry no longer needs the segment. A catch-up race, not
	// a data-integrity failure: no byte was ever verified and found wrong.
	// Joining ErrCorruptSegment here instead surfaced every such race as a
	// "checksum verification failed" data-integrity WARN (gastrolog-2as548).
	ErrPreHeadPurged = errors.New("pre-head segment purged during collect")
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

// isPreHeadPullingName reports whether name matches PullToPreHead's exact
// tmp-file naming contract (a segment ID suffixed with preHeadPullSuffix).
// Kept as a standalone predicate so a writer-sweeper contract test can
// assert against this package's actual naming contract, not a guessed
// pattern.
func isPreHeadPullingName(name string) bool {
	return strings.HasSuffix(name, preHeadPullSuffix)
}

// sweepOrphanPullingFiles removes pre-head/*.pulling files left behind by a
// pull that crashed between PullToPreHead's O_TRUNC open and its rename
// commit (gastrolog-5do8sh gap 7, gastrolog-66hmx3). Unlike the
// data.glcb.tmp wedge, this leak is hygiene rather than correctness: a
// FINAL-named pre-head orphan blocks nothing (a later collect pass reads
// through it and a real re-pull of the same segment reuses the exact same
// tmp path with O_TRUNC, discarding the stale bytes on rename — see
// TestRegisterVaultSweepsPullingOrphanAndRepullStillOverwrites). The sweep
// only exists so an unassigned segment's crash orphan does not sit in
// pre-head/ forever if that segment is never pulled again.
//
// Must run before any pull can start (i.e. at vault registration, before
// the worker goroutine is started) — the per-segment `pulling` exclusivity
// set only guards against two pulls of the SAME segment racing each other,
// not against this sweep racing a live in-flight pull.
func sweepOrphanPullingFiles(vaultRoot string, logger *slog.Logger) {
	entries, err := os.ReadDir(paths.PreHeadDir(vaultRoot))
	if err != nil {
		return
	}
	for _, entry := range entries {
		if entry.IsDir() || !isPreHeadPullingName(entry.Name()) {
			continue
		}
		path := filepath.Join(paths.PreHeadDir(vaultRoot), entry.Name())
		if err := os.Remove(path); err != nil {
			logger.Warn("failed to remove orphan pre-head pulling temp file", "path", path, "error", err)
		} else {
			logger.Info("removed orphan pre-head pulling temp file", "path", path)
		}
	}
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
		if errors.Is(err, fs.ErrNotExist) {
			// A concurrent release purge won the race for this file. Only a
			// verification failure may carry ErrCorruptSegment; a missing
			// file verified nothing (gastrolog-2as548).
			return "", segment.Header{}, fmt.Errorf("%w: %w", ErrPreHeadPurged, err)
		}
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
		if errors.Is(err, fs.ErrNotExist) {
			// Verified fine, then a concurrent release purge removed the
			// source before the rename (head/ was just ensured, so ENOENT
			// means the pre-head name is gone). Same race as the open-time
			// window above (gastrolog-2as548).
			return "", segment.Header{}, fmt.Errorf("%w: %w", ErrPreHeadPurged, err)
		}
		_ = os.Remove(preHeadPath)
		return "", segment.Header{}, err
	}
	if err := paths.SyncDir(paths.HeadDir(vaultRoot)); err != nil {
		return "", segment.Header{}, err
	}
	return dest, hdr, nil
}
