package distribution

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"gastrolog/internal/pipeline/paths"
)

// linkHead is os.Link, indirected so tests can force the link-error →
// copy-fallback dispatch without a link-refusing filesystem.
var linkHead = os.Link

// headPromoteSuffix marks an in-flight byte copy into head/ on the
// link-unsupported fallback path, mirroring collection's preHeadPullSuffix
// under pre-head/: a head/ name only ever appears complete, because the copy
// lands under the suffixed temp name and is renamed in after fsync.
const headPromoteSuffix = ".promote"

// PromoteToHead gives a completed segment a second name under head/ for local
// chunking. The completed/ name must remain so peer collectors can pull until
// ReleaseSegments purges it — a rename would destroy the only on-disk copy
// peers can reach.
//
// Both names live under the vault staging root (same filesystem) and segment
// files are immutable after finalize, so the head/ name is a hard link: O(1)
// I/O instead of rewriting every record's bytes. Purge semantics are
// unchanged — purge removes individual names, and unlinking one leaves the
// other's bytes intact. Filesystems that refuse hard links (some network/FAT
// mounts) fall back to the byte copy in copyToHead.
func PromoteToHead(completedPath, vaultRoot string) (string, error) {
	if err := paths.EnsureHeadDir(vaultRoot); err != nil {
		return "", err
	}
	dest := filepath.Join(paths.HeadDir(vaultRoot), filepath.Base(completedPath))
	switch err := linkHead(filepath.Clean(completedPath), dest); {
	case err == nil:
		// Durability barrier: head/ entries feed GLCB builds whose seal
		// commits to vault-ctl (gastrolog-4mqy06). The segment bytes
		// were already synced when the working segment was finalized;
		// a link adds only a directory entry, so syncing head/ is the
		// whole barrier.
		if err := paths.SyncDir(paths.HeadDir(vaultRoot)); err != nil {
			return "", err
		}
		return dest, nil
	case errors.Is(err, fs.ErrExist):
		// Already promoted (rescan or restart replay). Segments are
		// immutable and head/ names are only ever installed complete
		// (link, or temp+rename on the fallback path), so the existing
		// bytes are the segment's bytes — the same end state the old
		// copy path reached by overwriting them with identical content.
		return dest, nil
	case linkUnsupported(err):
		return copyToHead(completedPath, dest, vaultRoot)
	default:
		return "", fmt.Errorf("link head name: %w", err)
	}
}

// linkUnsupported reports whether a link failure means the filesystem refuses
// hard links (so a byte copy should be attempted) rather than a hard error.
func linkUnsupported(err error) bool {
	return errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EXDEV) ||
		errors.Is(err, syscall.ENOTSUP) ||
		errors.Is(err, syscall.EOPNOTSUPP)
}

// copyToHead byte-copies a completed segment into head/ — the fallback for
// filesystems that refuse hard links. The temp+rename dance keeps head/
// entries atomic: a name never appears until its bytes are complete and
// synced.
func copyToHead(completedPath, dest, vaultRoot string) (string, error) {
	src, err := os.Open(filepath.Clean(completedPath))
	if err != nil {
		return "", err
	}
	defer func() { _ = src.Close() }()
	tmp := dest + headPromoteSuffix
	out, err := os.OpenFile(filepath.Clean(tmp), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(out, src); err != nil {
		_ = out.Close()
		_ = os.Remove(tmp)
		return "", err
	}
	// Durability barrier: head/ copies feed GLCB builds whose seal commits
	// to vault-ctl; the bytes must survive a crash once referenced
	// (gastrolog-4mqy06).
	if err := out.Sync(); err != nil {
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
	if err := paths.SyncDir(paths.HeadDir(vaultRoot)); err != nil {
		return "", err
	}
	return dest, nil
}
