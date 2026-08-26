package glcb

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// DownloadAndUnwrap fetches a transport-framed GLCB object from the blob
// store and reassembles the plain GLCB into the given destination file,
// leaving dst positioned at offset 0. The caller then promotes the plain
// GLCB into place and opens it via OpenMappedBlob like any local blob (see
// downloadCloudBlobToChunkDir in internal/chunk/file/manager.go).
//
// The destination file is the caller's responsibility — typically a temp
// file promoted into the chunk dir on success, removed on failure.
//
// Cloud transport contract: cloud objects carry the per-section frame layout
// documented in transport.go; the framing is added at upload
// (WrapForTransport) and removed here. Reassembly decompresses the frames in
// blob order and verifies each frame's raw bytes against the directory's
// SHA-256, so a corrupt or truncated object fails loudly instead of
// promoting garbage into the chunk dir.
func DownloadAndUnwrap(ctx context.Context, store blobstore.Store, key string, dst *os.File) error {
	rc, err := store.Download(ctx, key)
	if err != nil {
		// Translate blob-store sentinels into chunk-level sentinels so
		// callers (cursor open, query path) can reason about archival
		// state and missing chunks without reaching into blobstore.
		if errors.Is(err, blobstore.ErrBlobArchived) {
			return fmt.Errorf("%w: %s: %w", chunk.ErrChunkArchived, key, err)
		}
		if errors.Is(err, blobstore.ErrBlobNotFound) {
			return fmt.Errorf("%w: %s: %w", chunk.ErrChunkSuspect, key, err)
		}
		return fmt.Errorf("download cloud blob %s: %w", key, err)
	}
	defer func() { _ = rc.Close() }()

	// The directory lives at the object's tail, so the sequential download
	// spills to a temp file first; frames are then read back by directory
	// geometry. The spill is the price of a tail directory — which is what
	// makes the ranged read path (FetchRemoteSection) possible at all.
	spill, err := os.CreateTemp(spillDir(dst), "glcb-transport-*.obj")
	if err != nil {
		return fmt.Errorf("create transport spill: %w", err)
	}
	defer func() {
		_ = spill.Close()
		_ = os.Remove(spill.Name())
	}()
	objSize, err := io.Copy(spill, rc)
	if err != nil {
		return fmt.Errorf("download cloud blob %s: %w", key, err)
	}

	if err := reassembleFromSpill(spill, objSize, dst); err != nil {
		return fmt.Errorf("unwrap cloud blob %s: %w", key, err)
	}
	if _, err := dst.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind GLCB: %w", err)
	}
	return nil
}

// spillDir returns the directory of dst's path, so the spill lands on the
// same filesystem as the destination (the same free-space budget the caller
// already reasoned about).
func spillDir(dst *os.File) string {
	if name := dst.Name(); name != "" {
		return filepath.Dir(name)
	}
	return os.TempDir()
}

// reassembleFromSpill reads the object's directory and decompresses every
// frame in order into dst, verifying each frame's hash.
func reassembleFromSpill(obj *os.File, objSize int64, dst *os.File) error {
	if objSize < transportFooterSize {
		return fmt.Errorf("%w: object is %d bytes", ErrNotTransportObject, objSize)
	}
	var foot [transportFooterSize]byte
	if _, err := obj.ReadAt(foot[:], objSize-transportFooterSize); err != nil {
		return fmt.Errorf("read transport footer: %w", err)
	}
	dirOffset, count, err := parseTransportFooter(foot[:], objSize)
	if err != nil {
		return err
	}
	dirBytes := make([]byte, int64(count)*transportDirEntrySize)
	if _, err := obj.ReadAt(dirBytes, dirOffset); err != nil {
		return fmt.Errorf("read transport directory: %w", err)
	}
	entries, err := decodeTransportDir(dirBytes, count)
	if err != nil {
		return err
	}

	for _, e := range entries {
		frame := make([]byte, e.ObjSize)
		if _, err := obj.ReadAt(frame, e.ObjOffset); err != nil {
			return fmt.Errorf("read frame at %d: %w", e.ObjOffset, err)
		}
		raw, err := decompressFrame(frame, e.RawSize)
		if err != nil {
			return fmt.Errorf("frame at %d: %w", e.ObjOffset, err)
		}
		if sum := sha256.Sum256(raw); sum != e.Hash {
			return fmt.Errorf("glcb: frame at %d hash mismatch — object corrupt", e.ObjOffset)
		}
		if _, err := dst.Write(raw); err != nil {
			return fmt.Errorf("write reassembled bytes: %w", err)
		}
	}
	return nil
}
