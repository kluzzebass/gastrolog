package cloud

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// DownloadAndUnwrap fetches a zstd-wrapped GLCB from the blob store,
// decompresses it into the given destination file, and returns the
// destination file (positioned at offset 0) ready to be passed to
// NewReader.
//
// The destination file is the caller's responsibility — typically a
// temp file or a local cache file. The caller closes / removes it via
// the returned Reader's Close().
//
// Cloud transport contract: cloud blobs are zstd-compressed wrappers
// around GLCBs (see docs/vault_redesign.md decisions 6 and 9). The
// format itself is silent on compression; the wrapper is added at
// upload and removed at download. This function is the canonical
// download-side unwrap.
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

	dec, err := zstd.NewReader(rc)
	if err != nil {
		return fmt.Errorf("zstd reader: %w", err)
	}
	defer dec.Close()

	if _, err := io.Copy(dst, dec); err != nil {
		return fmt.Errorf("decompress cloud blob: %w", err)
	}
	if _, err := dst.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("rewind GLCB: %w", err)
	}
	return nil
}
