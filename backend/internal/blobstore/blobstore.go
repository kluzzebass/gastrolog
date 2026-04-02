// Package blobstore provides a unified interface for cloud object storage
// operations across S3, Azure Blob Storage, and Google Cloud Storage.
//
// Each provider implementation returns custom metadata in List results using
// the provider's native API, avoiding the per-object round-trips that generic
// abstraction libraries (e.g. gocloud.dev/blob) require.
package blobstore

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrStopIteration can be returned from a List callback to stop iteration
	// without signaling an error.
	ErrStopIteration = errors.New("stop iteration")

	// ErrBlobArchived indicates the blob exists but is in an offline storage
	// tier (S3 Glacier Flexible Retrieval/Deep Archive, Azure Archive) and
	// cannot be read without a restore operation.
	ErrBlobArchived = errors.New("blob is archived and not immediately readable")

	// ErrBlobNotFound indicates the blob does not exist in the store.
	// Distinct from transient errors (timeouts, auth failures) — this is
	// a definitive 404 from the provider.
	ErrBlobNotFound = errors.New("blob not found")
)

// Store is the interface for cloud object storage operations.
type Store interface {
	// EnsureBucket creates the bucket/container if it doesn't already exist.
	// Safe to call repeatedly — a no-op if the bucket exists.
	EnsureBucket(ctx context.Context) error

	// Upload writes data to the given key with optional metadata.
	Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error

	// Download returns a reader for the blob at the given key.
	Download(ctx context.Context, key string) (io.ReadCloser, error)

	// DownloadRange returns a reader for a byte range of the blob.
	// offset is the starting byte position; length is the number of bytes.
	// All major providers (S3, GCS, Azure) support HTTP Range requests natively.
	DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)

	// Delete removes the blob at the given key. No error if the key doesn't exist.
	Delete(ctx context.Context, key string) error

	// List iterates over all blobs matching the prefix, calling fn for each.
	// Includes metadata. Stops early if fn returns a non-nil error;
	// if fn returns ErrStopIteration, List returns nil.
	List(ctx context.Context, prefix string, fn func(BlobInfo) error) error

	// Head returns info for a single blob without downloading its contents.
	Head(ctx context.Context, key string) (BlobInfo, error)
}

// BlobInfo describes a single object in the store.
type BlobInfo struct {
	Key          string
	Size         int64
	Metadata     map[string]string
	StorageClass string // Provider-specific: S3 StorageClass, Azure AccessTier, GCS StorageClass
}

// Archiver extends Store with storage-class lifecycle operations.
// Not all providers support this (GCS has no offline tiers). Callers
// should type-assert to check availability.
type Archiver interface {
	// Archive transitions a blob to an offline storage class.
	// The blob remains in the store but Download/DownloadRange will return
	// ErrBlobArchived until Restore completes.
	Archive(ctx context.Context, key string, storageClass string) error

	// Restore initiates retrieval of an archived blob. On S3 this is async
	// (RestoreObject, takes minutes to hours). On Azure this is sync
	// (SetBlobTier to Hot/Cool). Returns nil if already restored or not archived.
	// tier is the restore speed ("Expedited"/"Standard"/"Bulk" for S3,
	// "High"/"Standard" for Azure, ignored for GCS).
	// days is how long the restored copy stays readable (S3 only, ignored elsewhere).
	Restore(ctx context.Context, key string, tier string, days int) error

	// IsRestoring returns true if a restore is in progress for the key.
	IsRestoring(ctx context.Context, key string) (bool, error)
}

// IsArchived returns true if the blob is in an offline storage tier that
// requires a restore operation before it can be read.
// S3: GLACIER, DEEP_ARCHIVE. Azure: Archive. GCS: always false (all readable).
func (b BlobInfo) IsArchived() bool {
	switch b.StorageClass {
	case "GLACIER", "DEEP_ARCHIVE": // S3
		return true
	case "Archive": // Azure
		return true
	case "cold", "deep-freeze": // Memory provider
		return true
	}
	return false
}
