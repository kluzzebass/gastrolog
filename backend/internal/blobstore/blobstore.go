// Package blobstore provides a unified interface for cloud object storage
// operations across S3, Azure Blob Storage, and Google Cloud Storage.
//
// Each provider implementation returns custom metadata in List results using
// the provider's native API, avoiding the per-object round-trips that generic
// abstraction libraries (e.g. gocloud.dev/blob) require.
package blobstore

import (
	"context"
	"io"
)

// Store is the interface for cloud object storage operations.
type Store interface {
	// Upload writes data to the given key with optional metadata.
	Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error

	// Download returns a reader for the blob at the given key.
	Download(ctx context.Context, key string) (io.ReadCloser, error)

	// Delete removes the blob at the given key. No error if the key doesn't exist.
	Delete(ctx context.Context, key string) error

	// List returns all blobs matching the prefix, including their metadata.
	List(ctx context.Context, prefix string) ([]BlobInfo, error)

	// Head returns info for a single blob without downloading its contents.
	Head(ctx context.Context, key string) (BlobInfo, error)
}

// BlobInfo describes a single object in the store.
type BlobInfo struct {
	Key      string
	Size     int64
	Metadata map[string]string
}
