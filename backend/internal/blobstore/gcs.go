package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// GCSConfig holds configuration for a Google Cloud Storage store.
type GCSConfig struct {
	Bucket          string
	Endpoint        string // Optional: for fake-gcs-server or other emulators.
	CredentialsJSON string // Optional: inline service account JSON key.
}

// GCSStore implements Store using Google Cloud Storage.
type GCSStore struct {
	client *storage.Client
	bucket string
}

// NewGCS creates a new GCSStore. When Endpoint is set (e.g. for
// fake-gcs-server), authentication is skipped and JSON reads are
// forced for emulator compatibility.
func NewGCS(ctx context.Context, cfg GCSConfig) (*GCSStore, error) {
	// Always use JSON API for reads — the default XML path is not
	// supported by emulators and will be deprecated upstream.
	opts := []option.ClientOption{storage.WithJSONReads()}
	if cfg.Endpoint != "" {
		opts = append(opts,
			option.WithEndpoint(cfg.Endpoint+"/storage/v1/"),
			option.WithoutAuthentication(),
		)
	} else if cfg.CredentialsJSON != "" {
		opts = append(opts, option.WithAuthCredentialsJSON(option.ServiceAccount, []byte(cfg.CredentialsJSON)))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("create gcs client: %w", err)
	}
	return &GCSStore{client: client, bucket: cfg.Bucket}, nil
}

func (g *GCSStore) EnsureBucket(ctx context.Context) error {
	err := g.client.Bucket(g.bucket).Create(ctx, "", nil)
	if err != nil {
		// Ignore "bucket already exists" (status 409 / ErrBucketExists).
		return nil //nolint:nilerr
	}
	return nil
}

func (g *GCSStore) Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error {
	w := g.client.Bucket(g.bucket).Object(key).NewWriter(ctx)
	if len(metadata) > 0 {
		w.Metadata = metadata
	}
	if _, err := io.Copy(w, data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

func (g *GCSStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	return g.client.Bucket(g.bucket).Object(key).NewReader(ctx)
}

func (g *GCSStore) DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	return g.client.Bucket(g.bucket).Object(key).NewRangeReader(ctx, offset, length)
}

func (g *GCSStore) Delete(ctx context.Context, key string) error {
	err := g.client.Bucket(g.bucket).Object(key).Delete(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil // Match Store contract: no error if key doesn't exist.
	}
	return err
}

func (g *GCSStore) List(ctx context.Context, prefix string, fn func(BlobInfo) error) error {
	it := g.client.Bucket(g.bucket).Objects(ctx, &storage.Query{Prefix: prefix})
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
		info := BlobInfo{
			Key:          attrs.Name,
			Size:         attrs.Size,
			StorageClass: attrs.StorageClass,
		}
		if len(attrs.Metadata) > 0 {
			info.Metadata = attrs.Metadata
		}
		if err := fn(info); err != nil {
			if errors.Is(err, ErrStopIteration) {
				return nil
			}
			return err
		}
	}
}

func (g *GCSStore) Head(ctx context.Context, key string) (BlobInfo, error) {
	attrs, err := g.client.Bucket(g.bucket).Object(key).Attrs(ctx)
	if err != nil {
		return BlobInfo{}, err
	}
	info := BlobInfo{
		Key:          key,
		Size:         attrs.Size,
		StorageClass: attrs.StorageClass,
	}
	if len(attrs.Metadata) > 0 {
		info.Metadata = attrs.Metadata
	}
	return info, nil
}
