package blobstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// AzureConfig holds configuration for an Azure Blob Storage store.
type AzureConfig struct {
	Container        string
	ConnectionString string // e.g. "UseDevelopmentStorage=true" for Azurite.
}

// AzureStore implements Store using Azure Blob Storage.
type AzureStore struct {
	client        *azblob.Client
	containerName string
}

// NewAzure creates a new AzureStore.
func NewAzure(cfg AzureConfig) (*AzureStore, error) {
	client, err := azblob.NewClientFromConnectionString(cfg.ConnectionString, nil)
	if err != nil {
		return nil, fmt.Errorf("create azure client: %w", err)
	}
	return &AzureStore{client: client, containerName: cfg.Container}, nil
}

func (a *AzureStore) EnsureBucket(ctx context.Context) error {
	_, err := a.client.CreateContainer(ctx, a.containerName, nil)
	if err != nil {
		// Ignore "container already exists" errors.
		return nil //nolint:nilerr
	}
	return nil
}

func (a *AzureStore) Upload(ctx context.Context, key string, data io.Reader, metadata map[string]string) error {
	opts := &azblob.UploadStreamOptions{
		BlockSize:   4 * 1024 * 1024, // 4MB blocks
		Concurrency: 4,
	}
	if len(metadata) > 0 {
		m := make(map[string]*string, len(metadata))
		for k, v := range metadata {
			m[k] = &v
		}
		opts.Metadata = m
	}
	_, err := a.client.UploadStream(ctx, a.containerName, key, data, opts)
	return err
}

func (a *AzureStore) Download(ctx context.Context, key string) (io.ReadCloser, error) {
	resp, err := a.client.DownloadStream(ctx, a.containerName, key, nil)
	if err != nil {
		if isAzureArchivedError(err) {
			return nil, fmt.Errorf("%w: %s", ErrBlobArchived, key)
		}
		return nil, err
	}
	return resp.Body, nil
}

func (a *AzureStore) DownloadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	resp, err := a.client.DownloadStream(ctx, a.containerName, key, &azblob.DownloadStreamOptions{
		Range: blob.HTTPRange{Offset: offset, Count: length},
	})
	if err != nil {
		if isAzureArchivedError(err) {
			return nil, fmt.Errorf("%w: %s", ErrBlobArchived, key)
		}
		return nil, err
	}
	return resp.Body, nil
}

func (a *AzureStore) Delete(ctx context.Context, key string) error {
	_, err := a.client.DeleteBlob(ctx, a.containerName, key, nil)
	return err
}

func (a *AzureStore) List(ctx context.Context, prefix string, fn func(BlobInfo) error) error {
	include := container.ListBlobsInclude{Metadata: true}
	pager := a.client.NewListBlobsFlatPager(a.containerName, &azblob.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: include,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return err
		}
		for _, item := range page.Segment.BlobItems {
			info := azureBlobToInfo(item)
			if err := fn(info); err != nil {
				if errors.Is(err, ErrStopIteration) {
					return nil
				}
				return err
			}
		}
	}
	return nil
}

func (a *AzureStore) Head(ctx context.Context, key string) (BlobInfo, error) {
	resp, err := a.client.ServiceClient().NewContainerClient(a.containerName).NewBlobClient(key).GetProperties(ctx, &blob.GetPropertiesOptions{})
	if err != nil {
		return BlobInfo{}, err
	}
	info := BlobInfo{
		Key: key,
	}
	if resp.ContentLength != nil {
		info.Size = *resp.ContentLength
	}
	if resp.Metadata != nil {
		info.Metadata = make(map[string]string, len(resp.Metadata))
		for k, v := range resp.Metadata {
			if v != nil {
				info.Metadata[strings.ToLower(k)] = *v
			}
		}
	}
	return info, nil
}

// azureBlobToInfo converts an Azure BlobItem to a BlobInfo.
func azureBlobToInfo(item *container.BlobItem) BlobInfo {
	info := BlobInfo{Key: *item.Name}
	if item.Properties != nil {
		if item.Properties.ContentLength != nil {
			info.Size = *item.Properties.ContentLength
		}
		if item.Properties.AccessTier != nil {
			info.StorageClass = string(*item.Properties.AccessTier)
		}
	}
	if item.Metadata != nil {
		info.Metadata = make(map[string]string, len(item.Metadata))
		for k, v := range item.Metadata {
			if v != nil {
				info.Metadata[strings.ToLower(k)] = *v
			}
		}
	}
	return info
}

// isAzureArchivedError checks if an Azure error is due to the blob being
// in the Archive tier (409 Conflict with "BlobArchived" error code).
func isAzureArchivedError(err error) bool {
	if err == nil {
		return false
	}
	// Azure returns 409 Conflict with "BlobArchived" for archive-tier blobs.
	errStr := err.Error()
	return strings.Contains(errStr, "BlobArchived") ||
		strings.Contains(errStr, "This operation is not permitted on an archived blob")
}
