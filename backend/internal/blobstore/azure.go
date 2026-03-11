package blobstore

import (
	"context"
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
		return nil, err
	}
	return resp.Body, nil
}

func (a *AzureStore) Delete(ctx context.Context, key string) error {
	_, err := a.client.DeleteBlob(ctx, a.containerName, key, nil)
	return err
}

func (a *AzureStore) List(ctx context.Context, prefix string) ([]BlobInfo, error) {
	var result []BlobInfo
	include := container.ListBlobsInclude{Metadata: true}
	pager := a.client.NewListBlobsFlatPager(a.containerName, &azblob.ListBlobsFlatOptions{
		Prefix:  &prefix,
		Include: include,
	})
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Segment.BlobItems {
			info := BlobInfo{
				Key: *item.Name,
			}
			if item.Properties != nil && item.Properties.ContentLength != nil {
				info.Size = *item.Properties.ContentLength
			}
			if item.Metadata != nil {
				info.Metadata = make(map[string]string, len(item.Metadata))
				for k, v := range item.Metadata {
					if v != nil {
						info.Metadata[strings.ToLower(k)] = *v
					}
				}
			}
			result = append(result, info)
		}
	}
	return result, nil
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
