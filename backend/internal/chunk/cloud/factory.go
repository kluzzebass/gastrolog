package cloud

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"

	"gastrolog/internal/blobstore"
	"gastrolog/internal/chunk"
)

// Factory parameter keys.
const (
	ParamProvider         = "provider"          // "s3", "azure", "gcs"
	ParamBucket           = "bucket"            // S3/GCS bucket name
	ParamRegion           = "region"            // S3 region
	ParamEndpoint         = "endpoint"          // S3/GCS custom endpoint
	ParamAccessKey        = "access_key"        // S3 access key
	ParamSecretKey        = "secret_key"        // S3 secret key
	ParamContainer        = "container"         // Azure container name
	ParamConnectionString = "connection_string" // Azure connection string
	ParamVaultID          = "_vault_id"         // Injected by orchestrator
)

var (
	ErrMissingProvider = errors.New("missing required parameter: provider")
	ErrUnknownProvider = errors.New("unknown provider (must be s3, azure, or gcs)")
)

// NewFactory returns a factory function that creates cloud ChunkManagers.
func NewFactory() chunk.ManagerFactory {
	return func(params map[string]string, logger *slog.Logger) (chunk.ChunkManager, error) {
		provider := params[ParamProvider]
		if provider == "" {
			return nil, ErrMissingProvider
		}

		vaultID, err := uuid.Parse(params[ParamVaultID])
		if err != nil {
			return nil, fmt.Errorf("invalid vault ID: %w", err)
		}

		store, err := createStore(provider, params)
		if err != nil {
			return nil, fmt.Errorf("create %s store: %w", provider, err)
		}

		return NewManager(store, vaultID, logger), nil
	}
}

func createStore(provider string, params map[string]string) (blobstore.Store, error) {
	switch provider {
	case "s3":
		cfg := blobstore.S3Config{
			Bucket:    params[ParamBucket],
			Region:    params[ParamRegion],
			Endpoint:  params[ParamEndpoint],
			AccessKey: params[ParamAccessKey],
			SecretKey: params[ParamSecretKey],
		}
		if cfg.Bucket == "" {
			return nil, errors.New("missing required parameter: bucket")
		}
		return blobstore.NewS3(context.Background(), cfg)

	case "azure":
		cfg := blobstore.AzureConfig{
			Container:        params[ParamContainer],
			ConnectionString: params[ParamConnectionString],
		}
		if cfg.Container == "" {
			return nil, errors.New("missing required parameter: container")
		}
		if cfg.ConnectionString == "" {
			return nil, errors.New("missing required parameter: connection_string")
		}
		return blobstore.NewAzure(cfg)

	case "gcs":
		cfg := blobstore.GCSConfig{
			Bucket:   params[ParamBucket],
			Endpoint: params[ParamEndpoint],
		}
		if cfg.Bucket == "" {
			return nil, errors.New("missing required parameter: bucket")
		}
		return blobstore.NewGCS(context.Background(), cfg)

	default:
		return nil, ErrUnknownProvider
	}
}
