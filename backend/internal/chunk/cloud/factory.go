package cloud

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gastrolog/internal/blobstore"
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
	ParamConnectionString = "connection_string"  // Azure connection string
	ParamCredentialsJSON  = "credentials_json"   // GCS service account JSON
	ParamVaultID          = "_vault_id"          // Injected by orchestrator
)

var (
	ErrMissingProvider = errors.New("missing required parameter: provider")
	ErrUnknownProvider = errors.New("unknown provider (must be s3, azure, or gcs)")
)

// NewConnectionTester returns a function that validates cloud storage connectivity
// by creating a temporary store and listing objects.
// NewConnectionTester returns a function that validates cloud storage connectivity
// by exercising the same operations the vault will use at runtime:
// EnsureBucket, Upload, Download, List, Delete.
func NewConnectionTester() func(ctx context.Context, params map[string]string) (string, error) {
	return func(ctx context.Context, params map[string]string) (string, error) {
		provider := params[ParamProvider]
		if provider == "" {
			provider = params["sealed_backing"]
		}
		if provider == "" {
			return "", ErrMissingProvider
		}
		store, err := createStore(provider, params)
		if err != nil {
			return "", err
		}

		// 1. Ensure bucket exists.
		if err := store.EnsureBucket(ctx); err != nil {
			return "", fmt.Errorf("ensure bucket: %w", err)
		}

		// 2. Upload a probe object.
		probeKey := ".gastrolog-connection-test"
		probeData := strings.NewReader("ok")
		if err := store.Upload(ctx, probeKey, probeData, nil); err != nil {
			return "", fmt.Errorf("upload probe: %w", err)
		}

		// 3. Download it back.
		rc, err := store.Download(ctx, probeKey)
		if err != nil {
			return "", fmt.Errorf("download probe: %w", err)
		}
		_ = rc.Close()

		// 4. List to verify iteration works.
		if err := store.List(ctx, probeKey, func(_ blobstore.BlobInfo) error {
			return blobstore.ErrStopIteration
		}); err != nil {
			return "", fmt.Errorf("list: %w", err)
		}

		// 5. Clean up.
		_ = store.Delete(ctx, probeKey)

		return fmt.Sprintf("Connected to %s: bucket ok, read/write ok", provider), nil
	}
}

// CreateStore creates a blobstore.Store for the given provider and params.
// Exported for use by the file vault's sealed backing integration.
func CreateStore(provider string, params map[string]string) (blobstore.Store, error) {
	return createStore(provider, params)
}

// normalizeEndpoint ensures a custom endpoint has a scheme.
// The AWS SDK rejects bare host:port like "localhost:9000".
func normalizeEndpoint(ep string) string {
	if ep == "" {
		return ""
	}
	if !strings.Contains(ep, "://") {
		return "http://" + ep
	}
	return ep
}

func createStore(provider string, params map[string]string) (blobstore.Store, error) {
	switch provider {
	case "s3":
		cfg := blobstore.S3Config{
			Bucket:    params[ParamBucket],
			Region:    params[ParamRegion],
			Endpoint:  normalizeEndpoint(params[ParamEndpoint]),
			AccessKey: params[ParamAccessKey],
			SecretKey: params[ParamSecretKey],
		}
		if cfg.Bucket == "" {
			return nil, errors.New("missing required parameter: bucket")
		}
		if cfg.Region == "" {
			return nil, errors.New("missing required parameter: region")
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
			Bucket:          params[ParamBucket],
			Endpoint:        normalizeEndpoint(params[ParamEndpoint]),
			CredentialsJSON: params[ParamCredentialsJSON],
		}
		if cfg.Bucket == "" {
			return nil, errors.New("missing required parameter: bucket")
		}
		return blobstore.NewGCS(context.Background(), cfg)

	case "memory":
		return blobstore.NewMemory(), nil

	default:
		return nil, ErrUnknownProvider
	}
}
