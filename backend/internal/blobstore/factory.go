package blobstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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
	ParamCredentialsJSON  = "credentials_json"  // GCS service account JSON
	ParamSealedBacking    = "sealed_backing"    // file-vault sealed-backing provider; accepted as a provider alias
	ParamVaultID          = "_vault_id"         // Injected by orchestrator
)

// connectionProbeKey is the throwaway object key the connection tester writes,
// reads, lists, and deletes to prove the credentials and bucket work.
const connectionProbeKey = ".gastrolog-connection-test"

var (
	ErrMissingProvider = errors.New("missing required parameter: provider")
	ErrUnknownProvider = errors.New("unknown provider (must be s3, azure, gcs, or memory)")
)

// NewConnectionTester returns a function that validates cloud storage connectivity
// by exercising the same operations the vault will use at runtime:
// EnsureBucket, Upload, Download, List, Delete. log receives provider-SDK
// diagnostics via the blobstore component tree.
func NewConnectionTester(log *slog.Logger) func(ctx context.Context, params map[string]string) (string, error) {
	return func(ctx context.Context, params map[string]string) (string, error) {
		provider := params[ParamProvider]
		if provider == "" {
			provider = params[ParamSealedBacking]
		}
		if provider == "" {
			return "", ErrMissingProvider
		}
		store, err := createStore(provider, params, log)
		if err != nil {
			return "", err
		}

		// 1. Ensure bucket exists.
		if err := store.EnsureBucket(ctx); err != nil {
			return "", fmt.Errorf("ensure bucket: %w", err)
		}

		// 2. Upload a probe object.
		probeKey := connectionProbeKey
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
		if err := store.List(ctx, probeKey, func(_ BlobInfo) error {
			return ErrStopIteration
		}); err != nil {
			return "", fmt.Errorf("list: %w", err)
		}

		// 5. Clean up.
		_ = store.Delete(ctx, probeKey)

		return fmt.Sprintf("Connected to %s: bucket ok, read/write ok", provider), nil
	}
}

// CreateStore creates a Store for the given provider and params.
// Exported for use by the file vault's sealed backing integration. log
// receives provider-SDK diagnostics via the blobstore component tree.
func CreateStore(provider string, params map[string]string, log *slog.Logger) (Store, error) {
	return createStore(provider, params, log)
}

// validateEndpoint requires an explicit scheme on a custom endpoint. A bare
// host:port used to be silently upgraded to plaintext "http://", which pointed
// data-plane credentials at an unencrypted endpoint whenever an operator
// merely forgot the scheme. Misconfiguration now fails loudly at config time,
// naming both accepted forms, instead of quietly downgrading transport
// security.
func validateEndpoint(ep string) (string, error) {
	if ep == "" {
		return "", nil
	}
	if !strings.Contains(ep, "://") {
		return "", fmt.Errorf("endpoint %q has no scheme: use \"https://%s\", or \"http://%s\" for a plaintext local/dev endpoint", ep, ep, ep)
	}
	return ep, nil
}

func createStore(provider string, params map[string]string, log *slog.Logger) (Store, error) {
	switch provider {
	case "s3":
		endpoint, err := validateEndpoint(params[ParamEndpoint])
		if err != nil {
			return nil, err
		}
		cfg := S3Config{
			Bucket:    params[ParamBucket],
			Region:    params[ParamRegion],
			Endpoint:  endpoint,
			AccessKey: params[ParamAccessKey],
			SecretKey: params[ParamSecretKey],
			Logger:    log,
		}
		if cfg.Bucket == "" {
			return nil, errors.New("missing required parameter: bucket")
		}
		if cfg.Region == "" {
			return nil, errors.New("missing required parameter: region")
		}
		return NewS3(context.Background(), cfg)

	case "azure":
		cfg := AzureConfig{
			Container:        params[ParamContainer],
			ConnectionString: params[ParamConnectionString],
		}
		if cfg.Container == "" {
			return nil, errors.New("missing required parameter: container")
		}
		if cfg.ConnectionString == "" {
			return nil, errors.New("missing required parameter: connection_string")
		}
		return NewAzure(cfg)

	case "gcs":
		endpoint, err := validateEndpoint(params[ParamEndpoint])
		if err != nil {
			return nil, err
		}
		cfg := GCSConfig{
			Bucket:          params[ParamBucket],
			Endpoint:        endpoint,
			CredentialsJSON: params[ParamCredentialsJSON],
		}
		if cfg.Bucket == "" {
			return nil, errors.New("missing required parameter: bucket")
		}
		return NewGCS(context.Background(), cfg)

	case "memory":
		return NewMemory(), nil

	default:
		return nil, ErrUnknownProvider
	}
}
