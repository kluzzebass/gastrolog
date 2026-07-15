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

// ValidateConfig checks the deterministic config shape for a provider —
// per-provider required params and the endpoint scheme rule — without
// constructing a client or touching the network. It is the single source
// of truth for config-shape rules: createStore runs it before building a
// store, and the config-mutation RPC handler (PutCloudService) runs it
// before accepting a cloud-service config, so accept-time and init-time
// rules cannot drift. Keep it deterministic — no DNS, no probes — so it
// yields the same verdict on every node; it must never run inside the FSM
// apply path, where a rejection would break replay of persisted state.
func ValidateConfig(provider string, params map[string]string) error {
	switch provider {
	case "s3":
		if _, err := validateEndpoint(params[ParamEndpoint]); err != nil {
			return err
		}
		if params[ParamBucket] == "" {
			return errors.New("missing required parameter: bucket")
		}
		if params[ParamRegion] == "" {
			return errors.New("missing required parameter: region")
		}
		return nil

	case "azure":
		if params[ParamContainer] == "" {
			return errors.New("missing required parameter: container")
		}
		if params[ParamConnectionString] == "" {
			return errors.New("missing required parameter: connection_string")
		}
		return nil

	case "gcs":
		if _, err := validateEndpoint(params[ParamEndpoint]); err != nil {
			return err
		}
		if params[ParamBucket] == "" {
			return errors.New("missing required parameter: bucket")
		}
		return nil

	case "memory":
		return nil

	case "":
		return ErrMissingProvider

	default:
		return ErrUnknownProvider
	}
}

func createStore(provider string, params map[string]string, log *slog.Logger) (Store, error) {
	if err := ValidateConfig(provider, params); err != nil {
		return nil, err
	}
	switch provider {
	case "s3":
		return NewS3(context.Background(), S3Config{
			Bucket:    params[ParamBucket],
			Region:    params[ParamRegion],
			Endpoint:  params[ParamEndpoint],
			AccessKey: params[ParamAccessKey],
			SecretKey: params[ParamSecretKey],
			Logger:    log,
		})

	case "azure":
		return NewAzure(AzureConfig{
			Container:        params[ParamContainer],
			ConnectionString: params[ParamConnectionString],
		})

	case "gcs":
		return NewGCS(context.Background(), GCSConfig{
			Bucket:          params[ParamBucket],
			Endpoint:        params[ParamEndpoint],
			CredentialsJSON: params[ParamCredentialsJSON],
		})

	case "memory":
		return NewMemory(), nil

	default:
		// Unreachable: ValidateConfig rejected every other provider.
		return nil, ErrUnknownProvider
	}
}
