package file

import (
	"context"
	"errors"
	"fmt"
	"gastrolog/internal/glid"
	"log/slog"
	"os"
	"strconv"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"
	"gastrolog/internal/system"
)

// Factory parameter keys.
const (
	ParamDir           = "dir"
	ParamMaxChunkBytes = "maxChunkBytes"
	ParamMaxChunkAge   = "maxChunkAge"
	ParamFileMode      = "fileMode"
	ParamSealedBacking = "sealed_backing" // "local", "s3", "azure", "gcs" (default: local)

	// ParamExpectExisting is injected by the orchestrator when loading vaults
	// from config. It tells the chunk manager to warn if the vault directory
	// is missing (potential data loss). Not persisted in config.
	ParamExpectExisting = "_expect_existing"
)

// Default values.
const (
	DefaultMaxChunkBytes = 64 * 1024 * 1024 // 64 MiB
	DefaultFileMode      = 0o644
)

var (
	ErrMissingDirParam = errors.New("missing required parameter: dir")
)

// NewFactory returns a factory function that creates file-based ChunkManagers.
func NewFactory() chunk.ManagerFactory {
	return func(params map[string]string, logger *slog.Logger) (chunk.ChunkManager, error) {
		dir, ok := params[ParamDir]
		if !ok || dir == "" {
			return nil, ErrMissingDirParam
		}

		cfg := Config{
			Dir:            dir,
			FileMode:       DefaultFileMode,
			Logger:         logger,
			ExpectExisting: params[ParamExpectExisting] == "true",
		}

		policy, err := buildRotationPolicy(params)
		if err != nil {
			return nil, err
		}
		cfg.RotationPolicy = policy

		if v, ok := params[ParamFileMode]; ok {
			n, err := strconv.ParseUint(v, 8, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamFileMode, err)
			}
			cfg.FileMode = os.FileMode(n)
		}

		// Sealed backing: when set to a cloud provider, sealed chunks
		// are uploaded to cloud storage after compression.
		if err := configureSealedBacking(&cfg, params); err != nil {
			return nil, err
		}
		if params["_cloud_read_only"] == "true" {
			cfg.CloudReadOnly = true
		}

		// CloudServiceID: snapshot of the cloud service this Manager is
		// currently wired to. Stamped onto every CmdUploadChunk so the
		// chunk's authoritative store survives a future tier reconfiguration
		// that repoints CloudStore. Optional in tests; required in the
		// orchestrator dispatch path. See gastrolog-grnc3.
		if v := params["cloud_service_id"]; v != "" {
			csID, err := glid.ParseUUID(v)
			if err != nil {
				return nil, fmt.Errorf("invalid cloud_service_id: %w", err)
			}
			cfg.CloudServiceID = csID
		}

		// cache_dir is silently ignored — step 7k made <chunkDir>/data.glcb
		// the warm cache so a separate cache directory is no longer
		// meaningful.
		if v := params["cache_eviction"]; v != "" {
			cfg.CacheEviction = v
		}
		if v := params["cache_budget"]; v != "" {
			parsed, err := system.ParseSize(v)
			if err != nil {
				return nil, fmt.Errorf("invalid cache_budget: %w", err)
			}
			cfg.CacheBudgetBytes = parsed
		}
		if v := params["cache_ttl"]; v != "" {
			parsed, err := system.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid cache_ttl: %w", err)
			}
			cfg.CacheTTL = parsed
		}

		return NewManager(cfg)
	}
}

// configureSealedBacking sets up cloud store configuration when a sealed
// backing provider is specified.
func configureSealedBacking(cfg *Config, params map[string]string) error {
	backing := params[ParamSealedBacking]
	if backing == "" || backing == "local" {
		return nil
	}

	store, err := chunkcloud.CreateStore(backing, params)
	if err != nil {
		return fmt.Errorf("create %s store for sealed backing: %w", backing, err)
	}
	if err := store.EnsureBucket(context.Background()); err != nil {
		return fmt.Errorf("ensure %s bucket for sealed backing: %w", backing, err)
	}
	vaultID, err := glid.ParseUUID(params[chunkcloud.ParamVaultID])
	if err != nil {
		return fmt.Errorf("invalid vault ID for sealed backing: %w", err)
	}
	cfg.CloudStore = store
	cfg.VaultID = vaultID
	return nil
}

// buildRotationPolicy constructs a composite rotation policy from factory params.
func buildRotationPolicy(params map[string]string) (chunk.RotationPolicy, error) {
	var policies []chunk.RotationPolicy

	// Hard limits always apply (4GB for uint32 offsets).
	policies = append(policies, chunk.NewHardLimitPolicy(MaxRawLogSize, MaxAttrLogSize))

	// Size policy.
	if v, ok := params[ParamMaxChunkBytes]; ok {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", ParamMaxChunkBytes, err)
		}
		if n <= 0 {
			return nil, fmt.Errorf("invalid %s: must be positive", ParamMaxChunkBytes)
		}
		policies = append(policies, chunk.NewSizePolicy(uint64(n)))
	} else {
		policies = append(policies, chunk.NewSizePolicy(DefaultMaxChunkBytes))
	}

	// Age policy.
	if v, ok := params[ParamMaxChunkAge]; ok {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid %s: %w", ParamMaxChunkAge, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("invalid %s: must be positive", ParamMaxChunkAge)
		}
		policies = append(policies, chunk.NewAgePolicy(d, nil))
	}

	return chunk.NewCompositePolicy(policies...), nil
}
