package file

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"gastrolog/internal/chunk"
	chunkcloud "gastrolog/internal/chunk/cloud"

	"github.com/google/uuid"
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
	vaultID, err := uuid.Parse(params[chunkcloud.ParamVaultID])
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
