package file

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"gastrolog/internal/chunk"
)

// Factory parameter keys.
const (
	ParamDir           = "dir"
	ParamMaxChunkBytes = "maxChunkBytes"
	ParamMaxChunkAge   = "maxChunkAge"
	ParamFileMode      = "fileMode"
	ParamCompression   = "compression" // "none" or "zstd"

	// ParamExpectExisting is injected by the orchestrator when loading stores
	// from config. It tells the chunk manager to warn if the store directory
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

		// Build rotation policy from params
		var policies []chunk.RotationPolicy

		// Always include hard limits first (they always win)
		policies = append(policies, chunk.NewHardLimitPolicy(MaxRawLogSize, MaxAttrLogSize))

		// Add size policy if specified
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
			// Use default size limit
			policies = append(policies, chunk.NewSizePolicy(DefaultMaxChunkBytes))
		}

		// Add age policy if specified
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

		cfg.RotationPolicy = chunk.NewCompositePolicy(policies...)

		if v, ok := params[ParamCompression]; ok {
			switch v {
			case "zstd":
				cfg.Compression = CompressionZstd
			case "none", "":
				cfg.Compression = CompressionNone
			default:
				return nil, fmt.Errorf("invalid %s: %q (must be \"none\" or \"zstd\")", ParamCompression, v)
			}
		}

		if v, ok := params[ParamFileMode]; ok {
			n, err := strconv.ParseUint(v, 8, 32)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamFileMode, err)
			}
			cfg.FileMode = os.FileMode(n)
		}

		return NewManager(cfg)
	}
}
