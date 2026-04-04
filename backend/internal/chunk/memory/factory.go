package memory

import (
	"fmt"
	"log/slog"
	"strconv"

	"gastrolog/internal/chunk"
)

// Factory parameter keys.
const (
	ParamMaxRecords = "maxRecords"
	ParamMaxBytes   = "maxBytes" // per-chunk byte limit (from MemoryBudgetBytes)
)

// Default values.
const (
	DefaultMaxRecords = 10000 // 10k records per chunk
)

// NewFactory returns a factory function that creates in-memory ChunkManagers.
func NewFactory() chunk.ManagerFactory {
	return func(params map[string]string, logger *slog.Logger) (chunk.ChunkManager, error) {
		cfg := Config{
			Logger: logger,
		}

		var policies []chunk.RotationPolicy

		maxRecords := int64(DefaultMaxRecords)
		if v, ok := params[ParamMaxRecords]; ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamMaxRecords, err)
			}
			if n <= 0 {
				return nil, fmt.Errorf("invalid %s: must be positive", ParamMaxRecords)
			}
			maxRecords = n
		}
		policies = append(policies, chunk.NewRecordCountPolicy(uint64(maxRecords))) //nolint:gosec // G115: validated > 0

		if v, ok := params[ParamMaxBytes]; ok {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamMaxBytes, err)
			}
			if n > 0 {
				policies = append(policies, chunk.NewSizePolicy(n))
			}
		}

		cfg.RotationPolicy = chunk.NewCompositePolicy(policies...)

		return NewManager(cfg)
	}
}
