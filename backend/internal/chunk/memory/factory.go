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
	ParamBudget     = "budgetBytes" // total tier budget in bytes
)

// Default values.
const (
	DefaultMaxRecords   = 10000 // 10k records per chunk
	DefaultChunkDivisor = 10   // budget / 10 = per-chunk size limit
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

		if v, ok := params[ParamBudget]; ok {
			n, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamBudget, err)
			}
			if n > 0 {
				cfg.BudgetBytes = n
				// Per-chunk rotation at budget/10 so we have ~10 chunks to work with
				// before hitting the total budget. This keeps individual transitions small.
				perChunk := n / DefaultChunkDivisor
				if perChunk == 0 {
					perChunk = n
				}
				policies = append(policies, chunk.NewSizePolicy(perChunk))
			}
		}

		cfg.RotationPolicy = chunk.NewCompositePolicy(policies...)

		return NewManager(cfg)
	}
}
