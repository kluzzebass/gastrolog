package memory

import (
	"fmt"
	"strconv"

	"gastrolog/internal/chunk"
)

// Factory parameter keys.
const (
	ParamMaxRecords = "maxRecords"
	ParamMaxChunks  = "maxChunks"
)

// Default values.
const (
	DefaultMaxRecords = 10000 // 10k records per chunk
	DefaultMaxChunks  = 10    // keep at most 10 chunks in memory
)

// NewFactory returns a factory function that creates in-memory ChunkManagers.
func NewFactory() chunk.ManagerFactory {
	return func(params map[string]string) (chunk.ChunkManager, error) {
		cfg := Config{}

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
		cfg.RotationPolicy = chunk.NewRecordCountPolicy(uint64(maxRecords))

		cfg.MaxChunks = DefaultMaxChunks
		if v, ok := params[ParamMaxChunks]; ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamMaxChunks, err)
			}
			if n < 0 {
				return nil, fmt.Errorf("invalid %s: must be non-negative", ParamMaxChunks)
			}
			cfg.MaxChunks = int(n)
		}

		return NewManager(cfg)
	}
}
