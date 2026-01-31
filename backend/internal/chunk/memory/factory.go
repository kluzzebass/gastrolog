package memory

import (
	"fmt"
	"strconv"

	"gastrolog/internal/chunk"
)

// Factory parameter keys.
const (
	ParamMaxRecords = "max_records"
)

// Default values.
const (
	DefaultMaxRecords = 10000 // 10k records per chunk
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

		return NewManager(cfg)
	}
}
