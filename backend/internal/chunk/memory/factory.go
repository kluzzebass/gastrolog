package memory

import (
	"fmt"
	"strconv"

	"gastrolog/internal/chunk"
)

// Factory parameter keys.
const (
	ParamMaxChunkBytes = "max_chunk_bytes"
)

// Default values.
const (
	DefaultMaxChunkBytes = 64 * 1024 * 1024 // 64 MiB
)

// NewFactory returns a factory function that creates in-memory ChunkManagers.
func NewFactory() chunk.ManagerFactory {
	return func(params map[string]string) (chunk.ChunkManager, error) {
		cfg := Config{
			MaxChunkBytes: DefaultMaxChunkBytes,
		}

		if v, ok := params[ParamMaxChunkBytes]; ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamMaxChunkBytes, err)
			}
			if n <= 0 {
				return nil, fmt.Errorf("invalid %s: must be positive", ParamMaxChunkBytes)
			}
			cfg.MaxChunkBytes = n
		}

		return NewManager(cfg)
	}
}
