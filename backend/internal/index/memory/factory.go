package memory

import (
	"fmt"
	"log/slog"
	"strconv"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	memattr "gastrolog/internal/index/memory/attr"
	"gastrolog/internal/index/memory/kv"
	memtime "gastrolog/internal/index/memory/time"
	memtoken "gastrolog/internal/index/memory/token"
)

// Factory parameter keys.
const (
	ParamTimeSparsity = "timeSparsity"
	ParamKVBudget     = "kvBudget"
)

// Default values.
const (
	DefaultTimeSparsity = 1000 // Index every 1000th record for time index
)

// NewFactory returns a factory function that creates in-memory IndexManagers.
func NewFactory() index.ManagerFactory {
	return func(params map[string]string, chunkManager chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
		timeSparsity := DefaultTimeSparsity
		if v, ok := params[ParamTimeSparsity]; ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamTimeSparsity, err)
			}
			if n <= 0 {
				return nil, fmt.Errorf("invalid %s: must be positive", ParamTimeSparsity)
			}
			timeSparsity = n
		}

		var kvBudget int64
		if v, ok := params[ParamKVBudget]; ok {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid %s: %w", ParamKVBudget, err)
			}
			if n <= 0 {
				return nil, fmt.Errorf("invalid %s: must be positive", ParamKVBudget)
			}
			kvBudget = n
		}

		timeIdx := memtime.NewIndexer(chunkManager, timeSparsity)
		tokIdx := memtoken.NewIndexer(chunkManager)
		attrIdx := memattr.NewIndexer(chunkManager)
		kvIdx := kv.NewIndexerWithConfig(chunkManager, kv.Config{KVBudget: kvBudget})

		indexers := []index.Indexer{timeIdx, tokIdx, attrIdx, kvIdx}

		return NewManager(indexers, timeIdx, tokIdx, attrIdx, kvIdx, logger), nil
	}
}
