package memory

import (
	"fmt"
	"log/slog"
	"strconv"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	memattr "gastrolog/internal/index/memory/attr"
	"gastrolog/internal/index/memory/kv"
	memtoken "gastrolog/internal/index/memory/token"
)

// Factory parameter keys.
const (
	ParamKVBudget = "kvBudget"
)

// NewFactory returns a factory function that creates in-memory IndexManagers.
func NewFactory() index.ManagerFactory {
	return func(params map[string]string, chunkManager chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
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

		tokIdx := memtoken.NewIndexer(chunkManager)
		attrIdx := memattr.NewIndexer(chunkManager)
		kvIdx := kv.NewIndexerWithConfig(chunkManager, kv.Config{KVBudget: kvBudget})

		indexers := []index.Indexer{tokIdx, attrIdx, kvIdx}

		return NewManager(indexers, tokIdx, attrIdx, kvIdx, logger), nil
	}
}
