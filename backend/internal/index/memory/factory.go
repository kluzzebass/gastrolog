package memory

import (
	"fmt"
	"strconv"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	memsource "gastrolog/internal/index/memory/source"
	memtime "gastrolog/internal/index/memory/time"
	memtoken "gastrolog/internal/index/memory/token"
)

// Factory parameter keys.
const (
	ParamTimeSparsity = "time_sparsity"
)

// Default values.
const (
	DefaultTimeSparsity = 1000 // Index every 1000th record for time index
)

// NewFactory returns a factory function that creates in-memory IndexManagers.
func NewFactory() index.ManagerFactory {
	return func(params map[string]string, chunkManager chunk.ChunkManager) (index.IndexManager, error) {
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

		timeIdx := memtime.NewIndexer(chunkManager, timeSparsity)
		srcIdx := memsource.NewIndexer(chunkManager)
		tokIdx := memtoken.NewIndexer(chunkManager)

		indexers := []index.Indexer{timeIdx, srcIdx, tokIdx}

		return NewManager(indexers, timeIdx, srcIdx, tokIdx), nil
	}
}
