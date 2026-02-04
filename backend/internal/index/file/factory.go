package file

import (
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	fileattr "gastrolog/internal/index/file/attr"
	filekv "gastrolog/internal/index/file/kv"
	filetoken "gastrolog/internal/index/file/token"
)

// Factory parameter keys.
const (
	ParamDir      = "dir"
	ParamKVBudget = "kvBudget"
)

var (
	ErrMissingDirParam = errors.New("missing required parameter: dir")
)

// NewFactory returns a factory function that creates file-based IndexManagers.
func NewFactory() index.ManagerFactory {
	return func(params map[string]string, chunkManager chunk.ChunkManager, logger *slog.Logger) (index.IndexManager, error) {
		dir, ok := params[ParamDir]
		if !ok || dir == "" {
			return nil, ErrMissingDirParam
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

		indexers := []index.Indexer{
			filetoken.NewIndexer(dir, chunkManager, logger),
			fileattr.NewIndexer(dir, chunkManager, logger),
			filekv.NewIndexerWithConfig(dir, chunkManager, logger, filekv.Config{KVBudget: kvBudget}),
		}

		return NewManager(dir, indexers, logger), nil
	}
}
