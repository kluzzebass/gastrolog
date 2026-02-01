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
	filetime "gastrolog/internal/index/file/time"
	filetoken "gastrolog/internal/index/file/token"
)

// Factory parameter keys.
const (
	ParamDir          = "dir"
	ParamTimeSparsity = "timeSparsity"
	ParamKVBudget     = "kvBudget"
)

// Default values.
const (
	DefaultTimeSparsity = 1000 // Index every 1000th record for time index
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

		indexers := []index.Indexer{
			filetime.NewIndexer(dir, chunkManager, timeSparsity, logger),
			filetoken.NewIndexer(dir, chunkManager, logger),
			fileattr.NewIndexer(dir, chunkManager, logger),
			filekv.NewIndexerWithConfig(dir, chunkManager, logger, filekv.Config{KVBudget: kvBudget}),
		}

		return NewManager(dir, indexers, logger), nil
	}
}
