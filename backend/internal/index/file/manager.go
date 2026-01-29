package file

import (
	"context"
	"fmt"
	"log/slog"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	filesource "gastrolog/internal/index/file/source"
	filetime "gastrolog/internal/index/file/time"
	filetoken "gastrolog/internal/index/file/token"
	"gastrolog/internal/logging"
)

// Manager manages file-based index storage.
//
// Logging:
//   - Logger is dependency-injected via NewManager
//   - Manager owns its scoped logger (component="index-manager", type="file")
//   - Logging is intentionally sparse; only lifecycle events are logged
//   - No logging in hot paths (index lookups)
type Manager struct {
	dir      string
	indexers []index.Indexer
	builder  *index.BuildHelper

	// Logger for this manager instance.
	// Scoped with component="index-manager", type="file" at construction time.
	logger *slog.Logger
}

// NewManager creates a file-based index manager.
// If logger is nil, logging is disabled.
func NewManager(dir string, indexers []index.Indexer, logger *slog.Logger) *Manager {
	return &Manager{
		dir:      dir,
		indexers: indexers,
		builder:  index.NewBuildHelper(),
		logger:   logging.Default(logger).With("component", "index-manager", "type", "file"),
	}
}

func (m *Manager) BuildIndexes(ctx context.Context, chunkID chunk.ChunkID) error {
	return m.builder.Build(ctx, chunkID, m.indexers)
}

func (m *Manager) OpenTimeIndex(chunkID chunk.ChunkID) (*index.Index[index.TimeIndexEntry], error) {
	entries, err := filetime.LoadIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open time index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenSourceIndex(chunkID chunk.ChunkID) (*index.Index[index.SourceIndexEntry], error) {
	entries, err := filesource.LoadIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open source index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	entries, err := filetoken.LoadIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open token index: %w", err)
	}
	return index.NewIndex(entries), nil
}
