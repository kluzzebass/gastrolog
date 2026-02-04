package file

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	fileattr "gastrolog/internal/index/file/attr"
	filekv "gastrolog/internal/index/file/kv"
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

func (m *Manager) OpenTokenIndex(chunkID chunk.ChunkID) (*index.Index[index.TokenIndexEntry], error) {
	entries, err := filetoken.LoadIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open token index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenAttrKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKeyIndexEntry], error) {
	entries, err := fileattr.LoadKeyIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open attr key index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenAttrValueIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrValueIndexEntry], error) {
	entries, err := fileattr.LoadValueIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open attr value index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenAttrKVIndex(chunkID chunk.ChunkID) (*index.Index[index.AttrKVIndexEntry], error) {
	entries, err := fileattr.LoadKVIndex(m.dir, chunkID)
	if err != nil {
		return nil, fmt.Errorf("open attr kv index: %w", err)
	}
	return index.NewIndex(entries), nil
}

func (m *Manager) OpenKVKeyIndex(chunkID chunk.ChunkID) (*index.Index[index.KVKeyIndexEntry], index.KVIndexStatus, error) {
	entries, status, err := filekv.LoadKeyIndex(m.dir, chunkID)
	if err != nil {
		return nil, status, fmt.Errorf("open kv key index: %w", err)
	}
	return index.NewIndex(entries), status, nil
}

func (m *Manager) OpenKVValueIndex(chunkID chunk.ChunkID) (*index.Index[index.KVValueIndexEntry], index.KVIndexStatus, error) {
	entries, status, err := filekv.LoadValueIndex(m.dir, chunkID)
	if err != nil {
		return nil, status, fmt.Errorf("open kv value index: %w", err)
	}
	return index.NewIndex(entries), status, nil
}

func (m *Manager) OpenKVIndex(chunkID chunk.ChunkID) (*index.Index[index.KVIndexEntry], index.KVIndexStatus, error) {
	entries, status, err := filekv.LoadKVIndex(m.dir, chunkID)
	if err != nil {
		return nil, status, fmt.Errorf("open kv index: %w", err)
	}
	return index.NewIndex(entries), status, nil
}

// IndexesComplete reports whether all indexes exist for the given chunk.
// Also cleans up any orphaned temporary files from interrupted builds.
func (m *Manager) IndexesComplete(chunkID chunk.ChunkID) (bool, error) {
	// Check if all index files exist.
	indexPaths := []string{
		filetoken.IndexPath(m.dir, chunkID),
		fileattr.KeyIndexPath(m.dir, chunkID),
		fileattr.ValueIndexPath(m.dir, chunkID),
		fileattr.KVIndexPath(m.dir, chunkID),
		filekv.KeyIndexPath(m.dir, chunkID),
		filekv.ValueIndexPath(m.dir, chunkID),
		filekv.KVIndexPath(m.dir, chunkID),
	}

	for _, path := range indexPaths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return false, nil
		} else if err != nil {
			return false, err
		}
	}

	// Clean up orphaned temp files.
	tempPatterns := []string{
		filetoken.TempFilePattern(m.dir, chunkID),
		fileattr.KeyTempFilePattern(m.dir, chunkID),
		fileattr.ValueTempFilePattern(m.dir, chunkID),
		fileattr.KVTempFilePattern(m.dir, chunkID),
		filekv.KeyTempFilePattern(m.dir, chunkID),
		filekv.ValueTempFilePattern(m.dir, chunkID),
		filekv.KVTempFilePattern(m.dir, chunkID),
	}

	for _, pattern := range tempPatterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return false, err
		}
		for _, match := range matches {
			if err := os.Remove(match); err != nil {
				m.logger.Warn("failed to remove orphaned temp file",
					"path", match,
					"error", err)
			} else {
				m.logger.Info("removed orphaned temp file", "path", match)
			}
		}
	}

	return true, nil
}
