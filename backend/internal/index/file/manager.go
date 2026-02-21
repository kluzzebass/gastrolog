package file

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	fileattr "gastrolog/internal/index/file/attr"
	filejson "gastrolog/internal/index/file/json"
	filekv "gastrolog/internal/index/file/kv"
	filetoken "gastrolog/internal/index/file/token"
	filetsidx "gastrolog/internal/index/file/tsidx"
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

// DeleteIndexes removes all index files and temp files for the given chunk.
func (m *Manager) DeleteIndexes(chunkID chunk.ChunkID) error {
	// Remove final index files.
	paths := []string{
		filetoken.IndexPath(m.dir, chunkID),
		fileattr.KeyIndexPath(m.dir, chunkID),
		fileattr.ValueIndexPath(m.dir, chunkID),
		fileattr.KVIndexPath(m.dir, chunkID),
		filetsidx.IngestIndexPath(m.dir, chunkID),
		filetsidx.SourceIndexPath(m.dir, chunkID),
		filekv.KeyIndexPath(m.dir, chunkID),
		filekv.ValueIndexPath(m.dir, chunkID),
		filekv.KVIndexPath(m.dir, chunkID),
		filejson.IndexPath(m.dir, chunkID),
	}

	for _, path := range paths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	// Also remove any orphaned temp files.
	patterns := []string{
		filetoken.TempFilePattern(m.dir, chunkID),
		fileattr.KeyTempFilePattern(m.dir, chunkID),
		fileattr.ValueTempFilePattern(m.dir, chunkID),
		fileattr.KVTempFilePattern(m.dir, chunkID),
		filetsidx.IngestTempFilePattern(m.dir, chunkID),
		filetsidx.SourceTempFilePattern(m.dir, chunkID),
		filekv.KeyTempFilePattern(m.dir, chunkID),
		filekv.ValueTempFilePattern(m.dir, chunkID),
		filekv.KVTempFilePattern(m.dir, chunkID),
		filejson.TempFilePattern(m.dir, chunkID),
	}

	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return err
		}
		for _, match := range matches {
			if err := os.Remove(match); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}

	return nil
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

func (m *Manager) OpenJSONPathIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPathIndexEntry], index.JSONIndexStatus, error) {
	pathEntries, _, status, err := filejson.LoadIndex(m.dir, chunkID)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, index.JSONComplete, index.ErrIndexNotFound
		}
		return nil, status, fmt.Errorf("open json path index: %w", err)
	}
	return index.NewIndex(pathEntries), status, nil
}

func (m *Manager) OpenJSONPVIndex(chunkID chunk.ChunkID) (*index.Index[index.JSONPVIndexEntry], index.JSONIndexStatus, error) {
	_, pvEntries, status, err := filejson.LoadIndex(m.dir, chunkID)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, index.JSONComplete, index.ErrIndexNotFound
		}
		return nil, status, fmt.Errorf("open json pv index: %w", err)
	}
	return index.NewIndex(pvEntries), status, nil
}

// FindIngestStartPosition implements index.IndexManager.
func (m *Manager) FindIngestStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	entries, err := filetsidx.LoadIngestIndex(m.dir, chunkID)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, index.ErrIndexNotFound
		}
		return 0, false, err
	}
	pos, found := filetsidx.FindStartPosition(entries, ts.UnixNano())
	return pos, found, nil
}

// FindSourceStartPosition implements index.IndexManager.
func (m *Manager) FindSourceStartPosition(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
	entries, err := filetsidx.LoadSourceIndex(m.dir, chunkID)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, index.ErrIndexNotFound
		}
		return 0, false, err
	}
	pos, found := filetsidx.FindStartPosition(entries, ts.UnixNano())
	return pos, found, nil
}

// IndexSizes returns the on-disk file size for each index.
func (m *Manager) IndexSizes(chunkID chunk.ChunkID) map[string]int64 {
	sizes := make(map[string]int64)
	paths := map[string]string{
		"token":     filetoken.IndexPath(m.dir, chunkID),
		"attr_key":  fileattr.KeyIndexPath(m.dir, chunkID),
		"attr_val":  fileattr.ValueIndexPath(m.dir, chunkID),
		"attr_kv":   fileattr.KVIndexPath(m.dir, chunkID),
		"ingest":    filetsidx.IngestIndexPath(m.dir, chunkID),
		"source":    filetsidx.SourceIndexPath(m.dir, chunkID),
		"kv_key":    filekv.KeyIndexPath(m.dir, chunkID),
		"kv_val":    filekv.ValueIndexPath(m.dir, chunkID),
		"kv_kv":     filekv.KVIndexPath(m.dir, chunkID),
		"json":      filejson.IndexPath(m.dir, chunkID),
	}
	for name, path := range paths {
		if info, err := os.Stat(path); err == nil {
			sizes[name] = info.Size()
		}
	}
	return sizes
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
		filetsidx.IngestIndexPath(m.dir, chunkID),
		filetsidx.SourceIndexPath(m.dir, chunkID),
		filekv.KeyIndexPath(m.dir, chunkID),
		filekv.ValueIndexPath(m.dir, chunkID),
		filekv.KVIndexPath(m.dir, chunkID),
		filejson.IndexPath(m.dir, chunkID),
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
		filetsidx.IngestTempFilePattern(m.dir, chunkID),
		filetsidx.SourceTempFilePattern(m.dir, chunkID),
		filekv.KeyTempFilePattern(m.dir, chunkID),
		filekv.ValueTempFilePattern(m.dir, chunkID),
		filekv.KVTempFilePattern(m.dir, chunkID),
		filejson.TempFilePattern(m.dir, chunkID),
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
