package tsidx

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/format"
	"gastrolog/internal/logging"
)

// SourceIndexer builds a SourceTS index for sealed chunks.
// Only records with SourceTS != 0 are indexed.
type SourceIndexer struct {
	dir     string
	manager chunk.ChunkManager
	logger  *slog.Logger
}

// NewSourceIndexer creates an indexer for SourceTS.
func NewSourceIndexer(dir string, manager chunk.ChunkManager, logger *slog.Logger) *SourceIndexer {
	return &SourceIndexer{
		dir:     dir,
		manager: manager,
		logger:  logging.Default(logger).With("component", "indexer", "type", "source"),
	}
}

// Name implements index.Indexer.
func (s *SourceIndexer) Name() string {
	return "source"
}

// Build implements index.Indexer.
func (s *SourceIndexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	meta, err := s.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	var entries []Entry
	cursor, err := s.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, ref, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			return fmt.Errorf("read record: %w", err)
		}
		// Skip records with no source timestamp.
		if rec.SourceTS.IsZero() {
			continue
		}
		ts := rec.SourceTS.UnixNano()
		entries = append(entries, Entry{TS: ts, Pos: uint32(ref.Pos)})
	}

	chunkDir := filepath.Join(s.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	target := SourceIndexPath(s.dir, chunkID)
	tmpFile, err := os.CreateTemp(chunkDir, sourceFile+".tmp.*")
	if err != nil {
		return fmt.Errorf("create temp index: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if err := tmpFile.Chmod(0o644); err != nil {
		tmpFile.Close()
		return fmt.Errorf("chmod temp: %w", err)
	}
	data := encodeIndex(entries, format.TypeSourceIndex)
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		return fmt.Errorf("write index: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close temp: %w", err)
	}
	if err := os.Rename(tmpName, target); err != nil {
		return fmt.Errorf("rename index: %w", err)
	}

	s.logger.Debug("source index built", "chunk", chunkID.String(), "entries", len(entries))
	return nil
}
