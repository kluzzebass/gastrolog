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

// IngestIndexer builds an IngestTS index for sealed chunks.
type IngestIndexer struct {
	dir     string
	manager chunk.ChunkManager
	logger  *slog.Logger
}

// NewIngestIndexer creates an indexer for IngestTS.
func NewIngestIndexer(dir string, manager chunk.ChunkManager, logger *slog.Logger) *IngestIndexer {
	return &IngestIndexer{
		dir:     dir,
		manager: manager,
		logger:  logging.Default(logger).With("component", "indexer", "type", "ingest"),
	}
}

// Name implements index.Indexer.
func (i *IngestIndexer) Name() string {
	return "ingest"
}

// Build implements index.Indexer.
func (i *IngestIndexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	meta, err := i.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	var entries []Entry
	cursor, err := i.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	var pos uint32
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
		ts := rec.IngestTS.UnixNano()
		entries = append(entries, Entry{TS: ts, Pos: uint32(ref.Pos)})
		pos++
	}

	chunkDir := filepath.Join(i.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	target := IngestIndexPath(i.dir, chunkID)
	tmpFile, err := os.CreateTemp(chunkDir, ingestFile+".tmp.*")
	if err != nil {
		return fmt.Errorf("create temp index: %w", err)
	}
	tmpName := tmpFile.Name()
	defer os.Remove(tmpName)

	if err := tmpFile.Chmod(0o644); err != nil {
		tmpFile.Close()
		return fmt.Errorf("chmod temp: %w", err)
	}
	data := encodeIndex(entries, format.TypeIngestIndex)
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

	i.logger.Debug("ingest index built", "chunk", chunkID.String(), "entries", len(entries))
	return nil
}
