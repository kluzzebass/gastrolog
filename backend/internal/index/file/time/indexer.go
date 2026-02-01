package time

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/logging"
)

// Indexer builds a sparse time index for sealed chunks.
// For each chunk, it samples every N-th record's (WriteTS, RecordPos)
// and writes the result to <dir>/<chunkID>/_time.idx.
type Indexer struct {
	dir      string
	manager  chunk.ChunkManager
	sparsity int
	logger   *slog.Logger
}

func NewIndexer(dir string, manager chunk.ChunkManager, sparsity int, logger *slog.Logger) *Indexer {
	return &Indexer{
		dir:      dir,
		manager:  manager,
		sparsity: sparsity,
		logger:   logging.Default(logger).With("component", "indexer", "type", "time"),
	}
}

func (t *Indexer) Name() string {
	return "time"
}

func (t *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	buildStart := time.Now()

	meta, err := t.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	cursor, err := t.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	var entries []index.TimeIndexEntry
	var recordCount uint64

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

		if recordCount == 0 || recordCount%uint64(t.sparsity) == 0 {
			entries = append(entries, index.TimeIndexEntry{
				Timestamp: rec.WriteTS,
				RecordPos: ref.Pos,
			})
		}
		recordCount++
	}

	data := encodeIndex(entries)

	chunkDir := filepath.Join(t.dir, chunkID.String())
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return fmt.Errorf("create index dir: %w", err)
	}

	target := filepath.Join(chunkDir, indexFileName)

	tmpFile, err := os.CreateTemp(chunkDir, indexFileName+".tmp.*")
	if err != nil {
		return fmt.Errorf("create temp index: %w", err)
	}
	tmpName := tmpFile.Name()

	if err := tmpFile.Chmod(0o644); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("chmod temp index: %w", err)
	}

	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmpName)
		return fmt.Errorf("write temp index: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("close temp index: %w", err)
	}

	if err := os.Rename(tmpName, target); err != nil {
		os.Remove(tmpName)
		return fmt.Errorf("rename index: %w", err)
	}

	t.logger.Debug("time index built",
		"chunk", chunkID.String(),
		"chunk_start", meta.StartTS,
		"chunk_end", meta.EndTS,
		"chunk_duration", meta.EndTS.Sub(meta.StartTS),
		"records", recordCount,
		"entries", len(entries),
		"sparsity", t.sparsity,
		"file_size", len(data),
		"duration", time.Since(buildStart),
	)

	return nil
}
