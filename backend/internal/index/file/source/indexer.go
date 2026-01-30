package source

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
)

// Indexer builds a source index for sealed chunks.
// For each chunk, it maps every distinct SourceID to the list of
// record positions where that source appears, and writes the result
// to <dir>/<chunkID>/_source.idx.
type Indexer struct {
	dir     string
	manager chunk.ChunkManager
}

func NewIndexer(dir string, manager chunk.ChunkManager) *Indexer {
	return &Indexer{
		dir:     dir,
		manager: manager,
	}
}

func (s *Indexer) Name() string {
	return "source"
}

func (s *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
	meta, err := s.manager.Meta(chunkID)
	if err != nil {
		return fmt.Errorf("get chunk meta: %w", err)
	}
	if !meta.Sealed {
		return chunk.ErrChunkNotSealed
	}

	cursor, err := s.manager.OpenCursor(chunkID)
	if err != nil {
		return fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	// Single-pass scan: accumulate positions per source.
	posMap := make(map[chunk.SourceID][]uint64)

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

		posMap[rec.SourceID] = append(posMap[rec.SourceID], ref.Pos)
	}

	// Convert map to sorted slice.
	entries := make([]index.SourceIndexEntry, 0, len(posMap))
	for sid, positions := range posMap {
		entries = append(entries, index.SourceIndexEntry{
			SourceID:  sid,
			Positions: positions,
		})
	}

	data := encodeIndex(chunkID, entries)

	chunkDir := filepath.Join(s.dir, chunkID.String())
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

	return nil
}
