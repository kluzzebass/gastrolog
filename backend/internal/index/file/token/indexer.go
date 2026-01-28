package token

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"github.com/kluzzebass/gastrolog/internal/chunk"
	"github.com/kluzzebass/gastrolog/internal/index"
	"github.com/kluzzebass/gastrolog/internal/index/token"
)

// Indexer builds a token index for sealed chunks.
// For each chunk, it maps every distinct token to the list of
// record positions where that token appears, and writes the result
// to <dir>/<chunkID>/_token.idx.
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

func (t *Indexer) Name() string {
	return "token"
}

func (t *Indexer) Build(ctx context.Context, chunkID chunk.ChunkID) error {
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

	// Single-pass scan: accumulate positions per token.
	posMap := make(map[string][]uint64)

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

		tokens := token.Simple(rec.Raw)
		seen := make(map[string]bool) // dedupe within same record
		for _, tok := range tokens {
			if !seen[tok] {
				seen[tok] = true
				posMap[tok] = append(posMap[tok], ref.Pos)
			}
		}
	}

	// Convert map to sorted slice for deterministic output.
	entries := make([]index.TokenIndexEntry, 0, len(posMap))
	for tok, positions := range posMap {
		entries = append(entries, index.TokenIndexEntry{
			Token:     tok,
			Positions: positions,
		})
	}
	slices.SortFunc(entries, func(a, b index.TokenIndexEntry) int {
		return cmp.Compare(a.Token, b.Token)
	})

	data := encodeIndex(chunkID, entries)

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
