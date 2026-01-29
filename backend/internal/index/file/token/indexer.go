package token

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"

	"gastrolog/internal/chunk"
	"gastrolog/internal/index"
	"gastrolog/internal/index/token"
)

// Indexer builds a token index for sealed chunks.
// For each chunk, it maps every distinct token to the list of
// record positions where that token appears, and writes the result
// to <dir>/<chunkID>/_token.idx.
//
// The indexer uses a two-pass algorithm to bound peak memory usage:
//   - Pass 1: Count occurrences of each token (map[token]count)
//   - Allocate: Create posting slices with exact capacity
//   - Pass 2: Fill posting slices without dynamic growth
//
// This ensures peak memory is proportional to distinct tokens, not total occurrences.
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

	// PASS 1: Count token occurrences.
	counts, err := t.countTokens(ctx, chunkID)
	if err != nil {
		return fmt.Errorf("pass 1 (count): %w", err)
	}

	// ALLOCATE: Create posting slices with exact capacity.
	postings := make(map[string][]uint64, len(counts))
	writeIdx := make(map[string]int, len(counts))
	for tok, count := range counts {
		postings[tok] = make([]uint64, count)
		writeIdx[tok] = 0
	}

	// PASS 2: Fill posting slices.
	if err := t.fillPostings(ctx, chunkID, postings, writeIdx); err != nil {
		return fmt.Errorf("pass 2 (fill): %w", err)
	}

	// FINALIZE: Convert to sorted entries and write index.
	entries := make([]index.TokenIndexEntry, 0, len(postings))
	for tok, positions := range postings {
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

// countTokens performs pass 1: count occurrences of each token.
// Returns map[token]count where count is the number of records containing that token.
func (t *Indexer) countTokens(ctx context.Context, chunkID chunk.ChunkID) (map[string]uint32, error) {
	cursor, err := t.manager.OpenCursor(chunkID)
	if err != nil {
		return nil, fmt.Errorf("open cursor: %w", err)
	}
	defer cursor.Close()

	counts := make(map[string]uint32)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		rec, _, err := cursor.Next()
		if err != nil {
			if err == chunk.ErrNoMoreRecords {
				break
			}
			return nil, fmt.Errorf("read record: %w", err)
		}

		tokens := token.Simple(rec.Raw)
		seen := make(map[string]bool, len(tokens))
		for _, tok := range tokens {
			if !seen[tok] {
				seen[tok] = true
				counts[tok]++
			}
		}
	}

	return counts, nil
}

// fillPostings performs pass 2: fill posting slices with record positions.
// postings must be pre-allocated with exact capacity from pass 1.
// writeIdx tracks the next write position for each token.
func (t *Indexer) fillPostings(ctx context.Context, chunkID chunk.ChunkID, postings map[string][]uint64, writeIdx map[string]int) error {
	cursor, err := t.manager.OpenCursor(chunkID)
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

		tokens := token.Simple(rec.Raw)
		seen := make(map[string]bool, len(tokens))
		for _, tok := range tokens {
			if !seen[tok] {
				seen[tok] = true
				positions := postings[tok]
				idx := writeIdx[tok]
				if idx >= len(positions) {
					return fmt.Errorf("write index overflow for token %q: idx=%d, len=%d", tok, idx, len(positions))
				}
				positions[idx] = ref.Pos
				writeIdx[tok] = idx + 1
			}
		}
	}

	return nil
}
