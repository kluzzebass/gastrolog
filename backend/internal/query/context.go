package query

import (
	"context"
	"errors"
	"slices"

	"gastrolog/internal/chunk"
)

// gatherContextBefore collects up to n records before the anchor position.
// Returns records in the order they should be yielded (oldest first for forward,
// newest first for reverse).
func (e *Engine) gatherContextBefore(ctx context.Context, chunksAsc []chunk.ChunkMeta, anchor chunk.RecordRef, n int, reverse bool) ([]chunk.Record, error) {
	if n <= 0 {
		return nil, nil
	}

	// Find the chunk containing the anchor.
	chunkIdx := -1
	for i, m := range chunksAsc {
		if m.ID == anchor.ChunkID {
			chunkIdx = i
			break
		}
	}
	if chunkIdx < 0 {
		return nil, nil // chunk not found, no context
	}

	var collected []chunk.Record

	// Start from anchor chunk, walk backward.
	for ci := chunkIdx; ci >= 0 && len(collected) < n; ci-- {
		meta := chunksAsc[ci]

		cursor, err := e.chunks.OpenCursor(meta.ID)
		if err != nil {
			return nil, err
		}

		if ci == chunkIdx {
			// Seek to anchor position.
			if err := cursor.Seek(anchor); err != nil {
				cursor.Close()
				return nil, err
			}
		} else {
			// Seek to end of previous chunk.
			if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: uint64(meta.RecordCount)}); err != nil {
				cursor.Close()
				return nil, err
			}
		}

		// Walk backward collecting records.
		for len(collected) < n {
			rec, _, err := cursor.Prev()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				cursor.Close()
				return nil, err
			}
			collected = append(collected, rec)
		}

		cursor.Close()
	}

	// Reverse so oldest is first (for forward mode).
	// For reverse mode, we want newest first, so reverse again.
	slices.Reverse(collected)
	if reverse {
		slices.Reverse(collected)
	}

	return collected, nil
}

// gatherContextAfter collects up to n records after the anchor position.
// Returns records in the order they should be yielded (oldest first for forward,
// newest first for reverse).
func (e *Engine) gatherContextAfter(ctx context.Context, chunksAsc []chunk.ChunkMeta, anchor chunk.RecordRef, n int, reverse bool) ([]chunk.Record, error) {
	if n <= 0 {
		return nil, nil
	}

	// Find the chunk containing the anchor.
	chunkIdx := -1
	for i, m := range chunksAsc {
		if m.ID == anchor.ChunkID {
			chunkIdx = i
			break
		}
	}
	if chunkIdx < 0 {
		return nil, nil // chunk not found, no context
	}

	var collected []chunk.Record

	// Start from anchor chunk, walk forward.
	for ci := chunkIdx; ci < len(chunksAsc) && len(collected) < n; ci++ {
		meta := chunksAsc[ci]

		cursor, err := e.chunks.OpenCursor(meta.ID)
		if err != nil {
			return nil, err
		}

		if ci == chunkIdx {
			// Seek to anchor position and skip it.
			if err := cursor.Seek(anchor); err != nil {
				cursor.Close()
				return nil, err
			}
			// Skip the anchor record itself.
			if _, _, err := cursor.Next(); err != nil && !errors.Is(err, chunk.ErrNoMoreRecords) {
				cursor.Close()
				return nil, err
			}
		}
		// For subsequent chunks, cursor starts at beginning by default.

		// Walk forward collecting records.
		for len(collected) < n {
			rec, _, err := cursor.Next()
			if errors.Is(err, chunk.ErrNoMoreRecords) {
				break
			}
			if err != nil {
				cursor.Close()
				return nil, err
			}
			collected = append(collected, rec)
		}

		cursor.Close()
	}

	// For reverse mode, we want newest first.
	if reverse {
		slices.Reverse(collected)
	}

	return collected, nil
}
