package query

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/google/uuid"

	"gastrolog/internal/chunk"
)

// ContextRef identifies the anchor record.
type ContextRef struct {
	VaultID uuid.UUID
	ChunkID chunk.ChunkID
	Pos     uint64
}

// ContextResult holds the anchor and surrounding records.
type ContextResult struct {
	Anchor chunk.Record
	Before []chunk.Record
	After  []chunk.Record
}

// ReadRecord reads a single record by vault, chunk, and position.
func (e *Engine) ReadRecord(_ context.Context, vaultID uuid.UUID, chunkID chunk.ChunkID, pos uint64) (chunk.Record, error) {
	cm, _ := e.getVaultManagers(vaultID)
	if cm == nil {
		return chunk.Record{}, fmt.Errorf("vault %q not found", vaultID)
	}
	cursor, err := cm.OpenCursor(chunkID)
	if err != nil {
		return chunk.Record{}, fmt.Errorf("chunk %s not found: %w", chunkID, err)
	}
	ref := chunk.RecordRef{ChunkID: chunkID, Pos: pos}
	if err := cursor.Seek(ref); err != nil {
		_ = cursor.Close()
		return chunk.Record{}, fmt.Errorf("seek to position %d: %w", pos, err)
	}
	rec, _, err := cursor.Next()
	if err != nil {
		_ = cursor.Close()
		return chunk.Record{}, fmt.Errorf("read record: %w", err)
	}
	_ = cursor.Close()
	rec.VaultID = vaultID
	rec.Ref = ref
	return rec, nil
}

// GetContext returns records surrounding a specific record, across all vaults.
// It reads the anchor record directly, then uses time-windowed multi-vault
// searches to find nearby records.
func (e *Engine) GetContext(ctx context.Context, ref ContextRef, before, after int) (*ContextResult, error) {
	// Defaults and caps.
	if before == 0 {
		before = 5
	}
	if after == 0 {
		after = 5
	}
	if before > 50 {
		before = 50
	}
	if after > 50 {
		after = 50
	}

	anchorRec, err := e.ReadRecord(ctx, ref.VaultID, ref.ChunkID, ref.Pos)
	if err != nil {
		return nil, err
	}

	anchorTS := anchorRec.IngestTS

	isAnchor := func(rec chunk.Record) bool {
		return rec.VaultID == ref.VaultID &&
			rec.Ref.ChunkID == ref.ChunkID &&
			rec.Ref.Pos == ref.Pos
	}

	// Fetch records before: search backward from anchor timestamp.
	// Request extra to account for deduplication of the anchor itself.
	beforeQuery := Query{
		End:       anchorTS,
		Limit:     before + 1,
		IsReverse: true,
	}
	beforeIter, _ := e.Search(ctx, beforeQuery, nil)
	var beforeRecs []chunk.Record
	for rec, err := range beforeIter {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			return nil, err
		}
		if isAnchor(rec) {
			continue
		}
		beforeRecs = append(beforeRecs, rec)
		if len(beforeRecs) >= before {
			break
		}
	}
	// beforeRecs is newest-first (reverse search), flip to oldest-first.
	slices.Reverse(beforeRecs)

	// Fetch records after: search forward from anchor timestamp.
	afterQuery := Query{
		Start: anchorTS,
		Limit: after + 1,
	}
	afterIter, _ := e.Search(ctx, afterQuery, nil)
	var afterRecs []chunk.Record
	for rec, err := range afterIter {
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil, err
			}
			return nil, err
		}
		if isAnchor(rec) {
			continue
		}
		afterRecs = append(afterRecs, rec)
		if len(afterRecs) >= after {
			break
		}
	}

	return &ContextResult{
		Anchor: anchorRec,
		Before: beforeRecs,
		After:  afterRecs,
	}, nil
}

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
				_ = cursor.Close()
				return nil, err
			}
		} else {
			// Seek to end of previous chunk.
			if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: uint64(meta.RecordCount)}); err != nil { //nolint:gosec // G115: RecordCount is always non-negative
				_ = cursor.Close()
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
				_ = cursor.Close()
				return nil, err
			}
			collected = append(collected, rec)
		}

		_ = cursor.Close()
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
				_ = cursor.Close()
				return nil, err
			}
			// Skip the anchor record itself.
			if _, _, err := cursor.Next(); err != nil && !errors.Is(err, chunk.ErrNoMoreRecords) {
				_ = cursor.Close()
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
				_ = cursor.Close()
				return nil, err
			}
			collected = append(collected, rec)
		}

		_ = cursor.Close()
	}

	// For reverse mode, we want newest first.
	if reverse {
		slices.Reverse(collected)
	}

	return collected, nil
}
