package query

import (
	"context"
	"errors"
	"iter"
	"time"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
	"gastrolog/internal/index"
)

func tsIndexViewForChunk(cm chunk.ChunkManager, im index.IndexManager, orderBy OrderBy) tsIndexView {
	imView := tsIndexViewForOrder(im, orderBy)
	if orderBy == OrderBySourceTS {
		return imView
	}
	rankCM, ok := cm.(chunk.IngestTSRankView)
	if !ok {
		return imView
	}
	return tsIndexView{
		lenFn: func(chunkID chunk.ChunkID) (uint64, error) {
			n, err := imView.lenFn(chunkID)
			if err == nil {
				return n, nil
			}
			if !errors.Is(err, index.ErrIndexNotFound) {
				return 0, err
			}
			return rankCM.IngestTSRankLen(chunkID)
		},
		entryAt: func(chunkID chunk.ChunkID, rank uint64) (index.TSEntry, error) {
			entry, err := imView.entryAt(chunkID, rank)
			if err == nil {
				return entry, nil
			}
			if !errors.Is(err, index.ErrIndexNotFound) {
				return index.TSEntry{}, err
			}
			ts, pos, err := rankCM.IngestTSRankAt(chunkID, rank)
			if err != nil {
				return index.TSEntry{}, err
			}
			return index.TSEntry{TS: ts, Pos: pos}, nil
		},
		findRank: func(chunkID chunk.ChunkID, ts time.Time) (uint64, bool, error) {
			rank, found, err := imView.findRank(chunkID, ts)
			if err == nil {
				return rank, found, nil
			}
			if !errors.Is(err, index.ErrIndexNotFound) {
				return 0, false, err
			}
			return rankCM.FindIngestTSRank(chunkID, ts)
		},
	}
}

// tsIndexView provides rank-based access to a sealed chunk's mmap'd TS index.
type tsIndexView struct {
	lenFn    func(chunk.ChunkID) (uint64, error)
	entryAt  func(chunk.ChunkID, uint64) (index.TSEntry, error)
	findRank func(chunk.ChunkID, time.Time) (uint64, bool, error)
}

func chunkHasTSIndex(view tsIndexView, chunkID chunk.ChunkID) bool {
	_, err := view.lenFn(chunkID)
	return err == nil
}

func tsIndexViewForOrder(im index.IndexManager, orderBy OrderBy) tsIndexView {
	switch orderBy { //nolint:exhaustive // IngestTS is the default
	case OrderBySourceTS:
		return tsIndexView{
			lenFn:   im.SourceIndexLen,
			entryAt: im.SourceIndexEntryAt,
			findRank: func(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
				return im.FindSourceEntryIndex(id, ts)
			},
		}
	default:
		return tsIndexView{
			lenFn:   im.IngestIndexLen,
			entryAt: im.IngestIndexEntryAt,
			findRank: func(id chunk.ChunkID, ts time.Time) (uint64, bool, error) {
				return im.FindIngestEntryIndex(id, ts)
			},
		}
	}
}

// tsIndexRankBounds returns the half-open rank range [start, end) for a query's
// time bounds. ok is false when the range is empty.
func tsIndexRankBounds(view tsIndexView, chunkID chunk.ChunkID, q Query) (start, end uint64, ok bool, err error) {
	n, err := view.lenFn(chunkID)
	if err != nil {
		return 0, 0, false, err
	}
	if n == 0 {
		return 0, 0, false, nil
	}

	start = 0
	lower, upper := q.TimeBounds()
	if !lower.IsZero() {
		r, found, err := view.findRank(chunkID, lower)
		if err != nil {
			return 0, 0, false, err
		}
		if !found {
			return 0, 0, false, nil
		}
		start = r
	}

	end = n
	if !upper.IsZero() {
		r, found, err := view.findRank(chunkID, upper)
		if err != nil {
			return 0, 0, false, err
		}
		if found {
			end = r
		}
	}

	if start >= end {
		return 0, 0, false, nil
	}
	return start, end, true, nil
}

// buildMmapTSIndexScanner walks the mmap'd TS index by rank, seeking to each
// physical position on demand. Returns ErrIndexNotFound when the index is
// missing — sealed-chunk search must fail loudly; there is no heap fallback.
func buildMmapTSIndexScanner(
	ctx context.Context,
	cursor chunk.RecordCursor,
	q Query,
	b *scannerBuilder,
	meta chunk.ChunkMeta,
	cm chunk.ChunkManager,
	im index.IndexManager,
) (iter.Seq2[recordWithRef, error], error) {
	view := tsIndexViewForChunk(cm, im, q.OrderBy)
	if _, err := view.lenFn(meta.ID); err != nil {
		return nil, err
	}

	start, end, ok, err := tsIndexRankBounds(view, meta.ID, q)
	if err != nil {
		return nil, err
	}
	if !ok {
		return emptyScanner(), nil
	}

	if b.positions != nil {
		return buildMmapTSIndexFilteredScanner(ctx, cursor, q, b, meta, view, start, end), nil
	}
	return buildMmapTSIndexRankScanner(ctx, cursor, q, b, meta, view, start, end), nil
}

func buildMmapTSIndexRankScanner(
	ctx context.Context,
	cursor chunk.RecordCursor,
	q Query,
	b *scannerBuilder,
	meta chunk.ChunkMeta,
	view tsIndexView,
	start, end uint64,
) iter.Seq2[recordWithRef, error] {
	chunkID := meta.ID
	vaultID := b.vaultID
	filters := b.filters
	minPos := b.minPos
	hasMinPos := b.hasMinPos

	return func(yield func(recordWithRef, error) bool) {
		if q.Reverse() {
			for rank := end; rank > start; rank-- {
				if err := yieldMmapTSIndexRank(ctx, yield, cursor, chunkID, vaultID, view, rank-1, q, filters, minPos, hasMinPos); err != nil {
					return
				}
			}
			return
		}
		for rank := start; rank < end; rank++ {
			if err := yieldMmapTSIndexRank(ctx, yield, cursor, chunkID, vaultID, view, rank, q, filters, minPos, hasMinPos); err != nil {
				return
			}
		}
	}
}

func buildMmapTSIndexFilteredScanner(
	ctx context.Context,
	cursor chunk.RecordCursor,
	q Query,
	b *scannerBuilder,
	meta chunk.ChunkMeta,
	view tsIndexView,
	start, end uint64,
) iter.Seq2[recordWithRef, error] {
	posSet := make(map[uint64]struct{}, len(b.positions))
	for _, p := range b.positions {
		posSet[p] = struct{}{}
	}

	chunkID := meta.ID
	vaultID := b.vaultID
	filters := b.filters
	minPos := b.minPos
	hasMinPos := b.hasMinPos

	return func(yield func(recordWithRef, error) bool) {
		if q.Reverse() {
			for rank := end; rank > start; rank-- {
				if err := yieldMmapTSIndexRankFiltered(ctx, yield, cursor, chunkID, vaultID, view, rank-1, q, posSet, filters, minPos, hasMinPos); err != nil {
					return
				}
			}
			return
		}
		for rank := start; rank < end; rank++ {
			if err := yieldMmapTSIndexRankFiltered(ctx, yield, cursor, chunkID, vaultID, view, rank, q, posSet, filters, minPos, hasMinPos); err != nil {
				return
			}
		}
	}
}

func mmapRecordVisible(ts, lower, upper, resumeTS time.Time, reverse bool) bool {
	if !lower.IsZero() && ts.Before(lower) {
		return false
	}
	if !upper.IsZero() && !ts.Before(upper) {
		return false
	}
	if !resumeTS.IsZero() {
		if reverse && !ts.Before(resumeTS) {
			return false
		}
		if !reverse && !resumeTS.Before(ts) {
			return false
		}
	}
	return true
}

func yieldMmapTSIndexRank(
	ctx context.Context,
	yield func(recordWithRef, error) bool,
	cursor chunk.RecordCursor,
	chunkID chunk.ChunkID,
	vaultID glid.GLID,
	view tsIndexView,
	rank uint64,
	q Query,
	filters []recordFilter,
	minPos uint64,
	hasMinPos bool,
) error {
	if err := ctx.Err(); err != nil {
		yield(recordWithRef{VaultID: vaultID}, err)
		return err
	}

	entry, err := view.entryAt(chunkID, rank)
	if err != nil {
		yield(recordWithRef{VaultID: vaultID}, err)
		return err
	}
	pos := uint64(entry.Pos)
	if hasMinPos && pos < minPos {
		return nil
	}

	rec, ref, err := seekAndRead(cursor, chunkID, pos)
	if errors.Is(err, chunk.ErrNoMoreRecords) {
		return err
	}
	if err != nil {
		yield(recordWithRef{VaultID: vaultID, Ref: ref}, err)
		return err
	}
	if !applyFilters(rec, filters) {
		return nil
	}
	lower, upper := q.TimeBounds()
	if !mmapRecordVisible(q.OrderBy.RecordTS(rec), lower, upper, q.ResumeTS, q.Reverse()) {
		return nil
	}
	if !yield(recordWithRef{VaultID: vaultID, Record: rec, Ref: ref, Reordered: true}, nil) {
		return errScanStopped
	}
	return nil
}

func yieldMmapTSIndexRankFiltered(
	ctx context.Context,
	yield func(recordWithRef, error) bool,
	cursor chunk.RecordCursor,
	chunkID chunk.ChunkID,
	vaultID glid.GLID,
	view tsIndexView,
	rank uint64,
	q Query,
	posSet map[uint64]struct{},
	filters []recordFilter,
	minPos uint64,
	hasMinPos bool,
) error {
	entry, err := view.entryAt(chunkID, rank)
	if err != nil {
		yield(recordWithRef{VaultID: vaultID}, err)
		return err
	}
	pos := uint64(entry.Pos)
	if _, ok := posSet[pos]; !ok {
		return nil
	}
	return yieldMmapTSIndexRank(ctx, yield, cursor, chunkID, vaultID, view, rank, q, filters, minPos, hasMinPos)
}

// errScanStopped is returned internally when yield returns false.
var errScanStopped = errors.New("scan stopped")
