package query

import (
	"context"
	"errors"
	"iter"
	"slices"
	"time"

	"gastrolog/internal/chunk"
)

// sortedEntry is one surviving record of an unindexed chunk, held while the
// chunk is put into query order: its ordering timestamp, its identity for
// ties, and where to read it back from.
type sortedEntry struct {
	ts    time.Time
	event chunk.EventID
	pos   uint64
}

// sortedEntryBytes is the charge per entry against the memory budget.
const sortedEntryBytes = 64

// buildSortedScanner yields a chunk that has no index for the query's ordering
// axis in canonical order anyway. It reads the chunk once, keeping the
// ordering key and position of every record that passes the time bounds and
// filters, sorts those keys, and reads the records back in that order. The
// keys are charged against the memory budget, so a chunk too large to order
// fails with a named limit instead of being served in the wrong order.
//
// Records come back Reordered: a resume token carries no position for them,
// and the next page restarts the chunk under the canonical cursor.
func (e *Engine) buildSortedScanner(ctx context.Context, cursor chunk.RecordCursor, q Query, b *scannerBuilder, meta chunk.ChunkMeta) iter.Seq2[recordWithRef, error] {
	vaultID := b.vaultID
	return func(yield func(recordWithRef, error) bool) {
		entries, err := e.collectOrderKeys(ctx, cursor, q, b, meta)
		if err != nil {
			yield(recordWithRef{VaultID: vaultID}, err)
			return
		}
		reverse := q.Reverse()
		slices.SortFunc(entries, func(a, c sortedEntry) int {
			return compareOrderKeys(a.ts, a.event, c.ts, c.event, reverse)
		})
		for _, entry := range entries {
			if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: entry.pos}); err != nil {
				yield(recordWithRef{VaultID: vaultID}, err)
				return
			}
			rec, ref, err := cursor.Next()
			if err != nil {
				yield(recordWithRef{VaultID: vaultID, Ref: ref}, err)
				return
			}
			if !yield(recordWithRef{VaultID: vaultID, Record: rec, Ref: ref, Reordered: true}, nil) {
				return
			}
		}
	}
}

// collectOrderKeys reads the chunk from its start and returns the ordering
// key and position of every record inside the ingest window that passes the
// filters, charging each against a fresh budget.
func (e *Engine) collectOrderKeys(ctx context.Context, cursor chunk.RecordCursor, q Query, b *scannerBuilder, meta chunk.ChunkMeta) ([]sortedEntry, error) {
	lower, upper := q.TimeBounds()
	budget := e.newBudget()
	if err := cursor.Seek(chunk.RecordRef{ChunkID: meta.ID, Pos: 0}); err != nil {
		return nil, err
	}
	var entries []sortedEntry
	for n := 0; ; n++ {
		if n&1023 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		rec, ref, err := cursor.Next()
		if errors.Is(err, chunk.ErrNoMoreRecords) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		if (!lower.IsZero() && rec.IngestTS.Before(lower)) || (!upper.IsZero() && !rec.IngestTS.Before(upper)) {
			continue
		}
		if !applyFilters(rec, b.filters) {
			continue
		}
		if err := budget.Charge(consumerRecordBuffer, sortedEntryBytes); err != nil {
			return nil, err
		}
		entries = append(entries, sortedEntry{ts: q.OrderBy.RecordTS(rec), event: rec.EventID, pos: ref.Pos})
	}
}
