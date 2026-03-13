package query

import (
	"gastrolog/internal/chunk"

	"github.com/google/uuid"
)

// cursorEntry represents a cursor with its current record in the merge heap.
type cursorEntry struct {
	vaultID uuid.UUID
	chunkID chunk.ChunkID
	rec     chunk.Record
	ref     chunk.RecordRef
}

// tsHeap is a heap of cursor entries ordered by a configurable timestamp field.
// The less function determines both the timestamp field and direction (min/max).
type tsHeap struct {
	entries []*cursorEntry
	less    func(a, b *cursorEntry) bool
}

// newTSHeap creates a heap that orders entries by the given OrderBy field.
// When reverse is true, the heap yields newest-first (max-heap).
func newTSHeap(orderBy OrderBy, reverse bool, capacity int) *tsHeap {
	var less func(a, b *cursorEntry) bool
	switch orderBy {
	case OrderByIngestTS:
		if reverse {
			less = func(a, b *cursorEntry) bool { return a.rec.IngestTS.After(b.rec.IngestTS) }
		} else {
			less = func(a, b *cursorEntry) bool { return a.rec.IngestTS.Before(b.rec.IngestTS) }
		}
	case OrderBySourceTS:
		if reverse {
			less = func(a, b *cursorEntry) bool { return a.rec.SourceTS.After(b.rec.SourceTS) }
		} else {
			less = func(a, b *cursorEntry) bool { return a.rec.SourceTS.Before(b.rec.SourceTS) }
		}
	case OrderByWriteTS:
		if reverse {
			less = func(a, b *cursorEntry) bool { return a.rec.WriteTS.After(b.rec.WriteTS) }
		} else {
			less = func(a, b *cursorEntry) bool { return a.rec.WriteTS.Before(b.rec.WriteTS) }
		}
	}

	return &tsHeap{
		entries: make([]*cursorEntry, 0, capacity),
		less:    less,
	}
}

func (h *tsHeap) Len() int            { return len(h.entries) }
func (h *tsHeap) Less(i, j int) bool   { return h.less(h.entries[i], h.entries[j]) }
func (h *tsHeap) Swap(i, j int)        { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

func (h *tsHeap) Push(x any) {
	h.entries = append(h.entries, x.(*cursorEntry))
}

func (h *tsHeap) Pop() any {
	old := h.entries
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	h.entries = old[0 : n-1]
	return x
}
