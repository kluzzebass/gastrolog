package query

import (
	"container/heap"

	"gastrolog/internal/chunk"
)

// cursorEntry represents a cursor with its current record in the merge heap.
type cursorEntry struct {
	storeID string
	chunkID chunk.ChunkID
	cursor  chunk.RecordCursor
	rec     chunk.Record
	ref     chunk.RecordRef
}

// mergeHeap is a min-heap of cursor entries ordered by IngestTS.
// For reverse queries, use mergeHeapReverse instead.
type mergeHeap []*cursorEntry

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	return h[i].rec.IngestTS.Before(h[j].rec.IngestTS)
}

func (h mergeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeap) Push(x any) {
	*h = append(*h, x.(*cursorEntry))
}

func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}

// mergeHeapReverse is a max-heap of cursor entries ordered by IngestTS (for reverse queries).
type mergeHeapReverse []*cursorEntry

func (h mergeHeapReverse) Len() int { return len(h) }

func (h mergeHeapReverse) Less(i, j int) bool {
	return h[i].rec.IngestTS.After(h[j].rec.IngestTS)
}

func (h mergeHeapReverse) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *mergeHeapReverse) Push(x any) {
	*h = append(*h, x.(*cursorEntry))
}

func (h *mergeHeapReverse) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil // avoid memory leak
	*h = old[0 : n-1]
	return x
}

// mergeCursors creates a merged iterator over multiple cursors, ordered by IngestTS.
// Each cursor should already be positioned at its starting point.
// The advance function is called to get the next record from a cursor (Next or Prev).
// Cursors are closed when exhausted or when iteration stops.
func mergeCursors(entries []*cursorEntry, reverse bool, advance func(*cursorEntry) (chunk.Record, chunk.RecordRef, error)) func(yield func(*cursorEntry, error) bool) {
	return func(yield func(*cursorEntry, error) bool) {
		if len(entries) == 0 {
			return
		}

		// Initialize heap with first record from each cursor
		var h heap.Interface
		if reverse {
			rh := make(mergeHeapReverse, 0, len(entries))
			h = &rh
		} else {
			fh := make(mergeHeap, 0, len(entries))
			h = &fh
		}

		// Track cursors to close on exit
		activeCursors := make([]*cursorEntry, 0, len(entries))
		defer func() {
			for _, e := range activeCursors {
				if e.cursor != nil {
					e.cursor.Close()
				}
			}
		}()

		// Prime the heap with first record from each cursor
		for _, e := range entries {
			rec, ref, err := advance(e)
			if err != nil {
				if err == chunk.ErrNoMoreRecords {
					e.cursor.Close()
					continue
				}
				yield(nil, err)
				return
			}
			e.rec = rec
			e.ref = ref
			heap.Push(h, e)
			activeCursors = append(activeCursors, e)
		}

		// Merge
		for h.Len() > 0 {
			e := heap.Pop(h).(*cursorEntry)

			if !yield(e, nil) {
				return
			}

			// Advance this cursor
			rec, ref, err := advance(e)
			if err != nil {
				if err == chunk.ErrNoMoreRecords {
					// Remove from active cursors
					e.cursor.Close()
					e.cursor = nil
					continue
				}
				yield(nil, err)
				return
			}
			e.rec = rec
			e.ref = ref
			heap.Push(h, e)
		}
	}
}
