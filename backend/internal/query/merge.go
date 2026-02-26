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

