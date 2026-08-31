package query

import (
	"iter"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// mergeOrdered interleaves two record streams that are each already in
// OrderBy.CompareRecords order into a single stream in that same order. Which
// stream a record arrived on never affects where it lands.
//
// A stream is advanced only after its current record has been consumed, so a
// source that reuses its record buffer between yields cannot overwrite a
// record the consumer still holds.
func mergeOrdered(a, b iter.Seq2[chunk.Record, error], orderBy OrderBy, reverse bool) iter.Seq2[chunk.Record, error] {
	return func(yield func(chunk.Record, error) bool) {
		aNext, aStop := iter.Pull2(a)
		defer aStop()
		bNext, bStop := iter.Pull2(b)
		defer bStop()

		aRec, aErr, aOK := aNext()
		bRec, bErr, bOK := bNext()
		for aOK || bOK {
			if aOK && aErr != nil {
				yield(chunk.Record{}, aErr)
				return
			}
			if bOK && bErr != nil {
				yield(chunk.Record{}, bErr)
				return
			}

			takeA := aOK
			if aOK && bOK {
				takeA = orderBy.CompareRecords(aRec, bRec, reverse) <= 0
			}
			if takeA {
				if !yield(aRec, nil) {
					return
				}
				aRec, aErr, aOK = aNext()
				continue
			}
			if !yield(bRec, nil) {
				return
			}
			bRec, bErr, bOK = bNext()
		}
	}
}

// cursorEntry represents a cursor with its current record in the merge heap.
type cursorEntry struct {
	vaultID   glid.GLID
	chunkID   chunk.ChunkID
	rec       chunk.Record
	ref       chunk.RecordRef
	reordered bool // true when chunk was scanned without TS index (resume by IngestTS, not position)
}

// tsHeap is a heap of cursor entries in the cluster's canonical record order.
type tsHeap struct {
	entries []*cursorEntry
	less    func(a, b *cursorEntry) bool
}

// newTSHeap creates a heap that orders entries by OrderBy.CompareRecords.
// When reverse is true, the heap yields newest-first (max-heap).
//
// This is a node's fan-in across its own vaults and chunks, and it uses the
// same order as the merges above it: a tie must not resolve by heap shape,
// or a record's rank would depend on how many vaults happened to be local.
func newTSHeap(orderBy OrderBy, reverse bool, capacity int) *tsHeap {
	return &tsHeap{
		entries: make([]*cursorEntry, 0, capacity),
		less: func(a, b *cursorEntry) bool {
			return orderBy.CompareRecords(a.rec, b.rec, reverse) < 0
		},
	}
}

func (h *tsHeap) Len() int           { return len(h.entries) }
func (h *tsHeap) Less(i, j int) bool { return h.less(h.entries[i], h.entries[j]) }
func (h *tsHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }

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
