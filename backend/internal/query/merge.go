package query

import (
	"iter"

	"gastrolog/internal/chunk"
	"gastrolog/internal/glid"
)

// mergeOrdered interleaves two record streams that are each already ordered by
// orderBy into a single stream in that same order.
//
// Ties yield the first stream's record. Which side wins a tie is arbitrary —
// records sharing a timestamp have no defined relative order — but it must be
// deterministic, and the server's own local/remote merge breaks ties the other
// way, so neither is the cluster's canonical tie order. Nothing may depend on
// which of two equal-timestamp records comes first.
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
				at, bt := orderBy.RecordTS(aRec), orderBy.RecordTS(bRec)
				if reverse {
					takeA = !at.Before(bt)
				} else {
					takeA = !at.After(bt)
				}
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
	}

	return &tsHeap{
		entries: make([]*cursorEntry, 0, capacity),
		less:    less,
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
