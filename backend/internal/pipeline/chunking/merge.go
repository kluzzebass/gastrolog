package chunking

import (
	"iter"
	"slices"

	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

type spanCursor struct {
	idx  *OrderedIndex
	pos  uint32
	end  uint32
}

func (c *spanCursor) popEntry() (segment.IndexEntry, bool, error) {
	if c.pos >= c.end {
		return segment.IndexEntry{}, false, nil
	}
	entry, err := c.idx.EntryAt(c.pos)
	if err != nil {
		return segment.IndexEntry{}, false, err
	}
	c.pos++
	return entry, true, nil
}

type mergeEntry struct {
	entry segment.IndexEntry
	cur   int
}

type mergeHeap struct {
	entries []mergeEntry
}

func (h *mergeHeap) less(i, j int) bool {
	return h.entries[i].entry.EventID.Less(h.entries[j].entry.EventID)
}

func (h *mergeHeap) siftUp(i int) {
	for i > 0 {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h.entries[i], h.entries[parent] = h.entries[parent], h.entries[i]
		i = parent
	}
}

func (h *mergeHeap) siftDown(i int) {
	for {
		left := 2*i + 1
		if left >= len(h.entries) {
			break
		}
		smallest := left
		if right := left + 1; right < len(h.entries) && h.less(right, left) {
			smallest = right
		}
		if !h.less(smallest, i) {
			break
		}
		h.entries[i], h.entries[smallest] = h.entries[smallest], h.entries[i]
		i = smallest
	}
}

func (h *mergeHeap) push(e mergeEntry) {
	h.entries = append(h.entries, e)
	h.siftUp(len(h.entries) - 1)
}

func (h *mergeHeap) pop() mergeEntry {
	n := len(h.entries)
	top := h.entries[0]
	if n == 1 {
		h.entries = h.entries[:0]
		return top
	}
	h.entries[0] = h.entries[n-1]
	h.entries[n-1] = mergeEntry{}
	h.entries = h.entries[:n-1]
	h.siftDown(0)
	return top
}

func loadSpanCursors(refs []SpanRef) ([]spanCursor, error) {
	indexes := make(map[string]*OrderedIndex, len(refs))
	cursors := make([]spanCursor, len(refs))
	for i, ref := range refs {
		idx, ok := indexes[ref.Path]
		if !ok {
			var err error
			idx, err = BuildOrderedIndex(ref.Path)
			if err != nil {
				return nil, err
			}
			indexes[ref.Path] = idx
		}
		if err := ref.Span.validate(idx.Len()); err != nil {
			return nil, err
		}
		end, _ := spanEnd(ref.Span.Start, ref.Span.Count)
		cursors[i] = spanCursor{idx: idx, pos: ref.Span.Start, end: end}
	}
	return cursors, nil
}

func seedMergeEntries(cursors []spanCursor) ([]mergeEntry, error) {
	entries := make([]mergeEntry, 0, len(cursors))
	for i := range cursors {
		entry, ok, err := cursors[i].popEntry()
		if err != nil {
			return nil, err
		}
		if ok {
			entries = append(entries, mergeEntry{entry: entry, cur: i})
		}
	}
	return entries, nil
}

// MergeSpanRefs yields records from all spans merged in canonical EventID order.
// The k-way heap compares index entries (EventID + filepos) only; each frame is
// decoded when its entry wins and is yielded (design-notes §37).
func MergeSpanRefs(refs []SpanRef) iter.Seq2[record.Record, error] {
	return func(yield func(record.Record, error) bool) {
		if len(refs) == 0 {
			return
		}

		cursors, err := loadSpanCursors(refs)
		if err != nil {
			yield(record.Record{}, err)
			return
		}

		entries, err := seedMergeEntries(cursors)
		if err != nil {
			yield(record.Record{}, err)
			return
		}

		h := &mergeHeap{entries: entries}
		for i := len(h.entries)/2 - 1; i >= 0; i-- {
			h.siftDown(i)
		}

		for len(h.entries) > 0 {
			me := h.pop()
			rec, err := cursors[me.cur].idx.RecordAtFilePos(me.entry.FilePos)
			if err != nil {
				yield(record.Record{}, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
			entry, ok, err := cursors[me.cur].popEntry()
			if err != nil {
				yield(record.Record{}, err)
				return
			}
			if ok {
				h.push(mergeEntry{entry: entry, cur: me.cur})
			}
		}
	}
}

// MergeRecords materializes MergeSpanRefs (primarily for tests).
func MergeRecords(refs []SpanRef) ([]record.Record, error) {
	var out []record.Record
	for rec, err := range MergeSpanRefs(refs) {
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// IsSortedByEventID reports whether records are in non-decreasing EventID order.
func IsSortedByEventID(recs []record.Record) bool {
	return slices.IsSortedFunc(recs, func(a, b record.Record) int {
		return a.EventID.Compare(b.EventID)
	})
}
