package chunking

import (
	"iter"
	"slices"

	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

type spanCursor struct {
	seg *segment.MappedSegment
	pos uint32
	end uint32
}

func (c *spanCursor) popEntry() (segment.IndexEntry, bool, error) {
	if c.pos >= c.end {
		return segment.IndexEntry{}, false, nil
	}
	entry, err := c.seg.IndexEntryAt(c.pos)
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

// loadSpanCursors maps every referenced segment read-only (one mapping per
// distinct path, shared across spans) and returns a closer that unmaps them
// all. Mapped access replaces the previous ~3 preads per merged record, and
// skips the full-file CRC re-verification that segment.Open performs — each
// segment was already verified when it entered this node, and the built
// GLCB carries its own digest (gastrolog-1rca2d). The closer also fixes the
// long-standing leak of one open segment handle per merged segment: the
// previous OrderedIndex opens were never closed.
func loadSpanCursors(refs []SpanRef) ([]spanCursor, func(), error) {
	segments := make(map[string]*segment.MappedSegment, len(refs))
	closeAll := func() {
		for _, m := range segments {
			_ = m.Close()
		}
	}
	cursors := make([]spanCursor, len(refs))
	for i, ref := range refs {
		m, ok := segments[ref.Path]
		if !ok {
			var err error
			m, err = segment.OpenMapped(ref.Path)
			if err != nil {
				closeAll()
				return nil, nil, err
			}
			segments[ref.Path] = m
		}
		if err := ref.Span.validate(m.Len()); err != nil {
			closeAll()
			return nil, nil, err
		}
		end, _ := spanEnd(ref.Span.Start, ref.Span.Count)
		cursors[i] = spanCursor{seg: m, pos: ref.Span.Start, end: end}
	}
	return cursors, closeAll, nil
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

// MergeSpanViews yields record views from all spans merged in canonical
// EventID order. The k-way heap compares index entries (EventID + filepos)
// only; each frame is parsed when its entry wins. Views alias the span
// mappings and are valid only within the yield call — the GLCB build
// transcodes them immediately, which is the point: no per-record attrs
// map or Raw copy on the bulk path (gastrolog-11y2iv).
func MergeSpanViews(refs []SpanRef) iter.Seq2[record.View, error] {
	return func(yield func(record.View, error) bool) {
		if len(refs) == 0 {
			return
		}

		cursors, closeAll, err := loadSpanCursors(refs)
		if err != nil {
			yield(record.View{}, err)
			return
		}
		defer closeAll()

		entries, err := seedMergeEntries(cursors)
		if err != nil {
			yield(record.View{}, err)
			return
		}

		h := &mergeHeap{entries: entries}
		for i := len(h.entries)/2 - 1; i >= 0; i-- {
			h.siftDown(i)
		}

		for len(h.entries) > 0 {
			me := h.pop()
			v, err := cursors[me.cur].seg.RecordViewAtFilePos(me.entry.FilePos)
			if err != nil {
				yield(record.View{}, err)
				return
			}
			if !yield(v, nil) {
				return
			}
			entry, ok, err := cursors[me.cur].popEntry()
			if err != nil {
				yield(record.View{}, err)
				return
			}
			if ok {
				h.push(mergeEntry{entry: entry, cur: me.cur})
			}
		}
	}
}

// MergeSpanRefs is MergeSpanViews materialized into self-contained Records —
// the query path assembles results that outlive the mappings.
func MergeSpanRefs(refs []SpanRef) iter.Seq2[record.Record, error] {
	return func(yield func(record.Record, error) bool) {
		for v, err := range MergeSpanViews(refs) {
			if err != nil {
				yield(record.Record{}, err)
				return
			}
			rec, err := v.Materialize()
			if !yield(rec, err) {
				return
			}
			if err != nil {
				return
			}
		}
	}
}

// MergeRecords exhausts MergeSpanRefs (primarily for tests).
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
