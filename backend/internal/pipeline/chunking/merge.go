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
	var entries []mergeEntry
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

func minMergeEntryIndex(entries []mergeEntry) int {
	minIdx := 0
	for i := 1; i < len(entries); i++ {
		if entries[i].entry.EventID.Less(entries[minIdx].entry.EventID) {
			minIdx = i
		}
	}
	return minIdx
}

func advanceMergeEntry(entries []mergeEntry, at int, cursors []spanCursor) ([]mergeEntry, error) {
	entry, ok, err := cursors[entries[at].cur].popEntry()
	if err != nil {
		return nil, err
	}
	if ok {
		entries[at].entry = entry
		return entries, nil
	}
	entries[at] = entries[len(entries)-1]
	return entries[:len(entries)-1], nil
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

		for len(entries) > 0 {
			at := minMergeEntryIndex(entries)
			rec, err := cursors[entries[at].cur].idx.RecordAtFilePos(entries[at].entry.FilePos)
			if err != nil {
				yield(record.Record{}, err)
				return
			}
			if !yield(rec, nil) {
				return
			}
			entries, err = advanceMergeEntry(entries, at, cursors)
			if err != nil {
				yield(record.Record{}, err)
				return
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
