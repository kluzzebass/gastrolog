package chunking

import (
	"slices"

	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

// OrderedIndex is a segment's records in canonical EventID order.
type OrderedIndex struct {
	sf      *segment.File
	records []record.Record // fallback when the segment has no on-disk index yet
}

// BuildOrderedIndex opens path and returns records sorted by EventID order.
func BuildOrderedIndex(path string) (*OrderedIndex, error) {
	sf, err := segment.Open(path)
	if err != nil {
		return nil, err
	}
	if sf.Header().IndexOffset > 0 {
		return &OrderedIndex{sf: sf}, nil
	}
	recs, err := sf.ReadAll()
	_ = sf.Close()
	if err != nil {
		return nil, err
	}
	slices.SortFunc(recs, func(a, b record.Record) int {
		return a.EventID.Compare(b.EventID)
	})
	return &OrderedIndex{records: recs}, nil
}

// Len returns the number of records in EventID order.
func (idx *OrderedIndex) Len() uint32 {
	if idx.sf != nil {
		return idx.sf.Header().RecordCount
	}
	return uint32(len(idx.records)) //nolint:gosec // G115: test/merge segments are bounded
}

// Slice returns records [start:start+count) in EventID order.
func (idx *OrderedIndex) Slice(start, count uint32) ([]record.Record, error) {
	end, err := spanEnd(start, count)
	if err != nil {
		return nil, err
	}
	if end > idx.Len() {
		return nil, ErrSpanBounds
	}
	if count == 0 {
		return nil, ErrEmptySpan
	}
	out := make([]record.Record, count)
	for i := range out {
		rec, err := idx.RecordAt(start + uint32(i))
		if err != nil {
			return nil, err
		}
		out[i] = rec
	}
	return out, nil
}

// RecordAt returns the record at position pos in EventID order.
func (idx *OrderedIndex) RecordAt(pos uint32) (record.Record, error) {
	if idx.sf != nil {
		return idx.sf.RecordAtEventOrder(pos)
	}
	if pos >= idx.Len() {
		return record.Record{}, ErrSpanBounds
	}
	return idx.records[pos], nil
}
