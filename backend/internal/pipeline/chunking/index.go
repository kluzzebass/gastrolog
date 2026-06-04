package chunking

import (
	"slices"

	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

// OrderedIndex is a segment's records sorted by canonical EventID order.
// This stand-in scans and sorts until per-segment btree indexes land (design-notes §36).
type OrderedIndex struct {
	records []record.Record
}

// BuildOrderedIndex opens path and returns records sorted by EventID.
func BuildOrderedIndex(path string) (*OrderedIndex, error) {
	sf, err := segment.Open(path)
	if err != nil {
		return nil, err
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
	copy(out, idx.records[start:end])
	return out, nil
}

// RecordAt returns the record at position pos in EventID order.
func (idx *OrderedIndex) RecordAt(pos uint32) (record.Record, error) {
	if pos >= idx.Len() {
		return record.Record{}, ErrSpanBounds
	}
	return idx.records[pos], nil
}
