package chunking

import (
	"fmt"
	"time"

	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

// OrderedIndex is a finalized segment's records in canonical EventID order.
type OrderedIndex struct {
	sf *segment.File
}

// BuildOrderedIndex opens a completed segment and uses its on-disk EventID index.
// Segments without an index are not eligible for chunking.
func BuildOrderedIndex(path string) (*OrderedIndex, error) {
	sf, err := segment.Open(path)
	if err != nil {
		return nil, err
	}
	if sf.Header().IndexOffset == 0 {
		_ = sf.Close()
		return nil, fmt.Errorf("%w: %s", segment.ErrNoIndex, path)
	}
	return &OrderedIndex{sf: sf}, nil
}

// Len returns the number of records in EventID order.
func (idx *OrderedIndex) Len() uint32 {
	return idx.sf.Header().RecordCount
}

// EntryAt returns the index entry at position pos in EventID order.
func (idx *OrderedIndex) EntryAt(pos uint32) (segment.IndexEntry, error) {
	return idx.sf.IndexEntryAt(pos)
}

// RecordAtFilePos decodes the record at a frame offset from the index entry.
func (idx *OrderedIndex) RecordAtFilePos(filePos uint32) (record.Record, error) {
	return idx.sf.ReadRecordAtFilePos(filePos)
}

// RecordAt returns the record at position pos in EventID order.
func (idx *OrderedIndex) RecordAt(pos uint32) (record.Record, error) {
	return idx.sf.RecordAtEventOrder(pos)
}

// FindSourceStartPosition returns the first event-order position at or after start
// in the segment's sparse SourceTS index.
func (idx *OrderedIndex) FindSourceStartPosition(start time.Time) (uint32, bool, error) {
	return idx.sf.FindSourceStartPosition(start)
}

// TrimSpanForSourceStart narrows span to records with SourceTS >= start when indexed.
// When start is before all indexed sources or the index is empty, the span is unchanged.
func TrimSpanForSourceStart(span Span, start time.Time, idx *OrderedIndex) (Span, error) {
	pos, ok, err := idx.FindSourceStartPosition(start)
	if err != nil {
		return Span{}, err
	}
	if !ok || pos <= span.Start {
		return span, nil
	}
	end, err := spanEnd(span.Start, span.Count)
	if err != nil {
		return Span{}, err
	}
	if pos >= end {
		return Span{}, ErrEmptySpan
	}
	return Span{
		SegmentID: span.SegmentID,
		Start:     pos,
		Count:     end - pos,
	}, nil
}

// Close closes the underlying segment file.
func (idx *OrderedIndex) Close() error {
	if idx.sf == nil {
		return nil
	}
	return idx.sf.Close()
}

// FrameByteLenAt returns the on-disk frame byte length at EventID-order position pos.
func (idx *OrderedIndex) FrameByteLenAt(pos uint32) (uint64, error) {
	entry, err := idx.sf.IndexEntryAt(pos)
	if err != nil {
		return 0, err
	}
	n, err := idx.sf.FrameByteLen(entry.FilePos)
	if err != nil {
		return 0, err
	}
	return uint64(n), nil
}

// RecordSliceBytes returns the on-disk frame byte length for the record at pos.
func (idx *OrderedIndex) RecordSliceBytes(pos uint32) (uint64, error) {
	return idx.FrameByteLenAt(pos)
}
