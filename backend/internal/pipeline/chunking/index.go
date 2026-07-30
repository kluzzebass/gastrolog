package chunking

import (
	"fmt"
	"slices"
	"time"

	"gastrolog/internal/pipeline/segment"
	"gastrolog/internal/record"
)

// OrderedIndex is a finalized segment's records in canonical EventID order.
// Not safe for concurrent use: ViewAt reuses an internal scratch buffer
// (matching the planner's single-goroutine access pattern).
type OrderedIndex struct {
	sf *segment.File
	// scratch backs ViewAt frame reads, reused across calls so bounds
	// scans allocate nothing per record.
	scratch []byte
	// filePositions caches every record's frame offset in EventID order from
	// one bulk index read. Per-record paths (the GLCB build merge's ViewAt,
	// RecordAt) previously issued an index pread PER RECORD on top of the
	// frame read — half the build's syscalls, and under the shared-disk dev
	// cluster those blocking preads park OS threads and starve raft
	// heartbeat goroutines.
	filePositions []uint32
	// frameLens caches every record's on-disk frame length in EventID order,
	// derived from one bulk index read: frames are appended back-to-back, so
	// each length is the gap to the next frame offset (the last runs to the
	// index region). Built lazily on the first FrameByteLenAt — the planner's
	// slice-sizing loop previously issued two preads PER RECORD.
	frameLens []uint32
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
	filePos, err := idx.filePosAt(pos)
	if err != nil {
		return record.Record{}, err
	}
	return idx.sf.ReadRecordAtFilePos(filePos)
}

// filePosAt resolves an EventID-order position to its frame offset from the
// bulk-loaded cache — no per-record index pread.
func (idx *OrderedIndex) filePosAt(pos uint32) (uint32, error) {
	if idx.filePositions == nil {
		positions, err := idx.sf.IndexFilePositions()
		if err != nil {
			return 0, err
		}
		if positions == nil {
			positions = []uint32{}
		}
		idx.filePositions = positions
	}
	if uint64(pos) >= uint64(len(idx.filePositions)) {
		return 0, segment.ErrIndexBounds
	}
	return idx.filePositions[pos], nil
}

// ViewAt returns a zero-copy view of the record at position pos in EventID
// order. The view aliases the index's internal scratch buffer and is valid
// only until the next ViewAt call. Scan loops that consume fixed fields
// (bounds passes: three timestamps per record) use this instead of RecordAt
// to avoid materializing attrs maps and Raw copies they never read.
func (idx *OrderedIndex) ViewAt(pos uint32) (record.View, error) {
	filePos, err := idx.filePosAt(pos)
	if err != nil {
		return record.View{}, err
	}
	v, scratch, err := idx.sf.ReadViewAtFilePos(filePos, idx.scratch)
	idx.scratch = scratch
	return v, err
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
	if err := idx.ensureFrameLens(); err != nil {
		return 0, err
	}
	if uint64(pos) >= uint64(len(idx.frameLens)) {
		return 0, segment.ErrIndexBounds
	}
	return uint64(idx.frameLens[pos]), nil
}

// ensureFrameLens builds the frame-length cache from one bulk index read.
func (idx *OrderedIndex) ensureFrameLens() error {
	if idx.frameLens != nil {
		return nil
	}
	if idx.filePositions == nil {
		positions, err := idx.sf.IndexFilePositions()
		if err != nil {
			return err
		}
		if positions == nil {
			positions = []uint32{}
		}
		idx.filePositions = positions
	}
	positions := idx.filePositions
	if len(positions) == 0 {
		idx.frameLens = []uint32{}
		return nil
	}
	// Frames are contiguous: sorted by file offset, each frame ends where the
	// next begins; the last ends at the index region.
	sorted := make([]uint32, len(positions))
	copy(sorted, positions)
	slices.Sort(sorted)
	end := idx.sf.Header().IndexOffset
	next := make(map[uint32]uint32, len(sorted))
	for i, p := range sorted {
		if i+1 < len(sorted) {
			next[p] = sorted[i+1]
		} else {
			next[p] = end
		}
	}
	lens := make([]uint32, len(positions))
	for i, p := range positions {
		n := next[p]
		if n <= p {
			return fmt.Errorf("%w: non-monotonic frame offsets at pos %d", segment.ErrFrameLength, i)
		}
		lens[i] = n - p
	}
	idx.frameLens = lens
	return nil
}
