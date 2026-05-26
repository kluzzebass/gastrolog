package spool

import (
	"errors"
	"fmt"
)

// WindowID identifies a sequence window by allocator swath bounds [Start..End].
type WindowID struct {
	Start, End uint64
}

// DirName returns the on-disk directory name for this window.
func (w WindowID) DirName() string {
	return fmt.Sprintf("w-%020d-%020d", w.Start, w.End)
}

// ParseWindowDirName parses a window directory name back to WindowID.
func ParseWindowDirName(name string) (WindowID, error) {
	var start, end uint64
	if _, err := fmt.Sscanf(name, "w-%020d-%020d", &start, &end); err != nil {
		return WindowID{}, fmt.Errorf("spool: invalid window dir %q: %w", name, err)
	}
	if start == 0 || end == 0 || start > end {
		return WindowID{}, errors.New("spool: invalid window bounds")
	}
	return WindowID{Start: start, End: end}, nil
}

// SegmentID identifies a spool segment by the vault_seq of its first accepted record.
//
// Deprecated: new spool writes use WindowID; SegmentID remains for legacy segment dirs.
type SegmentID uint64

// DirName returns the on-disk directory name for this segment (sortable decimal).
func (id SegmentID) DirName() string {
	return fmt.Sprintf("%020d", uint64(id))
}

// ParseSegmentID parses a segment directory name back to SegmentID.
func ParseSegmentID(name string) (SegmentID, error) {
	var n uint64
	if _, err := fmt.Sscanf(name, "%020d", &n); err != nil {
		return 0, fmt.Errorf("spool: invalid segment dir %q: %w", name, err)
	}
	if n == 0 {
		return 0, errors.New("spool: segment id must be non-zero")
	}
	return SegmentID(n), nil
}

// SegmentMeta describes sequence bounds for one spool segment or window.
type SegmentMeta struct {
	ID          SegmentID
	Window      WindowID
	FirstSeq    uint64
	EndSeq      uint64 // inclusive window upper bound from allocator swath
	LastSeq     uint64
	RecordCount uint64
	Sealed      bool
}

// Bounds returns the inclusive sequence range covered by this segment.
func (m SegmentMeta) Bounds() (first, last uint64) {
	return m.FirstSeq, m.LastSeq
}

// CoversSeq reports whether seq falls within this window/segment bounds.
func (m SegmentMeta) CoversSeq(seq uint64) bool {
	if m.EndSeq > 0 {
		return seq >= m.FirstSeq && seq <= m.EndSeq
	}
	if m.RecordCount == 0 || m.FirstSeq == 0 {
		return false
	}
	return seq >= m.FirstSeq && seq <= m.LastSeq
}
