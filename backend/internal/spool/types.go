package spool

import (
	"errors"
	"fmt"
)

// SegmentID identifies a spool segment by the vault_seq of its first accepted record.
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

// SegmentMeta describes sequence bounds for one spool segment.
type SegmentMeta struct {
	ID          SegmentID
	FirstSeq    uint64
	LastSeq     uint64
	RecordCount uint64
	Sealed      bool
}

// Bounds returns the inclusive sequence range covered by this segment.
func (m SegmentMeta) Bounds() (first, last uint64) {
	return m.FirstSeq, m.LastSeq
}

// CoversSeq reports whether seq falls within this segment's inclusive bounds.
func (m SegmentMeta) CoversSeq(seq uint64) bool {
	if m.RecordCount == 0 || m.FirstSeq == 0 {
		return false
	}
	return seq >= m.FirstSeq && seq <= m.LastSeq
}
