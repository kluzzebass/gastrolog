package tsindex

import "fmt"

// View is the search API over one TS-index section, independent of how the
// section's bytes are laid out. Rank is the entry's index in the TS-sorted
// order; Pos is the physical record position from the entry. The two differ
// on non-monotonic chunks (built via ImportRecords), which is why both are
// exposed.
//
// Section-format versions each implement View over their own byte layout;
// the GLCB section registry picks the implementation from the (type,
// version) recorded in the blob's TOC entry.
type View interface {
	// SearchTS returns the first entry with TS >= tsNano, or ok=false when
	// tsNano is past every entry (or the section is empty).
	SearchTS(tsNano int64) (rank uint32, pos uint32, ok bool)
	Len() uint32
	// EntryAt returns the entry at rank i. Caller must ensure i < Len().
	EntryAt(i uint32) Entry
}

// RawView is the version-1 layout: contiguous [ts:i64][pos:u32] entries,
// TS-sorted, no header. It reads directly from the given bytes — an mmap
// alias or a heap buffer — and never copies, so its lifetime is bounded by
// the bytes' lifetime.
type RawView struct {
	data []byte
	n    uint32
}

// NewRawView validates the section's structure (whole entries only) and
// returns a view over it. Empty input is a valid zero-entry view — a chunk
// can have no records — so callers need no special case for it.
func NewRawView(data []byte) (RawView, error) {
	if len(data)%EntrySize != 0 {
		return RawView{}, fmt.Errorf("tsindex: section length %d not a multiple of %d", len(data), EntrySize)
	}
	return RawView{
		data: data,
		n:    uint32(len(data) / EntrySize), //nolint:gosec // G115: entry count bounded by chunk record count
	}, nil
}

// SearchTS binary-searches for the first entry with TS >= tsNano, reading
// entries straight out of the underlying bytes — no heap-allocated slice.
func (v RawView) SearchTS(tsNano int64) (rank uint32, pos uint32, ok bool) {
	if v.n == 0 {
		return 0, 0, false
	}
	if tsNano > v.EntryAt(v.n-1).TS {
		return 0, 0, false
	}
	lo, hi := uint32(0), v.n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if v.EntryAt(mid).TS < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, v.EntryAt(lo).Pos, true
}

// Len returns the number of entries in the section.
func (v RawView) Len() uint32 { return v.n }

// EntryAt returns the entry at rank i. Caller must ensure i < Len().
func (v RawView) EntryAt(i uint32) Entry {
	off := int(i) * EntrySize
	return Decode(v.data[off : off+EntrySize])
}
