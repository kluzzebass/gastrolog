package chunk

import (
	"encoding/binary"
	"errors"
	"sync/atomic"
)

// DictReader resolves string IDs during attribute decoding.
type DictReader interface {
	Get(id uint32) (string, error)
}

// mmapDictEntry is one string slot in a mmap-backed dictionary region.
type mmapDictEntry struct {
	off uint32
	len uint16
}

// MmapStringDict reads dictionary strings from a fixed byte region without
// copying every entry into the heap at open time. Get interns each entry on
// first access: the returned string is a heap copy shared by every
// subsequent Get for the same ID, so callers may retain it after the mmap
// backing store is released, and per-record decoding pays the copy once per
// unique string instead of once per lookup — the per-lookup copies were a
// measurable slice of drain/search GC churn (gastrolog-11y2iv).
type MmapStringDict struct {
	data    []byte
	entries []mmapDictEntry
	// strs holds the interned heap copy per entry, filled lazily. Concurrent
	// first accesses may each build a copy; both are identical values and
	// the atomic store keeps the race benign.
	strs []atomic.Pointer[string]
}

// NewMmapStringDict parses dictEntries from buf using the GLCB dict wire format:
// [strLen:u16][string bytes] repeated dictEntries times.
func NewMmapStringDict(buf []byte, dictEntries uint32) (*MmapStringDict, error) {
	entries := make([]mmapDictEntry, dictEntries)
	off := 0
	for i := range dictEntries {
		if off+2 > len(buf) {
			return nil, errors.New("truncated dict buffer")
		}
		strLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		if off+strLen > len(buf) {
			return nil, errors.New("truncated dict entry")
		}
		entries[i] = mmapDictEntry{off: uint32(off), len: uint16(strLen)} //nolint:gosec // G115: strLen bounded by buffer check
		off += strLen
	}
	return &MmapStringDict{data: buf, entries: entries, strs: make([]atomic.Pointer[string], dictEntries)}, nil
}

// ScanMmapStringDict parses every complete dict entry in buf, tolerating a
// partial trailing entry (attr_dict.log crash recovery).
func ScanMmapStringDict(buf []byte) (*MmapStringDict, error) {
	var entries []mmapDictEntry
	off := 0
	for off < len(buf) {
		if off+2 > len(buf) {
			break
		}
		strLen := int(binary.LittleEndian.Uint16(buf[off:]))
		off += 2
		if off+strLen > len(buf) {
			break
		}
		entries = append(entries, mmapDictEntry{off: uint32(off), len: uint16(strLen)}) //nolint:gosec // G115: strLen bounded by buffer check
		off += strLen
	}
	return &MmapStringDict{data: buf, entries: entries, strs: make([]atomic.Pointer[string], len(entries))}, nil
}

// Get returns the string for a dictionary ID, interned on first access.
// Safe for concurrent use; the returned string is valid after the mmap
// backing store is released.
func (d *MmapStringDict) Get(id uint32) (string, error) {
	if int(id) >= len(d.entries) {
		return "", ErrDictEntryNotFound
	}
	if p := d.strs[id].Load(); p != nil {
		return *p, nil
	}
	e := d.entries[id]
	start := int(e.off)
	end := start + int(e.len)
	if start < 0 || end > len(d.data) {
		return "", ErrInvalidAttrsData
	}
	s := string(d.data[start:end])
	d.strs[id].Store(&s)
	return s, nil
}
