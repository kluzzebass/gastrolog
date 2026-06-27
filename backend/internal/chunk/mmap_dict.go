package chunk

import (
	"encoding/binary"
	"errors"
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
// copying every entry into the heap at open time. Get returns strings that
// alias the region; the caller must keep the backing storage alive.
type MmapStringDict struct {
	data    []byte
	entries []mmapDictEntry
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
	return &MmapStringDict{data: buf, entries: entries}, nil
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
	return &MmapStringDict{data: buf, entries: entries}, nil
}

// Get returns the string for a dictionary ID. The returned string is a heap
// copy so callers can retain it after the mmap backing store is released.
func (d *MmapStringDict) Get(id uint32) (string, error) {
	if int(id) >= len(d.entries) {
		return "", ErrDictEntryNotFound
	}
	e := d.entries[id]
	start := int(e.off)
	end := start + int(e.len)
	if start < 0 || end > len(d.data) {
		return "", ErrInvalidAttrsData
	}
	return string(append([]byte(nil), d.data[start:end]...)), nil
}
