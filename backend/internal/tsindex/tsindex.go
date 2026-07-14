// Package tsindex owns the 12-byte (timestamp, record position) index entry
// wire layout shared by segment SourceTS index tails and the GLCB ITSI/STSI
// sections: [tsNano:i64 LE][pos:u32 LE] × N, no header. Sorting is stable by
// timestamp with ties broken by position so equal timestamps keep record
// order; lookup returns the first entry at or after a timestamp.
package tsindex

import (
	"encoding/binary"
	"io"
	"slices"
)

// EntrySize is the on-disk byte length of one TS index entry.
const EntrySize = 12

// Entry is one (timestamp, record position) pair.
type Entry struct {
	TS  int64  // UnixNano
	Pos uint32 // record position (EventID-order or GLCB record index)
}

// Compare orders entries by timestamp, breaking ties by position.
func Compare(a, b Entry) int {
	if a.TS != b.TS {
		if a.TS < b.TS {
			return -1
		}
		return 1
	}
	if a.Pos < b.Pos {
		return -1
	}
	if a.Pos > b.Pos {
		return 1
	}
	return 0
}

// Sort stable-sorts entries with Compare.
func Sort(entries []Entry) {
	slices.SortStableFunc(entries, Compare)
}

// Encode writes e into buf[:EntrySize]. Caller guarantees the length.
func Encode(buf []byte, e Entry) {
	binary.LittleEndian.PutUint64(buf[0:], uint64(e.TS)) //nolint:gosec // G115: nanosecond timestamps stored as uint64
	binary.LittleEndian.PutUint32(buf[8:], e.Pos)
}

// Decode reads one entry from buf[:EntrySize]. Caller guarantees the length.
func Decode(buf []byte) Entry {
	return Entry{
		TS:  int64(binary.LittleEndian.Uint64(buf[0:])), //nolint:gosec // G115: nanosecond timestamps fit in int64
		Pos: binary.LittleEndian.Uint32(buf[8:]),
	}
}

// EncodeAll serializes entries to their contiguous on-disk form.
func EncodeAll(entries []Entry) []byte {
	buf := make([]byte, len(entries)*EntrySize)
	for i, e := range entries {
		Encode(buf[i*EntrySize:], e)
	}
	return buf
}

// SortRegion stable-sorts an encoded on-disk region (e.g. an mmap'd index
// tail) in place.
func SortRegion(data []byte) {
	n := len(data) / EntrySize
	if n == 0 {
		return
	}
	entries := make([]Entry, n)
	for i := range n {
		entries[i] = Decode(data[i*EntrySize : (i+1)*EntrySize])
	}
	Sort(entries)
	for i, e := range entries {
		Encode(data[i*EntrySize:(i+1)*EntrySize], e)
	}
}

// FindStart binary-searches encoded sorted entries for the first entry with
// TS >= tsNano. Returns (pos, true), or (0, false) when tsNano is after all
// entries or data is empty.
func FindStart(data []byte, tsNano int64) (uint32, bool) {
	n := len(data) / EntrySize
	if n == 0 {
		return 0, false
	}

	readTS := func(i int) int64 {
		return int64(binary.LittleEndian.Uint64(data[i*EntrySize:])) //nolint:gosec // G115: nanosecond timestamps fit in int64
	}
	readPos := func(i int) uint32 {
		return binary.LittleEndian.Uint32(data[i*EntrySize+8:])
	}

	// Quick bounds check.
	if tsNano > readTS(n-1) {
		return 0, false // past all entries
	}
	if tsNano <= readTS(0) {
		return readPos(0), true
	}

	// Binary search: first index i where TS[i] >= tsNano.
	lo, hi := 0, n
	for lo < hi {
		mid := lo + (hi-lo)/2
		if readTS(mid) < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return readPos(lo), true
}

// FindStartAt is FindStart over count entries stored at base in r, probing
// the reader directly instead of loading the tail.
func FindStartAt(r io.ReaderAt, base int64, count int, tsNano int64) (uint32, bool, error) {
	if count == 0 {
		return 0, false, nil
	}

	readEntry := func(i int) (Entry, error) {
		var buf [EntrySize]byte
		if _, err := r.ReadAt(buf[:], base+int64(i)*EntrySize); err != nil {
			return Entry{}, err
		}
		return Decode(buf[:]), nil
	}

	first, err := readEntry(0)
	if err != nil {
		return 0, false, err
	}
	last, err := readEntry(count - 1)
	if err != nil {
		return 0, false, err
	}
	if tsNano > last.TS {
		return 0, false, nil
	}
	if tsNano <= first.TS {
		return first.Pos, true, nil
	}

	lo, hi := 0, count
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := readEntry(mid)
		if err != nil {
			return 0, false, err
		}
		if entry.TS < tsNano {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	entry, err := readEntry(lo)
	if err != nil {
		return 0, false, err
	}
	return entry.Pos, true, nil
}
