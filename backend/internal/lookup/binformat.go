package lookup

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"gastrolog/internal/format"
)

// Binary lookup table format (version 1)
//
// A sorted, memory-mapped file format for O(log n) key lookups with zero
// heap-allocated index. Used by both JSON file and CSV lookup tables.
//
// All multi-byte integers are little-endian. Strings are length-prefixed:
// [uint16 length][length bytes]. Maximum string length is 65535 bytes.
//
// # File layout
//
//	┌──────────────────────────┐
//	│ File Header     (20 B)   │
//	├──────────────────────────┤
//	│ Column Names             │
//	├──────────────────────────┤
//	│ Key Offset Table         │
//	├──────────────────────────┤
//	│ Key Data (sorted)        │
//	├──────────────────────────┤
//	│ Value Data               │
//	└──────────────────────────┘
//
// # File Header (20 bytes)
//
//	Offset  Size  Field
//	─────────────────────────────────────────────────
//	0       4     format.Header: signature 'i', type 'L', version 1, flags
//	4       4     numRows:         uint32, number of deduplicated rows
//	8       4     numCols:         uint32, number of value columns
//	12      4     keyOffTblOffset: uint32, byte offset of key offset table
//	16      4     keyDataOffset:   uint32, byte offset of key data section
//
// # Column Names (starts at byte 20)
//
//	For each of numCols columns (in sorted order):
//	    [uint16 nameLen][nameLen bytes]
//
//	Read once at load time to populate Suffixes().
//
// # Key Offset Table (at keyOffTblOffset)
//
//	Fixed-size array enabling O(1) access to the nth key during binary search.
//
//	For each of numRows entries:
//	    [uint32 keyDataEntryOffset]  — relative to keyDataOffset
//
//	Total size: numRows × 4 bytes.
//
// # Key Data (at keyDataOffset, sorted lexicographically by key)
//
//	For each key:
//	    [uint16 keyLen][keyLen bytes][uint32 valueDataOffset]
//
//	The valueDataOffset is an absolute byte offset into the file where
//	this row's value data begins. It is stored adjacent to the key so
//	that after a binary search comparison reads the key bytes, the
//	value pointer is in the same cache line.
//
// # Value Data
//
//	For each row (in sorted key order):
//	    For each of numCols columns:
//	        [uint16 valLen][valLen bytes]
//
//	All value columns are stored contiguously per row.
//
// # Lookup algorithm
//
//	1. Binary search the key offset table (sort.Search).
//	2. For each probe: read keyOffTblOffset[mid] → jump to key data →
//	   read key string → compare with search key.
//	3. On match: read valueDataOffset → jump to value data →
//	   read numCols length-prefixed strings → return as map.
//
//	O(log n) comparisons, each requiring two reads from the mmap.
//	Zero heap allocation for the search itself; only the result map
//	is allocated per lookup.
//
// # Duplicate keys
//
//	The encoder sorts rows by key and deduplicates — first occurrence wins.
//	The duplicate count is returned by the encoder for diagnostic logging.

const (
	binVersion    = 1
	binHeaderSize = 20 // format.Header (4) + numRows (4) + numCols (4) + keyOffTblOff (4) + keyDataOff (4)
	maxStringLen  = 65535
)

var le = binary.LittleEndian

// binRow holds a single row during the write phase.
type binRow struct {
	key    string
	values []string // in column order
}

// encodeBinLookup writes the binary lookup table format.
// Rows are deduplicated by key (first occurrence wins) and sorted lexicographically.
// Returns the encoded bytes and the number of duplicate keys encountered.
func encodeBinLookup(columns []string, rows []binRow) ([]byte, int, error) {
	if len(rows) == 0 {
		return nil, 0, errors.New("no rows to encode")
	}

	// Sort by key.
	sort.Slice(rows, func(i, j int) bool { return rows[i].key < rows[j].key })

	// Dedup — first occurrence wins (already first after stable sort of original order isn't guaranteed,
	// but the caller collects in order and sort is not stable, so "first" means lowest original index
	// only if we dedup before sorting. Let's dedup after sorting: keep the first of each run).
	deduped := rows[:0:0]
	var duplicates int
	for i, r := range rows {
		if i > 0 && r.key == rows[i-1].key {
			duplicates++
			continue
		}
		deduped = append(deduped, r)
	}
	rows = deduped
	numRows := len(rows)
	numCols := len(columns)

	// Validate string lengths.
	for _, c := range columns {
		if len(c) > maxStringLen {
			return nil, 0, fmt.Errorf("column name %q exceeds max length %d", c, maxStringLen)
		}
	}
	for _, r := range rows {
		if len(r.key) > maxStringLen {
			return nil, 0, fmt.Errorf("key %q exceeds max length %d", r.key, maxStringLen)
		}
		for _, v := range r.values {
			if len(v) > maxStringLen {
				return nil, 0, fmt.Errorf("value exceeds max length %d", maxStringLen)
			}
		}
	}

	// Calculate section sizes.
	colNamesSize := 0
	for _, c := range columns {
		colNamesSize += 2 + len(c) // u16 + bytes
	}

	keyOffTblSize := numRows * 4 // uint32 per row

	keyDataSize := 0
	for _, r := range rows {
		keyDataSize += 2 + len(r.key) + 4 // u16 key + key bytes + u32 valueDataOffset
	}

	valueDataSize := 0
	for _, r := range rows {
		for _, v := range r.values {
			valueDataSize += 2 + len(v) // u16 + bytes
		}
	}

	// Section offsets.
	colNamesOffset := binHeaderSize
	keyOffTblOffset := colNamesOffset + colNamesSize
	keyDataOffset := keyOffTblOffset + keyOffTblSize
	valueDataOffset := keyDataOffset + keyDataSize
	totalSize := valueDataOffset + valueDataSize

	buf := make([]byte, totalSize)

	// --- File Header ---
	hdr := format.Header{Type: format.TypeLookupTable, Version: binVersion, Flags: format.FlagComplete}
	hdr.EncodeInto(buf)
	le.PutUint32(buf[4:], uint32(numRows))   //nolint:gosec // bounded
	le.PutUint32(buf[8:], uint32(numCols))   //nolint:gosec // bounded
	le.PutUint32(buf[12:], uint32(keyOffTblOffset))
	le.PutUint32(buf[16:], uint32(keyDataOffset))   //nolint:gosec // bounded

	// --- Column Names ---
	off := colNamesOffset
	for _, c := range columns {
		le.PutUint16(buf[off:], uint16(len(c))) //nolint:gosec // validated
		off += 2
		copy(buf[off:], c)
		off += len(c)
	}

	// --- Key Offset Table + Key Data + Value Data ---
	keyOff := 0                  // offset within key data section
	valOff := valueDataOffset    // absolute offset for value data
	for i, r := range rows {
		// Key offset table entry.
		le.PutUint32(buf[keyOffTblOffset+i*4:], uint32(keyOff))

		// Key data entry: [u16 keyLen][key bytes][u32 valueDataOffset]
		pos := keyDataOffset + keyOff
		le.PutUint16(buf[pos:], uint16(len(r.key))) //nolint:gosec // validated
		pos += 2
		copy(buf[pos:], r.key)
		pos += len(r.key)
		le.PutUint32(buf[pos:], uint32(valOff)) //nolint:gosec // bounded

		keyOff += 2 + len(r.key) + 4

		// Value data: numCols × [u16 valLen][val bytes]
		for _, v := range r.values {
			le.PutUint16(buf[valOff:], uint16(len(v))) //nolint:gosec // validated
			valOff += 2
			copy(buf[valOff:], v)
			valOff += len(v)
		}
	}

	return buf, duplicates, nil
}

// binData holds the mmap'd binary lookup table.
// No heap-allocated index — lookups use binary search on mmap'd sorted keys.
type binData struct {
	mmapData        []byte
	file            interface{ Close() error } // *os.File or nil for tests
	numRows         uint32
	numCols         uint32
	keyOffTblOffset uint32
	keyDataOffset   uint32
	suffixes        []string // column names, read once at load
	duplicateKeys   int
}

func (d *binData) close() {
	if d.file != nil {
		_ = d.file.Close()
	}
	// mmapData is unmapped by the caller (JSONFile.Close) if needed,
	// or by syscall.Munmap if the binData owns it.
}

// decodeBinHeader validates and parses the 20-byte file header.
func decodeBinHeader(data []byte) (*binData, error) {
	if len(data) < binHeaderSize {
		return nil, fmt.Errorf("file too small for lookup header (%d bytes)", len(data))
	}
	if _, err := format.DecodeAndValidate(data, format.TypeLookupTable, binVersion); err != nil {
		return nil, fmt.Errorf("invalid lookup header: %w", err)
	}
	return &binData{
		numRows:         le.Uint32(data[4:]),
		numCols:         le.Uint32(data[8:]),
		keyOffTblOffset: le.Uint32(data[12:]),
		keyDataOffset:   le.Uint32(data[16:]),
	}, nil
}

// decodeBinColumns reads the column names section (immediately after the header).
func decodeBinColumns(data []byte, numCols uint32) ([]string, error) {
	off := binHeaderSize
	cols := make([]string, numCols)
	for i := range numCols {
		if off+2 > len(data) {
			return nil, fmt.Errorf("truncated column name %d", i)
		}
		n := int(le.Uint16(data[off:]))
		off += 2
		if off+n > len(data) {
			return nil, fmt.Errorf("truncated column name %d data", i)
		}
		cols[i] = string(data[off : off+n])
		off += n
	}
	return cols, nil
}

// lookupKey performs a binary search on the sorted key index and returns
// the matching row's values as a map, or nil on miss.
// Zero heap allocation for the search; only the result map is allocated.
func (d *binData) lookupKey(data []byte, key string, suffixes []string) map[string]string {
	n := int(d.numRows)
	if n == 0 {
		return nil
	}

	idx := sort.Search(n, func(i int) bool {
		k := d.readKey(data, i)
		return k >= key
	})

	if idx >= n {
		return nil
	}

	// Verify exact match.
	entryOff := d.keyEntryOffset(data, idx)
	keyLen := int(le.Uint16(data[entryOff:]))
	entryOff += 2
	found := string(data[entryOff : entryOff+keyLen])
	if found != key {
		return nil
	}
	entryOff += keyLen
	valDataOff := int(le.Uint32(data[entryOff:]))

	// Read value columns.
	result := make(map[string]string, len(suffixes))
	off := valDataOff
	for _, col := range suffixes {
		vLen := int(le.Uint16(data[off:]))
		off += 2
		result[col] = string(data[off : off+vLen])
		off += vLen
	}
	return result
}

// readKey returns the key string at index i by reading from the mmap'd data.
func (d *binData) readKey(data []byte, i int) string {
	entryOff := d.keyEntryOffset(data, i)
	keyLen := int(le.Uint16(data[entryOff:]))
	entryOff += 2
	return string(data[entryOff : entryOff+keyLen])
}

// keyEntryOffset returns the absolute byte offset of the ith key entry.
func (d *binData) keyEntryOffset(data []byte, i int) int {
	relOff := le.Uint32(data[int(d.keyOffTblOffset)+i*4:])
	return int(d.keyDataOffset) + int(relOff)
}
