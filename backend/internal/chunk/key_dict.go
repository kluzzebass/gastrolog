package chunk

import (
	"encoding/binary"
	"errors"
	"slices"
)

var (
	ErrDictFull         = errors.New("string dictionary full")
	ErrDictEntryNotFound = errors.New("entry not found in dictionary")
)

// StringDict is a per-chunk dictionary mapping strings to uint32 IDs.
// Used for both attribute keys and values. IDs are assigned sequentially
// starting from 0.
type StringDict struct {
	strings []string
	lookup  map[string]uint32
}

// NewStringDict creates an empty string dictionary.
func NewStringDict() *StringDict {
	return &StringDict{
		lookup: make(map[string]uint32),
	}
}

// Add registers a string and returns its ID. If the string already exists,
// the existing ID is returned. Returns ErrDictFull if the dictionary has
// reached its capacity.
func (d *StringDict) Add(s string) (uint32, error) {
	if id, ok := d.lookup[s]; ok {
		return id, nil
	}
	if len(d.strings) >= 1<<32-1 {
		return 0, ErrDictFull
	}
	id := uint32(len(d.strings)) //nolint:gosec // G115: bounded by 1<<32-1 check above
	d.strings = append(d.strings, s)
	d.lookup[s] = id
	return id, nil
}

// Lookup returns the ID for a string, or false if not present.
func (d *StringDict) Lookup(s string) (uint32, bool) {
	id, ok := d.lookup[s]
	return id, ok
}

// Get returns the string for a given ID.
func (d *StringDict) Get(id uint32) (string, error) {
	if int(id) >= len(d.strings) {
		return "", ErrDictEntryNotFound
	}
	return d.strings[id], nil
}

// Len returns the number of entries in the dictionary.
func (d *StringDict) Len() int {
	return len(d.strings)
}

// EncodeDictEntry serializes one dictionary entry: [strLen:u16][string bytes].
func EncodeDictEntry(s string) []byte {
	buf := make([]byte, 2+len(s))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(s))) //nolint:gosec // G115: dict entry string length bounded by key/value size limits
	copy(buf[2:], s)
	return buf
}

// EncodeWithDict encodes attributes using the string dictionary for both
// keys and values.
// Format: [count:u16][keyID:u32][valID:u32]... repeated count times.
// Keys are sorted lexicographically for deterministic output.
// Returns the encoded bytes and any newly-added strings (for appending to dict file).
func EncodeWithDict(attrs Attributes, dict *StringDict) (encoded []byte, newEntries []string, err error) {
	if len(attrs) == 0 {
		return []byte{0, 0}, nil, nil
	}

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Register all keys and values, collect new entries.
	keyIDs := make([]uint32, len(keys))
	valIDs := make([]uint32, len(keys))
	for i, k := range keys {
		prevLen := dict.Len()
		id, err := dict.Add(k)
		if err != nil {
			return nil, nil, err
		}
		keyIDs[i] = id
		if dict.Len() > prevLen {
			newEntries = append(newEntries, k)
		}

		v := attrs[k]
		prevLen = dict.Len()
		id, err = dict.Add(v)
		if err != nil {
			return nil, nil, err
		}
		valIDs[i] = id
		if dict.Len() > prevLen {
			newEntries = append(newEntries, v)
		}
	}

	// Calculate total size: 2 (count) + count * 8 (keyID:u32 + valID:u32).
	size := 2 + len(attrs)*8

	if size > 65535 {
		return nil, nil, ErrAttrsTooLarge
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(attrs))) //nolint:gosec // G115: attribute count bounded by size check above

	offset := 2
	for i := range keys {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], keyIDs[i])
		offset += 4

		binary.LittleEndian.PutUint32(buf[offset:offset+4], valIDs[i])
		offset += 4
	}

	return buf, newEntries, nil
}

// DecodeWithDict decodes attributes that were encoded with EncodeWithDict.
// Format: [count:u16][keyID:u32][valID:u32]...
func DecodeWithDict(data []byte, dict *StringDict) (Attributes, error) {
	if len(data) < 2 {
		return nil, ErrInvalidAttrsData
	}

	count := int(binary.LittleEndian.Uint16(data[0:2]))
	if count == 0 {
		return Attributes{}, nil
	}

	attrs := make(Attributes, count)
	offset := 2

	for range count {
		// Read key ID.
		if offset+4 > len(data) {
			return nil, ErrInvalidAttrsData
		}
		keyID := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		key, err := dict.Get(keyID)
		if err != nil {
			return nil, ErrInvalidAttrsData
		}

		// Read value ID.
		if offset+4 > len(data) {
			return nil, ErrInvalidAttrsData
		}
		valID := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		val, err := dict.Get(valID)
		if err != nil {
			return nil, ErrInvalidAttrsData
		}

		attrs[key] = val
	}

	return attrs, nil
}

// DecodeDictData rebuilds a StringDict from the data section of attr_dict.log
// (after the 4-byte header). Tolerates a partial trailing entry for crash recovery.
func DecodeDictData(data []byte) (*StringDict, error) {
	dict := NewStringDict()
	offset := 0

	for offset < len(data) {
		// Need at least 2 bytes for strLen.
		if offset+2 > len(data) {
			break // Partial trailing entry — tolerate.
		}
		strLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		// Need strLen bytes for the string.
		if offset+strLen > len(data) {
			break // Partial trailing entry — tolerate.
		}
		s := string(data[offset : offset+strLen])
		offset += strLen

		if _, err := dict.Add(s); err != nil {
			return nil, err
		}
	}

	return dict, nil
}
