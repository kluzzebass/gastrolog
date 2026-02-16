package chunk

import (
	"encoding/binary"
	"errors"
	"slices"
)

var (
	ErrKeyDictFull       = errors.New("key dictionary full (65535 keys)")
	ErrInvalidKeyDictData = errors.New("invalid key dictionary data")
	ErrKeyNotFound       = errors.New("key not found in dictionary")
)

// KeyDict is a per-chunk dictionary mapping attribute key strings to uint16 IDs.
// IDs are assigned sequentially starting from 0.
type KeyDict struct {
	keys   []string
	lookup map[string]uint16
}

// NewKeyDict creates an empty key dictionary.
func NewKeyDict() *KeyDict {
	return &KeyDict{
		lookup: make(map[string]uint16),
	}
}

// Add registers a key and returns its ID. If the key already exists, the
// existing ID is returned. Returns ErrKeyDictFull if the dictionary has
// reached its 65535-key capacity.
func (d *KeyDict) Add(key string) (uint16, error) {
	if id, ok := d.lookup[key]; ok {
		return id, nil
	}
	if len(d.keys) >= 1<<16-1 {
		return 0, ErrKeyDictFull
	}
	id := uint16(len(d.keys))
	d.keys = append(d.keys, key)
	d.lookup[key] = id
	return id, nil
}

// Lookup returns the ID for a key, or false if not present.
func (d *KeyDict) Lookup(key string) (uint16, bool) {
	id, ok := d.lookup[key]
	return id, ok
}

// Key returns the key string for a given ID.
func (d *KeyDict) Key(id uint16) (string, error) {
	if int(id) >= len(d.keys) {
		return "", ErrKeyNotFound
	}
	return d.keys[id], nil
}

// Len returns the number of keys in the dictionary.
func (d *KeyDict) Len() int {
	return len(d.keys)
}

// EncodeDictEntry serializes one dictionary entry: [keyLen:u16][key bytes].
func EncodeDictEntry(key string) []byte {
	buf := make([]byte, 2+len(key))
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(key)))
	copy(buf[2:], key)
	return buf
}

// EncodeWithDict encodes attributes using the key dictionary.
// Format: [count:u16][keyID:u16][valLen:u16][val bytes]... repeated count times.
// Keys are sorted lexicographically for deterministic output.
// Returns the encoded bytes and any newly-added keys (for appending to dict file).
func EncodeWithDict(attrs Attributes, dict *KeyDict) (encoded []byte, newKeys []string, err error) {
	if len(attrs) == 0 {
		return []byte{0, 0}, nil, nil
	}

	// Sort keys for deterministic output.
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Register all keys and collect new ones.
	keyIDs := make([]uint16, len(keys))
	for i, k := range keys {
		prevLen := dict.Len()
		id, err := dict.Add(k)
		if err != nil {
			return nil, nil, err
		}
		keyIDs[i] = id
		if dict.Len() > prevLen {
			newKeys = append(newKeys, k)
		}
	}

	// Calculate total size: 2 (count) + sum of (2 + 2 + valLen).
	size := 2
	for _, k := range keys {
		v := attrs[k]
		size += 2 + 2 + len(v) // keyID + valLen + val
	}

	if size > 65535 {
		return nil, nil, ErrAttrsTooLarge
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(attrs)))

	offset := 2
	for i, k := range keys {
		v := attrs[k]

		binary.LittleEndian.PutUint16(buf[offset:offset+2], keyIDs[i])
		offset += 2

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(v)))
		offset += 2
		copy(buf[offset:], v)
		offset += len(v)
	}

	return buf, newKeys, nil
}

// DecodeWithDict decodes attributes that were encoded with EncodeWithDict.
// Format: [count:u16][keyID:u16][valLen:u16][val bytes]...
func DecodeWithDict(data []byte, dict *KeyDict) (Attributes, error) {
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
		if offset+2 > len(data) {
			return nil, ErrInvalidAttrsData
		}
		keyID := binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2

		key, err := dict.Key(keyID)
		if err != nil {
			return nil, ErrInvalidAttrsData
		}

		// Read value length.
		if offset+2 > len(data) {
			return nil, ErrInvalidAttrsData
		}
		valLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		// Read value.
		if offset+valLen > len(data) {
			return nil, ErrInvalidAttrsData
		}
		val := string(data[offset : offset+valLen])
		offset += valLen

		attrs[key] = val
	}

	return attrs, nil
}

// DecodeDictData rebuilds a KeyDict from the data section of attr_dict.log
// (after the 4-byte header). Tolerates a partial trailing entry for crash recovery.
func DecodeDictData(data []byte) (*KeyDict, error) {
	dict := NewKeyDict()
	offset := 0

	for offset < len(data) {
		// Need at least 2 bytes for keyLen.
		if offset+2 > len(data) {
			break // Partial trailing entry — tolerate.
		}
		keyLen := int(binary.LittleEndian.Uint16(data[offset : offset+2]))
		offset += 2

		// Need keyLen bytes for the key.
		if offset+keyLen > len(data) {
			break // Partial trailing entry — tolerate.
		}
		key := string(data[offset : offset+keyLen])
		offset += keyLen

		if _, err := dict.Add(key); err != nil {
			return nil, err
		}
	}

	return dict, nil
}
