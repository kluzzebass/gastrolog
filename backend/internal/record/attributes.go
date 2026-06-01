package record

import (
	"encoding/binary"
	"errors"
	"maps"
	"slices"
)

var (
	// ErrAttrsTooLarge is returned when encoded attributes exceed uint16 capacity.
	ErrAttrsTooLarge = errors.New("attributes too large to encode")
	// ErrInvalidAttrsData is returned when attribute blob data is malformed.
	ErrInvalidAttrsData = errors.New("invalid attributes data")
)

// Attributes represents record metadata as key-value pairs.
type Attributes map[string]string

// Encode serializes attributes to binary format.
// Format: [count:u16][keyLen:u16][key bytes][valLen:u16][val bytes]... repeated count times
// Keys are sorted lexicographically for deterministic output.
// Returns error if the encoded size would exceed uint16 (65535 bytes).
func (a Attributes) Encode() ([]byte, error) {
	if len(a) == 0 {
		return []byte{0, 0}, nil
	}

	keys := make([]string, 0, len(a))
	for k := range a {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	size := 2
	for _, k := range keys {
		v := a[k]
		size += 2 + len(k) + 2 + len(v)
	}

	if size > 65535 {
		return nil, ErrAttrsTooLarge
	}

	buf := make([]byte, size)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(len(a))) //nolint:gosec // G115: attribute count bounded by size check above

	offset := 2
	for _, k := range keys {
		v := a[k]

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(k))) //nolint:gosec // G115: key length bounded by size check above
		offset += 2
		copy(buf[offset:], k)
		offset += len(k)

		binary.LittleEndian.PutUint16(buf[offset:offset+2], uint16(len(v))) //nolint:gosec // G115: value length bounded by size check above
		offset += 2
		copy(buf[offset:], v)
		offset += len(v)
	}

	return buf, nil
}

// Copy returns a deep copy of the attributes.
func (a Attributes) Copy() Attributes {
	if a == nil {
		return nil
	}
	cp := make(Attributes, len(a))
	maps.Copy(cp, a)
	return cp
}
