package source

import (
	"encoding/binary"
	"slices"
	"strings"
)

// makeKey generates a canonical sourceKey from attributes.
// The key is deterministic: same attributes always produce the same key.
//
// Canonicalization:
//  1. Sort attribute keys lexicographically
//  2. Encode as length-prefixed key/value pairs
//
// Format: [keyLen:2][key][valLen:2][val]...
// Length prefixes are little-endian uint16.
func makeKey(attrs map[string]string) sourceKey {
	if len(attrs) == 0 {
		return ""
	}

	// Collect and sort keys.
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	// Calculate total size for pre-allocation.
	size := 0
	for _, k := range keys {
		size += 2 + len(k) + 2 + len(attrs[k])
	}

	// Build the canonical key.
	var b strings.Builder
	b.Grow(size)
	buf := make([]byte, 2)

	for _, k := range keys {
		v := attrs[k]

		binary.LittleEndian.PutUint16(buf, uint16(len(k)))
		b.Write(buf)
		b.WriteString(k)

		binary.LittleEndian.PutUint16(buf, uint16(len(v)))
		b.Write(buf)
		b.WriteString(v)
	}

	return sourceKey(b.String())
}
