package chunk

import (
	"encoding/binary"
	"testing"
)

// FuzzDictAttrsRoundTrip verifies that EncodeWithDict/DecodeWithDict
// round-trip correctly for arbitrary attribute maps.
func FuzzDictAttrsRoundTrip(f *testing.F) {
	// Seed: pairs of null-terminated strings interpreted as key-value pairs.
	f.Add([]byte("host\x00server1\x00level\x00info\x00"))
	f.Add([]byte("k\x00v\x00"))
	f.Add([]byte(""))
	f.Add([]byte("\x00\x00\x00\x00"))
	// Seed with repeated keys (last value wins in map).
	f.Add([]byte("a\x00b\x00a\x00c\x00"))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Parse data as null-separated strings, then pair them as key-value.
		var parts []string
		start := 0
		for i, b := range data {
			if b == 0 {
				parts = append(parts, string(data[start:i]))
				start = i + 1
			}
		}
		if start < len(data) {
			parts = append(parts, string(data[start:]))
		}

		// Need at least 2 parts to form one key-value pair.
		// Empty attrs is also a valid test case.
		attrs := make(Attributes)
		for i := 0; i+1 < len(parts); i += 2 {
			attrs[parts[i]] = parts[i+1]
		}

		// Limit attribute count to prevent hitting ErrAttrsTooLarge.
		if len(attrs) > 500 {
			return
		}

		dict := NewStringDict()

		encoded, _, err := EncodeWithDict(attrs, dict)
		if err != nil {
			// ErrAttrsTooLarge or ErrDictFull are acceptable.
			if err == ErrAttrsTooLarge || err == ErrDictFull {
				return
			}
			t.Fatalf("EncodeWithDict: %v", err)
		}

		decoded, err := DecodeWithDict(encoded, dict)
		if err != nil {
			t.Fatalf("DecodeWithDict: %v", err)
		}

		// Verify all original attributes are present and correct.
		if len(decoded) != len(attrs) {
			t.Fatalf("attribute count: got %d, want %d", len(decoded), len(attrs))
		}
		for k, wantV := range attrs {
			gotV, ok := decoded[k]
			if !ok {
				t.Fatalf("missing key %q", k)
			}
			if gotV != wantV {
				t.Fatalf("key %q: got %q, want %q", k, gotV, wantV)
			}
		}

		// Also verify the encoded format header: first 2 bytes = count.
		if len(encoded) >= 2 {
			count := binary.LittleEndian.Uint16(encoded[0:2])
			if int(count) != len(attrs) {
				t.Fatalf("encoded count header: got %d, want %d", count, len(attrs))
			}
		}
	})
}
