package chunk

import (
	"testing"
)

// FuzzStringDict verifies that StringDict.Add and Get round-trip correctly
// for arbitrary strings, and that duplicate adds return the same ID.
func FuzzStringDict(f *testing.F) {
	f.Add([]byte("hello\x00world\x00foo\x00"))
	f.Add([]byte("\x00\x00\x00"))
	f.Add([]byte(""))
	f.Add([]byte("a]b]c]d]e]f]g]h]i]j]k]l]m]n]o]p"))

	f.Fuzz(func(t *testing.T, data []byte) {
		dict := NewStringDict()

		// Split data on null bytes to get a list of strings to add.
		var strings []string
		start := 0
		for i, b := range data {
			if b == 0 {
				strings = append(strings, string(data[start:i]))
				start = i + 1
			}
		}
		if start < len(data) {
			strings = append(strings, string(data[start:]))
		}

		// Limit to prevent huge dicts in fuzzing.
		if len(strings) > 1000 {
			strings = strings[:1000]
		}

		// Track expected string→ID mapping.
		expectedID := make(map[string]uint32)

		for _, s := range strings {
			id, err := dict.Add(s)
			if err != nil {
				// ErrDictFull is acceptable (though unlikely with <1000 entries).
				if err == ErrDictFull {
					return
				}
				t.Fatalf("Add(%q): %v", s, err)
			}

			if prev, ok := expectedID[s]; ok {
				// Duplicate: must return same ID.
				if id != prev {
					t.Fatalf("Add(%q): duplicate returned id=%d, want %d", s, id, prev)
				}
			} else {
				expectedID[s] = id
			}

			// Verify Get returns the original string.
			got, err := dict.Get(id)
			if err != nil {
				t.Fatalf("Get(%d): %v", id, err)
			}
			if got != s {
				t.Fatalf("Get(%d): got %q, want %q", id, got, s)
			}
		}

		// Verify Len matches unique strings.
		if dict.Len() != len(expectedID) {
			t.Fatalf("Len: got %d, want %d", dict.Len(), len(expectedID))
		}

		// Verify Lookup for all registered strings.
		for s, wantID := range expectedID {
			gotID, ok := dict.Lookup(s)
			if !ok {
				t.Fatalf("Lookup(%q): not found", s)
			}
			if gotID != wantID {
				t.Fatalf("Lookup(%q): got %d, want %d", s, gotID, wantID)
			}
		}
	})
}
