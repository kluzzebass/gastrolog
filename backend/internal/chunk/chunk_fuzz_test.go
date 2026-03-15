package chunk

import "testing"

func FuzzParseChunkID(f *testing.F) {
	// Seed corpus: valid and invalid chunk ID strings.
	seeds := []string{
		// Valid: 26-char base32hex strings
		"0123456789abcdefghijklmnop",
		"00000000000000000000000000",
		"vvvvvvvvvvvvvvvvvvvvvvvvvv",
		// Wrong lengths
		"",
		"a",
		"abcdefghijklmnopqrstuvwxy",  // 25 chars
		"abcdefghijklmnopqrstuvwxyz0", // 27 chars
		// Invalid characters (base32hex uses 0-9, a-v)
		"wwwwwwwwwwwwwwwwwwwwwwwwww", // 'w' is invalid in base32hex
		"zzzzzzzzzzzzzzzzzzzzzzzzzz",
		"!!!!!!!!!!!!!!!!!!!!!!!!!!",
		// Mixed case
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ",
		"AbCdEfGhIjKlMnOpQrStUvWx01",
		// Binary / control chars
		"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
		"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, input string) {
		// ParseChunkID must not panic on any input.
		// It should return either a valid ChunkID or an error.
		id, err := ParseChunkID(input)
		if err == nil {
			// If parsing succeeded, roundtrip must produce the same string.
			roundtrip := id.String()
			id2, err2 := ParseChunkID(roundtrip)
			if err2 != nil {
				t.Fatalf("roundtrip failed: ParseChunkID(%q) returned error: %v", roundtrip, err2)
			}
			if id != id2 {
				t.Fatalf("roundtrip mismatch: %v != %v", id, id2)
			}
		}
	})
}
