package tokenizer

import "testing"

func FuzzTokens(f *testing.F) {
	// Seed corpus: various log-like byte sequences.
	seeds := []string{
		"",
		"hello",
		"level=error status=500",
		"2024-01-15T10:30:00Z INFO request completed",
		`{"key":"value","count":42}`,
		"GET /api/v1/users HTTP/1.1 200 1234",
		"019c0bc0-d19f-77db-bbdf-4c36766e13ca", // UUID
		"0xdeadbeef",
		"0b10101010",
		"0o777",
		"a]b[c{d}e(f)g",
		"\x00\x01\x02\xff\xfe\xfd",
		"a-b_c",
		"AB CD EF",
		"    ",
		"\t\n\r",
		"a",  // too short
		"ab", // minimum length
		"aaaaaaaaaaaaaaaa",  // exactly max length
		"aaaaaaaaaaaaaaaaa", // exceeds max length
	}

	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Tokens must not panic on any input.
		_ = Tokens(data)
	})
}

func FuzzIterTokens(f *testing.F) {
	seeds := []string{
		"",
		"hello world",
		"key=value foo bar",
		"\x00\xff",
	}

	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		// IterTokens must not panic on any input.
		IterTokens(data, nil, DefaultMaxTokenLen, func(token []byte) bool {
			return true
		})
	})
}
