package querylang

import "testing"

func FuzzCompileGlob(f *testing.F) {
	// Seed corpus: various glob patterns.
	seeds := []string{
		// Simple patterns
		"*",
		"?",
		"hello",
		"hello*",
		"*world",
		"he?lo",
		"*.log",
		"test-*-file",
		// Character classes
		"[abc]",
		"[a-z]",
		"[!abc]",
		"[!a-z]",
		"[]]", // literal ] at start
		// Nested / combined
		"*[abc]*",
		"?[!x]?",
		"[a-z]*[0-9]",
		// Edge cases
		"",
		"[",
		"[abc",
		"]",
		"[!",
		"[!]",
		// Special regex chars that need escaping
		"hello.world",
		"test+file",
		"a^b$c",
		"(test)",
		"{a,b}",
		`file\name`,
		// Long patterns
		"*****",
		"?????",
		"[abc][def][ghi]",
		// Binary / control chars
		"\x00",
		"\xff",
		"\n\t\r",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, pattern string) {
		// CompileGlob must not panic on any input.
		// It should return either a compiled regex or an error.
		_, _ = CompileGlob(pattern)
	})
}

func FuzzExtractGlobPrefix(f *testing.F) {
	seeds := []string{
		"",
		"hello",
		"*",
		"hello*",
		"?world",
		"[abc]test",
		"prefix*suffix",
		"no-meta-chars",
	}

	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, pattern string) {
		// ExtractGlobPrefix must not panic on any input.
		_, _ = ExtractGlobPrefix(pattern)
	})
}
