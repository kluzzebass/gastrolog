// Package tokenizer provides extraction of searchable elements from raw log data.
//
// This package contains tokenizers for different use cases:
//   - Token extraction: splits log text into indexable tokens
//   - KV extraction: extracts key=value pairs from log messages
package tokenizer

// Character classification functions shared across tokenizers.

// IsLetter returns true if c is an ASCII letter (A-Z or a-z).
func IsLetter(c byte) bool {
	return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
}

// IsDigit returns true if c is an ASCII digit (0-9).
func IsDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// IsHexDigit returns true if c is a hex digit (0-9 or a-f).
func IsHexDigit(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

// IsWhitespace returns true if c is ASCII whitespace.
func IsWhitespace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r'
}

// Lowercase converts ASCII uppercase to lowercase.
// Non-uppercase bytes are returned unchanged.
func Lowercase(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

// ToLowerASCII converts a byte slice to lowercase ASCII string.
func ToLowerASCII(b []byte) string {
	result := make([]byte, len(b))
	for i, c := range b {
		result[i] = Lowercase(c)
	}
	return string(result)
}
