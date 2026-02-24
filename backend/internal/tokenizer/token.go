package tokenizer

const (
	minTokenLen        = 2
	DefaultMaxTokenLen = 16
)

// IterTokens calls fn for each indexable token in data, passing the raw bytes.
// The byte slice passed to fn is reused between calls and must not be retained.
// If fn returns false, iteration stops early.
//
// The buf parameter is a reusable buffer for building tokens. Pass nil to
// allocate a new buffer, or pass a slice with capacity >= maxLen for zero allocations.
//
// The maxLen parameter sets the maximum token length. Tokens longer than this
// are truncated. Use DefaultMaxTokenLen for the standard limit.
//
// Token characters (ASCII only):
//   - 'a'-'z', 'A'-'Z' (lowercased)
//   - '0'-'9'
//   - '_', '-'
//
// All other bytes are delimiters.
// Tokens must be 2-16 bytes. Numeric and UUID tokens are excluded.
func IterTokens(data []byte, buf []byte, maxLen int, fn func(token []byte) bool) {
	if len(data) == 0 {
		return
	}
	if maxLen <= 0 {
		maxLen = DefaultMaxTokenLen
	}

	current := buf[:0]
	if cap(current) < maxLen {
		current = make([]byte, 0, maxLen)
	}

	for _, b := range data {
		if isTokenByte(b) {
			if len(current) < maxLen {
				current = append(current, Lowercase(b))
			}
			continue
		}
		if len(current) >= minTokenLen && isIndexable(current) && !fn(current) {
			return
		}
		current = current[:0]
	}

	if len(current) >= minTokenLen && isIndexable(current) {
		fn(current)
	}
}

// Tokens extracts indexable tokens from raw log data using DefaultMaxTokenLen.
func Tokens(data []byte) []string {
	return TokensWithMaxLen(data, DefaultMaxTokenLen)
}

// TokensWithMaxLen extracts indexable tokens with a custom max length.
func TokensWithMaxLen(data []byte, maxLen int) []string {
	if len(data) == 0 {
		return nil
	}
	if maxLen <= 0 {
		maxLen = DefaultMaxTokenLen
	}

	current := make([]byte, 0, maxLen)
	var tokens []string

	for _, b := range data {
		if isTokenByte(b) {
			if len(current) < maxLen {
				current = append(current, Lowercase(b))
			}
			continue
		}
		if len(current) >= minTokenLen && isIndexable(current) {
			tokens = appendToken(tokens, current, len(data))
		}
		current = current[:0]
	}

	if len(current) >= minTokenLen && isIndexable(current) {
		tokens = appendToken(tokens, current, len(data))
	}

	return tokens
}

func appendToken(tokens []string, current []byte, dataLen int) []string {
	if tokens == nil {
		tokens = make([]string, 0, dataLen/8)
	}
	return append(tokens, string(current))
}

// isTokenByte returns true if b is a valid token character.
// Only ASCII: a-z, A-Z, 0-9, underscore, hyphen.
func isTokenByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b == '_' || b == '-':
		return true
	default:
		return false
	}
}

// isIndexable returns true if the token should be indexed.
// Excludes numeric-like tokens and UUIDs.
func isIndexable(tok []byte) bool {
	if isNumeric(tok) {
		return false
	}
	if isUUID(tok) {
		return false
	}
	return true
}

// IsIndexable reports whether a query token would be present in the token index
// if it appeared in a record. Returns false for tokens the indexer skips:
// too short, too long, numeric, UUID, non-ASCII, or containing non-token bytes.
// When this returns true and the token is not found in a sealed chunk's index,
// the token does not appear in that chunk — zero matches, no runtime scan needed.
func IsIndexable(token string) bool {
	if len(token) < minTokenLen || len(token) > DefaultMaxTokenLen {
		return false
	}
	// Check all bytes are valid token bytes (ASCII letters, digits, underscore, hyphen).
	for i := range len(token) {
		if !isTokenByte(token[i]) {
			return false
		}
	}
	return isIndexable([]byte(token))
}

// isNumeric returns true if tok looks like a number in any common base.
// This includes pure decimal, hex (0x prefix or all hex digits), octal, binary.
// Also catches hex-with-hyphens (like UUIDs or partial UUIDs).
func isNumeric(tok []byte) bool {
	if len(tok) == 0 {
		return false
	}

	// Check for prefixed forms: 0x, 0o, 0b
	if len(tok) >= 2 && tok[0] == '0' {
		switch tok[1] {
		case 'x', 'b':
			// 0x... or 0b... - check rest are valid digits
			return isPrefixedNumber(tok)
		case 'o':
			// 0o... octal
			return isPrefixedNumber(tok)
		}
	}

	// Check if all characters are hex digits or hyphens
	// This catches: "15", "404", "deadbeef", "a1b2c3d4", "019c0bc0-d19f-77db", etc.
	allHexOrHyphen := true
	hasHex := false
	for _, b := range tok {
		if IsHexDigit(b) {
			hasHex = true
		} else if b != '-' {
			allHexOrHyphen = false
			break
		}
	}
	if allHexOrHyphen && hasHex {
		return true
	}

	return false
}

// isPrefixedNumber checks if tok is a valid 0x/0o/0b prefixed number.
func isPrefixedNumber(tok []byte) bool {
	if len(tok) < 3 {
		return false
	}
	base := tok[1]
	for _, b := range tok[2:] {
		switch base {
		case 'x':
			if !IsHexDigit(b) {
				return false
			}
		case 'o':
			if b < '0' || b > '7' {
				return false
			}
		case 'b':
			if b != '0' && b != '1' {
				return false
			}
		}
	}
	return true
}

// isUUID checks if tok matches the canonical UUID format: 8-4-4-4-12 hex digits.
// Example: 019c0bc0-d19f-77db-bbdf-4c36766e13ca
func isUUID(tok []byte) bool {
	// UUID is exactly 36 bytes: 8 + 1 + 4 + 1 + 4 + 1 + 4 + 1 + 12
	if len(tok) != 36 {
		return false
	}

	// Check hyphens at positions 8, 13, 18, 23
	if tok[8] != '-' || tok[13] != '-' || tok[18] != '-' || tok[23] != '-' {
		return false
	}

	// Check hex digits at all other positions
	for i, b := range tok {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			continue
		}
		if !IsHexDigit(b) {
			return false
		}
	}

	return true
}
