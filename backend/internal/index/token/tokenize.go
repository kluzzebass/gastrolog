package token

// Tokenize extracts tokens from raw log data.
//
// Token rules:
//   - Word bytes: a-z, A-Z (lowercased), 0-9, 0x80-0x9F, 0xA1-0xFF
//   - Delimiters: everything else (ASCII control/punctuation/space, 0xA0)
//   - Tokens shorter than 2 bytes are skipped
func Tokenize(data []byte) []string {
	var tokens []string
	var current []byte

	for _, b := range data {
		if isWordByte(b) {
			current = append(current, lowercase(b))
		} else {
			if len(current) >= 2 {
				tokens = append(tokens, string(current))
			}
			current = current[:0]
		}
	}

	// Don't forget trailing token.
	if len(current) >= 2 {
		tokens = append(tokens, string(current))
	}

	return tokens
}

// isWordByte returns true if b is part of a token.
func isWordByte(b byte) bool {
	switch {
	case b >= 'a' && b <= 'z':
		return true
	case b >= 'A' && b <= 'Z':
		return true
	case b >= '0' && b <= '9':
		return true
	case b >= 0x80 && b <= 0x9F:
		return true
	case b >= 0xA1: // 0xA1-0xFF (excludes 0xA0 non-breaking space)
		return true
	default:
		return false
	}
}

// lowercase converts ASCII uppercase to lowercase, leaves other bytes unchanged.
func lowercase(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}
