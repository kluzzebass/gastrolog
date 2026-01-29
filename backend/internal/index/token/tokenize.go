package token

// IterBytes calls fn for each token in data, passing the raw bytes.
// The byte slice passed to fn is reused between calls and must not be retained.
// If fn returns false, iteration stops early.
//
// The buf parameter is a reusable buffer for building tokens. Pass nil to
// allocate a new buffer, or pass a slice with capacity >= 64 for zero allocations.
//
// Token rules are the same as Simple().
func IterBytes(data []byte, buf []byte, fn func(token []byte) bool) {
	if len(data) == 0 {
		return
	}

	current := buf[:0]
	if cap(current) < 64 {
		current = make([]byte, 0, 64)
	}

	for _, b := range data {
		if isWordByte(b) {
			current = append(current, lowercase(b))
		} else {
			if len(current) >= 2 {
				if !fn(current) {
					return
				}
			}
			current = current[:0]
		}
	}

	// Don't forget trailing token.
	if len(current) >= 2 {
		fn(current)
	}
}

// Simple extracts tokens from raw log data using basic byte-level rules.
//
// Token rules:
//   - Word bytes: a-z, A-Z (lowercased), 0-9, any byte >= 0x80 (except 0xA0)
//   - Delimiters: everything else (ASCII control/punctuation/space, 0xA0)
//   - Tokens shorter than 2 bytes are skipped
func Simple(data []byte) []string {
	if len(data) == 0 {
		return nil
	}

	// Pre-allocate buffer for building tokens. Max token length 64 bytes.
	current := make([]byte, 0, 64)

	// Delay slice allocation until we find the first token.
	var tokens []string

	for _, b := range data {
		if isWordByte(b) {
			current = append(current, lowercase(b))
		} else {
			if len(current) >= 2 {
				if tokens == nil {
					// Allocate on first token. Estimate ~1 token per 8 bytes.
					tokens = make([]string, 0, len(data)/8)
				}
				tokens = append(tokens, string(current))
			}
			current = current[:0]
		}
	}

	// Don't forget trailing token.
	if len(current) >= 2 {
		if tokens == nil {
			tokens = make([]string, 0, 1)
		}
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
