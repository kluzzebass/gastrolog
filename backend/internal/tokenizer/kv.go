package tokenizer

// KeyValue represents an extracted key=value pair from a log message.
type KeyValue struct {
	Key   string
	Value string
}

// MaxKeyLength is the maximum allowed key length in bytes.
const MaxKeyLength = 64

// MaxValueLength is the maximum allowed value length in bytes.
const MaxValueLength = 64

// ExtractKeyValues extracts key=value pairs from a log message.
// Both keys and values are normalized to lowercase for case-insensitive matching.
//
// Key grammar:
//
//	key := segment ( "." segment )*
//	segment := [A-Za-z_][A-Za-z0-9_]*
//
// Value grammar:
//   - Bytes until whitespace or delimiter
//   - ASCII only, no quoting or escaping
//   - Max 64 bytes
//
// This function is conservative: false negatives are acceptable,
// false positives should be rare.
func ExtractKeyValues(msg []byte) []KeyValue {
	var results []KeyValue
	seen := make(map[string]struct{}) // dedupe within message

	i := 0
	for i < len(msg) {
		// Find next '='
		eqPos := -1
		for j := i; j < len(msg); j++ {
			if msg[j] == '=' {
				eqPos = j
				break
			}
		}
		if eqPos == -1 {
			break
		}

		// Extract and validate key (bytes before '=')
		keyStart := findKeyStart(msg, eqPos)
		if keyStart == -1 {
			i = eqPos + 1
			continue
		}

		keyBytes := msg[keyStart:eqPos]
		if len(keyBytes) == 0 || len(keyBytes) > MaxKeyLength {
			i = eqPos + 1
			continue
		}

		if !isValidKey(keyBytes) {
			i = eqPos + 1
			continue
		}

		// Extract and validate value (bytes after '=')
		valueStart := eqPos + 1
		valueEnd := findValueEnd(msg, valueStart)
		valueBytes := msg[valueStart:valueEnd]

		if len(valueBytes) == 0 || len(valueBytes) > MaxValueLength {
			i = valueEnd
			continue
		}

		if !isValidValue(valueBytes) {
			i = valueEnd
			continue
		}

		// Normalize both key and value to lowercase for case-insensitive matching.
		key := ToLowerASCII(keyBytes)
		value := ToLowerASCII(valueBytes)

		// Dedupe within message
		kvKey := key + "\x00" + value
		if _, ok := seen[kvKey]; !ok {
			seen[kvKey] = struct{}{}
			results = append(results, KeyValue{Key: key, Value: value})
		}

		i = valueEnd
	}

	return results
}

// findKeyStart finds the start of a potential key ending at eqPos.
// Returns -1 if no valid key start is found.
func findKeyStart(msg []byte, eqPos int) int {
	if eqPos == 0 {
		return -1
	}

	// Scan backwards to find start of key
	start := eqPos - 1
	for start > 0 {
		c := msg[start-1]
		if isKeyChar(c) || c == '.' {
			start--
		} else {
			break
		}
	}

	// Key must not start with delimiter or at position that follows non-whitespace/non-delimiter
	if start > 0 {
		prev := msg[start-1]
		if !isDelimiter(prev) && !IsWhitespace(prev) {
			return -1
		}
	}

	return start
}

// findValueEnd finds the end of a value starting at valueStart.
func findValueEnd(msg []byte, valueStart int) int {
	end := valueStart
	for end < len(msg) {
		c := msg[end]
		if IsWhitespace(c) || isValueDelimiter(c) {
			break
		}
		end++
	}
	return end
}

// isValidKey validates that keyBytes matches the key grammar.
// key := segment ( "." segment )*
// segment := [A-Za-z_][A-Za-z0-9_]*
func isValidKey(keyBytes []byte) bool {
	if len(keyBytes) == 0 {
		return false
	}

	// Must not start or end with '.'
	if keyBytes[0] == '.' || keyBytes[len(keyBytes)-1] == '.' {
		return false
	}

	// Check for empty segments (consecutive dots)
	for i := 0; i < len(keyBytes)-1; i++ {
		if keyBytes[i] == '.' && keyBytes[i+1] == '.' {
			return false
		}
	}

	// Validate each segment
	segmentStart := 0
	for i := 0; i <= len(keyBytes); i++ {
		if i == len(keyBytes) || keyBytes[i] == '.' {
			segment := keyBytes[segmentStart:i]
			if !isValidSegment(segment) {
				return false
			}
			segmentStart = i + 1
		}
	}

	return true
}

// isValidSegment validates a single key segment.
// segment := [A-Za-z_][A-Za-z0-9_]*
func isValidSegment(segment []byte) bool {
	if len(segment) == 0 {
		return false
	}

	// First char must be letter or underscore
	c := segment[0]
	if !IsLetter(c) && c != '_' {
		return false
	}

	// Rest must be alphanumeric or underscore
	for i := 1; i < len(segment); i++ {
		c := segment[i]
		if !IsLetter(c) && !IsDigit(c) && c != '_' {
			return false
		}
	}

	return true
}

// isValidValue validates that valueBytes is acceptable.
// ASCII only, no special characters that indicate structured data.
func isValidValue(valueBytes []byte) bool {
	if len(valueBytes) == 0 {
		return false
	}

	for _, c := range valueBytes {
		// ASCII only
		if c > 127 {
			return false
		}
		// Reject characters that indicate structured data
		if c == '{' || c == '}' || c == '[' || c == ']' || c == '"' || c == '\'' {
			return false
		}
		// Reject additional '=' in value (e.g., a=b=c)
		if c == '=' {
			return false
		}
		// Reject '&' which indicates URL params (e.g., x=y&z=w)
		if c == '&' {
			return false
		}
	}

	return true
}

// isKeyChar returns true if c is valid within a key segment (letter, digit, or underscore).
func isKeyChar(c byte) bool {
	return IsLetter(c) || IsDigit(c) || c == '_'
}

// isDelimiter returns true if c is a delimiter that can precede a key.
func isDelimiter(c byte) bool {
	return IsWhitespace(c) || c == ',' || c == ';' || c == ':' || c == '(' || c == ')' || c == '[' || c == ']' || c == '{' || c == '}'
}

// isValueDelimiter returns true if c terminates a value.
func isValueDelimiter(c byte) bool {
	return c == ',' || c == ';' || c == ')' || c == ']' || c == '}'
}
