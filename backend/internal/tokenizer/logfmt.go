package tokenizer

import "bytes"

// ExtractLogfmt parses a log message as logfmt and extracts key=value pairs.
// Returns nil if the message does not appear to be logfmt.
//
// Logfmt grammar (per kr/logfmt):
//
//	key   := ident
//	value := ident | '"' quoted_string '"'
//	ident := byte > ' ', excluding '=' and '"'
//	pair  := key '=' value | key '=' | key (bare key → true)
//
// This is more permissive than the heuristic KV extractor: keys may contain
// hyphens, colons, slashes, and other printable characters. Bare keys (without
// '=') emit key=true.
func ExtractLogfmt(msg []byte) []KeyValue {
	if !isLogfmt(msg) {
		return nil
	}

	var result []KeyValue
	seen := make(map[string]struct{})
	i := 0

	for i < len(msg) {
		i = skipLogfmtWhitespace(msg, i)
		if i >= len(msg) {
			break
		}

		keyStart, keyEnd, next := parseLogfmtKey(msg, i)
		i = next

		keyLen := keyEnd - keyStart
		if keyLen == 0 {
			// Invalid key — skip past any =value that follows.
			for i < len(msg) && !IsWhitespace(msg[i]) {
				i++
			}
			continue
		}
		if keyLen > MaxKeyLength {
			for i < len(msg) && !IsWhitespace(msg[i]) {
				i++
			}
			continue
		}

		if i >= len(msg) || msg[i] != '=' {
			key := ToLowerASCII(msg[keyStart:keyEnd])
			addLogfmtPair(&result, seen, key, "true")
			continue
		}
		i++ // skip '='

		if i >= len(msg) || IsWhitespace(msg[i]) {
			continue
		}

		value, next := parseLogfmtValue(msg, i)
		i = next
		if value == "" {
			continue
		}

		key := ToLowerASCII(msg[keyStart:keyEnd])
		addLogfmtPair(&result, seen, key, value)
	}

	if len(result) == 0 {
		return nil
	}
	return result
}

func isLogfmt(msg []byte) bool {
	if len(msg) == 0 {
		return false
	}
	first := skipSpaces(msg)
	if first < len(msg) && (msg[first] == '{' || msg[first] == '[' || msg[first] == '<') {
		return false
	}
	return bytes.ContainsRune(msg, '=')
}

func skipLogfmtWhitespace(msg []byte, i int) int {
	for i < len(msg) && IsWhitespace(msg[i]) {
		i++
	}
	return i
}

func parseLogfmtKey(msg []byte, i int) (keyStart, keyEnd, next int) {
	keyStart = i
	for i < len(msg) && msg[i] > ' ' && msg[i] != '=' && msg[i] != '"' {
		i++
	}
	// Validate: key must match [A-Za-z_][A-Za-z0-9_\-.]* to filter out
	// punctuation and structured-data fragments that the permissive logfmt
	// spec would otherwise accept.
	if !isValidLogfmtKey(msg[keyStart:i]) {
		return keyStart, keyStart, i // zero-length key → skipped by caller
	}
	return keyStart, i, i
}

// isValidLogfmtKey checks that a logfmt key looks like a real field name:
// starts with letter or underscore, rest may contain letters, digits,
// underscores, hyphens, dots, colons, and slashes (for k8s annotations,
// Prometheus labels, etc.).
func isValidLogfmtKey(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	c := key[0]
	if !IsLetter(c) && c != '_' {
		return false
	}
	for i := 1; i < len(key); i++ {
		c := key[i]
		if !IsLetter(c) && !IsDigit(c) && c != '_' && c != '-' && c != '.' && c != ':' && c != '/' {
			return false
		}
	}
	return true
}

func parseLogfmtValue(msg []byte, i int) (string, int) {
	if msg[i] == '"' {
		return parseLogfmtQuotedValue(msg, i)
	}
	return parseLogfmtUnquotedValue(msg, i)
}

func parseLogfmtQuotedValue(msg []byte, i int) (string, int) {
	i++ // skip opening quote
	var buf []byte
	for i < len(msg) && msg[i] != '"' {
		if msg[i] == '\\' && i+1 < len(msg) && (msg[i+1] == '"' || msg[i+1] == '\\') {
			buf = append(buf, msg[i+1])
			i += 2
			continue
		}
		buf = append(buf, msg[i])
		i++
	}
	if i < len(msg) {
		i++ // skip closing quote
	}
	if len(buf) == 0 || len(buf) > MaxValueLength {
		return "", i
	}
	return ToLowerASCII(buf), i
}

func parseLogfmtUnquotedValue(msg []byte, i int) (string, int) {
	valStart := i
	for i < len(msg) && msg[i] > ' ' && msg[i] != '=' && msg[i] != '"' {
		i++
	}
	valLen := i - valStart
	if valLen == 0 || valLen > MaxValueLength {
		return "", i
	}
	return ToLowerASCII(msg[valStart:i]), i
}

func addLogfmtPair(result *[]KeyValue, seen map[string]struct{}, key, value string) {
	dedup := key + "\x00" + value
	if _, ok := seen[dedup]; ok {
		return
	}
	seen[dedup] = struct{}{}
	*result = append(*result, KeyValue{Key: key, Value: value})
}

// skipSpaces returns the index of the first non-whitespace byte.
func skipSpaces(msg []byte) int {
	i := 0
	for i < len(msg) && IsWhitespace(msg[i]) {
		i++
	}
	return i
}
