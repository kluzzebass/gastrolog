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
	// Quick reject: empty, or looks like JSON.
	if len(msg) == 0 {
		return nil
	}
	first := skipSpaces(msg)
	if first < len(msg) && (msg[first] == '{' || msg[first] == '[' || msg[first] == '<') {
		return nil
	}
	// Must contain at least one '=' to be logfmt.
	if !bytes.ContainsRune(msg, '=') {
		return nil
	}

	var result []KeyValue
	seen := make(map[string]struct{})
	i := 0

	for i < len(msg) {
		// Skip whitespace between pairs.
		for i < len(msg) && IsWhitespace(msg[i]) {
			i++
		}
		if i >= len(msg) {
			break
		}

		// Parse key: ident bytes (> ' ', not '=' or '"').
		keyStart := i
		for i < len(msg) && msg[i] > ' ' && msg[i] != '=' && msg[i] != '"' {
			i++
		}
		keyEnd := i

		keyLen := keyEnd - keyStart
		if keyLen == 0 {
			// Not a valid key char — skip this byte.
			i++
			continue
		}
		if keyLen > MaxKeyLength {
			// Key too long — skip to next whitespace.
			for i < len(msg) && !IsWhitespace(msg[i]) {
				i++
			}
			continue
		}

		if i >= len(msg) || msg[i] != '=' {
			// Bare key (no '='): emit key=true.
			key := ToLowerASCII(msg[keyStart:keyEnd])
			addLogfmtPair(&result, seen, key, "true")
			continue
		}

		// Skip '='.
		i++

		// Parse value.
		if i >= len(msg) || IsWhitespace(msg[i]) {
			// Empty value (key=): skip, nothing useful to index.
			continue
		}

		var value string
		if msg[i] == '"' {
			// Quoted value with escape handling.
			i++ // skip opening quote
			var buf []byte
			for i < len(msg) && msg[i] != '"' {
				if msg[i] == '\\' && i+1 < len(msg) {
					next := msg[i+1]
					if next == '"' || next == '\\' {
						buf = append(buf, next)
						i += 2
						continue
					}
				}
				buf = append(buf, msg[i])
				i++
			}
			if i < len(msg) {
				i++ // skip closing quote
			}
			if len(buf) == 0 || len(buf) > MaxValueLength {
				continue
			}
			value = ToLowerASCII(buf)
		} else {
			// Unquoted value: ident bytes.
			valStart := i
			for i < len(msg) && msg[i] > ' ' && msg[i] != '=' && msg[i] != '"' {
				i++
			}
			valLen := i - valStart
			if valLen == 0 || valLen > MaxValueLength {
				continue
			}
			value = ToLowerASCII(msg[valStart:i])
		}

		key := ToLowerASCII(msg[keyStart:keyEnd])
		addLogfmtPair(&result, seen, key, value)
	}

	if len(result) == 0 {
		return nil
	}
	return result
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
