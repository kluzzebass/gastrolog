// Package safeutf8 sanitizes strings at the proto boundary.
//
// Proto3 string fields must be valid UTF-8 or marshal fails at runtime.
// Log record content is arbitrary bytes (partial UTF-8 at buffer
// boundaries, escape sequences, binary payloads in stdout), and
// extracted KV keys/values, group-by keys, and attrs can carry that
// invalidity all the way to the wire. Sanitize just before the string
// becomes a proto field.
package safeutf8

import (
	"strings"
	"unicode/utf8"
)

// String returns s unchanged if it is already valid UTF-8; otherwise
// each invalid byte sequence is replaced with U+FFFD ("�").
func String(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	return strings.ToValidUTF8(s, "�")
}

// Strings returns s unchanged (shared reference) when every element is
// already valid UTF-8 — the common case. Otherwise allocates a fresh
// slice with each element sanitized.
func Strings(s []string) []string {
	if s == nil {
		return nil
	}
	for _, v := range s {
		if !utf8.ValidString(v) {
			out := make([]string, len(s))
			for i, vv := range s {
				out[i] = String(vv)
			}
			return out
		}
	}
	return s
}

// Attrs returns m unchanged (shared reference) when every key and value
// is already valid UTF-8 — the common case. Otherwise allocates a fresh
// map with each key and value sanitized.
func Attrs(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	for k, v := range m {
		if !utf8.ValidString(k) || !utf8.ValidString(v) {
			out := make(map[string]string, len(m))
			for kk, vv := range m {
				out[String(kk)] = String(vv)
			}
			return out
		}
	}
	return m
}
