// Package level provides a digester that extracts severity/level from
// log message bodies and sets a normalized "level" attribute.
package level

import (
	"bytes"

	"gastrolog/internal/orchestrator"
)

// Digester extracts severity from message text and sets a "level" attr.
// It recognizes three patterns:
//   - KV format:   level=ERROR, severity=warn
//   - JSON format: "level":"error", "severity":"warn"
//   - Syslog:      <priority> where severity = priority % 8
//
// The extracted value is normalized to one of: error, warn, info, debug, trace.
// If a level/severity attr is already present, the message is left unchanged.
type Digester struct{}

// New creates a level digester.
func New() *Digester { return &Digester{} }

func (d *Digester) Digest(msg *orchestrator.IngestMessage) {
	// Skip if level/severity already set by ingester.
	if _, ok := msg.Attrs["level"]; ok {
		return
	}
	if _, ok := msg.Attrs["severity"]; ok {
		return
	}
	if _, ok := msg.Attrs["severity_name"]; ok {
		return
	}

	lvl := extractLevel(msg.Raw)
	if lvl == "" {
		return
	}

	if msg.Attrs == nil {
		msg.Attrs = make(map[string]string)
	}
	msg.Attrs["level"] = lvl
}

// extractLevel tries multiple strategies to find a severity level in raw.
func extractLevel(raw []byte) string {
	// 1. Try syslog priority: <NNN> at start of line.
	if lvl := extractSyslogPriority(raw); lvl != "" {
		return lvl
	}

	// 2. Try KV or JSON patterns.
	if lvl := extractKVOrJSON(raw); lvl != "" {
		return lvl
	}

	return ""
}

// extractSyslogPriority parses <priority> at the start of a message
// and derives severity from priority % 8.
func extractSyslogPriority(raw []byte) string {
	if len(raw) < 3 || raw[0] != '<' {
		return ""
	}

	// Parse digits between < and >.
	i := 1
	for i < len(raw) && i < 5 && raw[i] >= '0' && raw[i] <= '9' {
		i++
	}
	if i == 1 || i >= len(raw) || raw[i] != '>' {
		return ""
	}

	// Convert to integer.
	priority := 0
	for _, b := range raw[1:i] {
		priority = priority*10 + int(b-'0')
	}

	severity := priority % 8
	switch severity {
	case 0, 1, 2, 3: // emerg, alert, crit, err
		return "error"
	case 4: // warning
		return "warn"
	case 5, 6: // notice, info
		return "info"
	case 7: // debug
		return "debug"
	default:
		return ""
	}
}

// extractKVOrJSON looks for level= or "level": patterns in the message.
func extractKVOrJSON(raw []byte) string {
	// Search for level/severity keys in both KV and JSON formats.
	for _, key := range [][]byte{
		[]byte("level"),
		[]byte("severity"),
	} {
		if lvl := findKeyValue(raw, key); lvl != "" {
			return lvl
		}
	}
	return ""
}

// findKeyValue searches for key=value or "key":"value" patterns.
func findKeyValue(raw, key []byte) string {
	pos := 0
	for pos < len(raw) {
		// Find next occurrence of the key.
		idx := bytes.Index(raw[pos:], key)
		if idx < 0 {
			return ""
		}
		idx += pos
		keyEnd := idx + len(key)

		// Check we're at a word boundary (not mid-word).
		if idx > 0 && isWordChar(raw[idx-1]) {
			pos = keyEnd
			continue
		}

		rest := raw[keyEnd:]
		if len(rest) == 0 {
			return ""
		}

		var val string
		switch {
		case rest[0] == '=':
			// KV format: key=value or key="value"
			val = extractValueAfterSep(rest[1:])
		case rest[0] == '"' && len(rest) > 1 && rest[1] == ':':
			// JSON format: key": "value" (key was unquoted match)
			val = extractJSONValue(rest[2:])
		case rest[0] == ':':
			// JSON without quotes around key, or YAML-like: key: value
			val = extractJSONValue(rest[1:])
		default:
			pos = keyEnd
			continue
		}

		if normalized := normalize(val); normalized != "" {
			return normalized
		}
		pos = keyEnd
	}
	return ""
}

// extractValueAfterSep extracts a value after = separator.
func extractValueAfterSep(rest []byte) string {
	if len(rest) == 0 {
		return ""
	}
	// Skip optional quote.
	if rest[0] == '"' || rest[0] == '\'' {
		quote := rest[0]
		end := bytes.IndexByte(rest[1:], quote)
		if end < 0 {
			return ""
		}
		return string(rest[1 : 1+end])
	}
	// Unquoted: read until whitespace, comma, or end.
	end := 0
	for end < len(rest) && !isDelimiter(rest[end]) {
		end++
	}
	return string(rest[:end])
}

// extractJSONValue extracts a value after : separator (JSON-style).
func extractJSONValue(rest []byte) string {
	// Skip whitespace.
	i := 0
	for i < len(rest) && (rest[i] == ' ' || rest[i] == '\t') {
		i++
	}
	if i >= len(rest) {
		return ""
	}
	// Expect quoted string.
	if rest[i] == '"' || rest[i] == '\'' {
		quote := rest[i]
		end := bytes.IndexByte(rest[i+1:], quote)
		if end < 0 {
			return ""
		}
		return string(rest[i+1 : i+1+end])
	}
	// Unquoted value (unusual for JSON but handle it).
	start := i
	for i < len(rest) && !isDelimiter(rest[i]) {
		i++
	}
	return string(rest[start:i])
}

// normalize maps a raw level string to a canonical value.
func normalize(val string) string {
	// Fast lowercase for short ASCII strings.
	lower := make([]byte, len(val))
	for i := 0; i < len(val); i++ {
		c := val[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		lower[i] = c
	}
	s := string(lower)

	switch s {
	case "error", "err", "fatal", "critical", "emerg", "emergency", "alert", "crit":
		return "error"
	case "warn", "warning":
		return "warn"
	case "info", "notice", "informational":
		return "info"
	case "debug":
		return "debug"
	case "trace":
		return "trace"
	default:
		return ""
	}
}

func isWordChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func isDelimiter(b byte) bool {
	return b == ' ' || b == '\t' || b == ',' || b == ';' || b == '}' || b == ']' || b == '\n' || b == '\r'
}
