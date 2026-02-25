package querylang

import "strings"

// StripComments removes # line comments from a query string.
// Everything from an unquoted # to the end of that line is removed.
// Characters inside quoted strings ("..." or '...') and regex
// literals (/.../) are not treated as comment starts.
func StripComments(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))

	inSingle := false
	inDouble := false
	inRegex := false
	escaped := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if escaped {
			buf.WriteByte(c)
			escaped = false
			continue
		}

		if c == '\\' && (inSingle || inDouble || inRegex) {
			buf.WriteByte(c)
			escaped = true
			continue
		}

		if c == '"' && !inSingle && !inRegex {
			inDouble = !inDouble
			buf.WriteByte(c)
			continue
		}

		if c == '\'' && !inDouble && !inRegex {
			inSingle = !inSingle
			buf.WriteByte(c)
			continue
		}

		if c == '/' && !inSingle && !inDouble {
			inRegex = !inRegex
			buf.WriteByte(c)
			continue
		}

		if c == '#' && !inSingle && !inDouble && !inRegex {
			// Skip to end of line.
			for i < len(s) && s[i] != '\n' {
				i++
			}
			// Preserve the newline to maintain line structure.
			if i < len(s) {
				buf.WriteByte('\n')
			}
			continue
		}

		buf.WriteByte(c)
	}

	return buf.String()
}
