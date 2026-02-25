package querylang

import (
	"errors"
	"strings"
)

// directiveKeys is the set of keys that parseExpression strips before
// calling ParsePipeline. Must stay in sync with server/query.go:applyDirective.
var directiveKeys = map[string]bool{
	"reverse":      true,
	"start":        true,
	"end":          true,
	"last":         true,
	"limit":        true,
	"pos":          true,
	"source_start": true,
	"source_end":   true,
	"ingest_start": true,
	"ingest_end":   true,
}

// directiveRange records the byte range [start, end) of a directive token
// that was removed from the expression before parsing.
type directiveRange struct {
	start int // inclusive
	end   int // exclusive (includes trailing whitespace)
}

// ValidateExpression checks whether a query expression is syntactically valid.
// Returns (valid, errorMessage, errorOffset). errorOffset is the byte position
// in the original expression; -1 if valid.
func ValidateExpression(expr string) (bool, string, int) {
	if len(expr) == 0 {
		return true, "", -1
	}

	// Strip comments first, recording removed ranges for offset mapping.
	commentStripped, commentRanges := stripCommentsWithRanges(expr)

	// Strip directives the same way parseExpression does, recording removed ranges.
	stripped, removedRanges := stripDirectives(commentStripped)

	// If everything was directives, the query is valid (directive-only).
	if strings.TrimSpace(stripped) == "" {
		return true, "", -1
	}

	pipeline, err := ParsePipeline(stripped)
	if err != nil {
		var pe *ParseError
		if !errors.As(err, &pe) {
			return false, err.Error(), 0
		}
		// Map the error offset back through directive removal, then comment removal.
		posInCommentStripped := mapOffsetToOriginal(pe.Pos, removedRanges)
		originalPos := mapOffsetToOriginal(posInCommentStripped, commentRanges)
		return false, pe.Message, originalPos
	}

	_ = pipeline
	return true, "", -1
}

// stripDirectives removes directive tokens (key=value) from the expression,
// recording their positions for offset mapping. Non-directive key=value pairs
// (like level=error) are kept. Returns the stripped string and removed ranges.
func stripDirectives(expr string) (string, []directiveRange) {
	var removed []directiveRange
	var result strings.Builder
	result.Grow(len(expr))

	pos := 0
	for pos < len(expr) {
		// Skip leading whitespace, copying it to result.
		wsStart := pos
		for pos < len(expr) && (expr[pos] == ' ' || expr[pos] == '\t' || expr[pos] == '\n' || expr[pos] == '\r') {
			pos++
		}
		if pos == len(expr) {
			result.WriteString(expr[wsStart:pos])
			break
		}

		// Scan the next token (non-whitespace run).
		tokStart := pos
		for pos < len(expr) && expr[pos] != ' ' && expr[pos] != '\t' && expr[pos] != '\n' && expr[pos] != '\r' {
			pos++
		}
		token := expr[tokStart:pos]

		// Check if it's a directive: key=value where key is known.
		if k, _, ok := strings.Cut(token, "="); ok && directiveKeys[k] {
			// Record the removed range including preceding whitespace if result is empty
			// or following whitespace.
			removeStart := wsStart
			removeEnd := pos
			// If we already have content in result, also remove the preceding whitespace.
			// If we don't, the whitespace was the start of the string.
			if result.Len() == 0 {
				// Skip trailing whitespace after this directive too.
				removeStart = wsStart
			}
			removed = append(removed, directiveRange{start: removeStart, end: removeEnd})
			// Don't copy the preceding whitespace or this token.
			continue
		}

		// Not a directive — copy whitespace + token.
		result.WriteString(expr[wsStart:pos])
	}

	return result.String(), removed
}

// stripCommentsWithRanges strips # line comments from a query string and
// records the byte ranges of removed comments for offset mapping.
// Characters inside quoted strings ("..." or '...') and regex literals (/.../)
// are not treated as comment starts. Newlines after comments are preserved.
func stripCommentsWithRanges(s string) (string, []directiveRange) {
	var buf strings.Builder
	buf.Grow(len(s))

	var removed []directiveRange
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
			commentStart := i
			// Skip to end of line.
			for i < len(s) && s[i] != '\n' {
				i++
			}
			removed = append(removed, directiveRange{start: commentStart, end: i})
			// Preserve the newline to maintain line structure.
			if i < len(s) {
				buf.WriteByte('\n')
			}
			continue
		}

		buf.WriteByte(c)
	}

	return buf.String(), removed
}

// mapOffsetToOriginal maps a byte offset in the stripped string back to the
// corresponding offset in the original expression.
func mapOffsetToOriginal(strippedPos int, removed []directiveRange) int {
	originalPos := strippedPos
	for _, r := range removed {
		rangeLen := r.end - r.start
		if r.start <= originalPos {
			originalPos += rangeLen
		} else {
			break
		}
	}
	return originalPos
}
