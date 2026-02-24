// Package timestamp provides a digester that extracts source timestamps
// from raw log lines when the ingester did not set one.
package timestamp

import (
	"time"

	"gastrolog/internal/orchestrator"
)

// Digester extracts timestamps from raw log content and sets SourceTS.
// It recognizes (in priority order by earliest match position):
//   - RFC 3339 / ISO 8601:  2024-01-15T10:30:45.123456Z
//   - Apple unified log:    2024-01-15 10:30:45.123456-0800
//   - Syslog BSD (RFC 3164): Jan  5 15:04:02
//   - Common Log Format:    [02/Jan/2006:15:04:05 -0700]
//   - Go/Ruby datestamp:    2024/01/15 10:30:45
//
// If SourceTS is already set (non-zero), the message is left unchanged.
// Parsing is best-effort: if nothing matches, the message passes through.
//
// Additionally recognized (added for common log frameworks):
//   - Ctime / BSD:            Fri Feb 13 17:49:50.028 2026
type Digester struct{}

// New creates a timestamp digester.
func New() *Digester { return &Digester{} }

func (d *Digester) Digest(msg *orchestrator.IngestMessage) {
	if !msg.SourceTS.IsZero() {
		return
	}
	if len(msg.Raw) == 0 {
		return
	}

	ts := extractTimestamp(msg.Raw)
	if !ts.IsZero() {
		msg.SourceTS = ts
	}
}

// extractor tries to parse a timestamp starting at raw[pos].
// Returns the parsed time and true on success.
type extractor func(raw []byte, pos int) (time.Time, bool)

// extractTimestamp scans raw for the earliest matching timestamp.
// Each extractor is tried at the position where its prefix pattern first appears.
// The match at the lowest byte position wins.
func extractTimestamp(raw []byte) time.Time {
	bestTS := time.Time{}
	bestPos := len(raw)

	// Each entry: find the earliest position where the format could start,
	// then try to parse. We scan for prefix patterns to avoid calling every
	// extractor at every position.
	type candidate struct {
		pos int
		ext extractor
	}

	var candidates []candidate

	// 1. YYYY-MM-DDT... (RFC 3339) or YYYY-MM-DD ... (Apple unified)
	//    Both start with a 4-digit year, dash, 2-digit month, dash, 2-digit day.
	//    Disambiguated by byte at position 10 (T vs space).
	if pos := findYearDashPrefix(raw); pos >= 0 && pos < bestPos {
		candidates = append(candidates, candidate{pos, tryDateDash})
	}

	// 2. Syslog BSD: 3-letter month at line start area (Jan, Feb, ...)
	if pos := findMonthPrefix(raw); pos >= 0 && pos < bestPos {
		candidates = append(candidates, candidate{pos, trySyslogBSD})
	}

	// 3. Common Log Format: [DD/Mon/
	if pos := findCLFPrefix(raw); pos >= 0 && pos < bestPos {
		candidates = append(candidates, candidate{pos, tryCLF})
	}

	// 4. Go/Ruby: YYYY/MM/DD
	if pos := findYearSlashPrefix(raw); pos >= 0 && pos < bestPos {
		candidates = append(candidates, candidate{pos, tryGoRuby})
	}

	// 5. Ctime / BSD: "Fri Feb 13 17:49:50" (weekday + month)
	if pos := findWeekdayMonthPrefix(raw); pos >= 0 && pos < bestPos {
		candidates = append(candidates, candidate{pos, tryCtime})
	}

	for _, c := range candidates {
		if c.pos >= bestPos {
			continue
		}
		if ts, ok := c.ext(raw, c.pos); ok {
			bestTS = ts
			bestPos = c.pos
		}
	}

	return bestTS
}

// findYearDashPrefix finds the first position matching YYYY-MM-DD.
func findYearDashPrefix(raw []byte) int {
	// Need at least "YYYY-MM-DD" = 10 bytes.
	for i := 0; i+9 < len(raw); i++ {
		if isDigit(raw[i]) && isDigit(raw[i+1]) && isDigit(raw[i+2]) && isDigit(raw[i+3]) &&
			raw[i+4] == '-' && isDigit(raw[i+5]) && isDigit(raw[i+6]) &&
			raw[i+7] == '-' && isDigit(raw[i+8]) && isDigit(raw[i+9]) {
			return i
		}
	}
	return -1
}

// tryDateDash tries RFC 3339 or Apple unified log at the given position.
func tryDateDash(raw []byte, pos int) (time.Time, bool) {
	r := raw[pos:]

	// Need at least "YYYY-MM-DDTHH:MM:SS" = 19 bytes.
	if len(r) < 19 {
		return time.Time{}, false
	}

	// Check the separator at position 10: T = RFC 3339, space = Apple unified.
	sep := r[10]
	if sep == 'T' {
		return tryRFC3339(r)
	}
	if sep == ' ' {
		return tryAppleUnified(r)
	}
	return time.Time{}, false
}

// tryRFC3339 parses RFC 3339 / ISO 8601 timestamps.
// Examples: 2024-01-15T10:30:45Z, 2024-01-15T10:30:45.123456Z,
// 2024-01-15T10:30:45+01:00, 2024-01-15T10:30:45.123+01:00
func tryRFC3339(r []byte) (time.Time, bool) {
	// Find the end of the timestamp. After the seconds, we may have:
	// - fractional seconds (.NNN...)
	// - timezone: Z, +HH:MM, -HH:MM
	// Minimum: "YYYY-MM-DDTHH:MM:SSZ" = 20 bytes
	if len(r) < 20 {
		return time.Time{}, false
	}

	// Validate basic structure: HH:MM:SS
	if r[13] != ':' || r[16] != ':' {
		return time.Time{}, false
	}

	// Find the end of the timestamp by scanning for the timezone indicator.
	end := 19 // past seconds
	if end < len(r) && r[end] == '.' {
		// Skip fractional seconds.
		end++
		for end < len(r) && isDigit(r[end]) {
			end++
		}
	}

	if end >= len(r) {
		return time.Time{}, false
	}

	switch r[end] {
	case 'Z':
		end++
	case '+', '-':
		// +HH:MM or -HH:MM (6 bytes)
		if end+6 > len(r) {
			return time.Time{}, false
		}
		end += 6
	default:
		return time.Time{}, false
	}

	ts, err := time.Parse(time.RFC3339Nano, string(r[:end]))
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// tryAppleUnified parses Apple unified log timestamps.
// Format: 2024-01-15 10:30:45.123456-0800
// Like RFC 3339 but space instead of T, compact timezone (-0800 not -08:00).
func tryAppleUnified(r []byte) (time.Time, bool) {
	// Minimum: "YYYY-MM-DD HH:MM:SS" = 19 bytes
	if len(r) < 19 {
		return time.Time{}, false
	}

	// Validate HH:MM:SS
	if r[13] != ':' || r[16] != ':' {
		return time.Time{}, false
	}

	end := 19
	hasFrac := false
	if end < len(r) && r[end] == '.' {
		hasFrac = true
		end++
		for end < len(r) && isDigit(r[end]) {
			end++
		}
	}

	// Check for compact timezone: +HHMM or -HHMM
	hasTZ := false
	if end+5 <= len(r) && (r[end] == '+' || r[end] == '-') &&
		isDigit(r[end+1]) && isDigit(r[end+2]) && isDigit(r[end+3]) && isDigit(r[end+4]) {
		hasTZ = true
		end += 5
	}

	// Build the format string to match.
	format := "2006-01-02 15:04:05"
	if hasFrac {
		// Count fractional digits.
		fracEnd := 20 // past the dot
		for fracEnd < end && isDigit(r[fracEnd]) {
			fracEnd++
		}
		nFrac := fracEnd - 20
		if nFrac > 0 {
			// Use .000000000 with the right number of digits.
			frac := ".000000000"
			if nFrac < len(frac)-1 {
				frac = frac[:nFrac+1]
			}
			format += frac
		}
	}
	if hasTZ {
		format += "-0700"
	}

	ts, err := time.Parse(format, string(r[:end]))
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// monthPrefixes maps 3-letter month abbreviations to month numbers.
var monthPrefixes = map[string]time.Month{
	"Jan": time.January, "Feb": time.February, "Mar": time.March,
	"Apr": time.April, "May": time.May, "Jun": time.June,
	"Jul": time.July, "Aug": time.August, "Sep": time.September,
	"Oct": time.October, "Nov": time.November, "Dec": time.December,
}

// findMonthPrefix finds the first position matching a 3-letter month
// followed by a space (e.g. "Jan " or "Feb  ").
func findMonthPrefix(raw []byte) int {
	for i := 0; i+3 < len(raw); i++ {
		if raw[i+3] == ' ' && isUpperAlpha(raw[i]) && isLowerAlpha(raw[i+1]) && isLowerAlpha(raw[i+2]) {
			if _, ok := monthPrefixes[string(raw[i:i+3])]; ok {
				return i
			}
		}
	}
	return -1
}

// trySyslogBSD parses RFC 3164 BSD syslog timestamps.
// Format: "Jan  5 15:04:02" or "Jan 05 15:04:02" (15 bytes).
// No year — infer current year with rollover handling.
func trySyslogBSD(raw []byte, pos int) (time.Time, bool) {
	r := raw[pos:]
	if len(r) < 15 {
		return time.Time{}, false
	}

	// Validate structure: Mon DD HH:MM:SS
	// Position 3: space, position 6: space, position 9: colon, position 12: colon.
	if r[3] != ' ' || r[6] != ' ' || r[9] != ':' || r[12] != ':' {
		return time.Time{}, false
	}

	now := time.Now()

	tsStr := string(r[:15])
	if ts, err := time.Parse("Jan  2 15:04:05", tsStr); err == nil {
		ts = ts.AddDate(now.Year(), 0, 0)
		if ts.After(now.Add(24 * time.Hour)) {
			ts = ts.AddDate(-1, 0, 0)
		}
		return ts, true
	}
	if ts, err := time.Parse("Jan 02 15:04:05", tsStr); err == nil {
		ts = ts.AddDate(now.Year(), 0, 0)
		if ts.After(now.Add(24 * time.Hour)) {
			ts = ts.AddDate(-1, 0, 0)
		}
		return ts, true
	}

	return time.Time{}, false
}

// findCLFPrefix finds the first position matching [DD/Mon/.
func findCLFPrefix(raw []byte) int {
	for i := 0; i+7 < len(raw); i++ {
		if raw[i] == '[' && isDigit(raw[i+1]) && isDigit(raw[i+2]) && raw[i+3] == '/' &&
			isUpperAlpha(raw[i+4]) && isLowerAlpha(raw[i+5]) && isLowerAlpha(raw[i+6]) && raw[i+7] == '/' {
			return i
		}
	}
	return -1
}

// tryCLF parses Common Log Format timestamps.
// Format: [02/Jan/2006:15:04:05 -0700]
func tryCLF(raw []byte, pos int) (time.Time, bool) {
	r := raw[pos:]
	// Minimum: "[02/Jan/2006:15:04:05 -0700]" = 28 bytes.
	if len(r) < 28 {
		return time.Time{}, false
	}

	// Find the closing bracket.
	end := 1
	for end < len(r) && end < 32 && r[end] != ']' {
		end++
	}
	if end >= len(r) || r[end] != ']' {
		return time.Time{}, false
	}

	// Parse the content between brackets.
	ts, err := time.Parse("02/Jan/2006:15:04:05 -0700", string(r[1:end]))
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// findYearSlashPrefix finds the first position matching YYYY/MM/DD.
func findYearSlashPrefix(raw []byte) int {
	for i := 0; i+9 < len(raw); i++ {
		if isDigit(raw[i]) && isDigit(raw[i+1]) && isDigit(raw[i+2]) && isDigit(raw[i+3]) &&
			raw[i+4] == '/' && isDigit(raw[i+5]) && isDigit(raw[i+6]) &&
			raw[i+7] == '/' && isDigit(raw[i+8]) && isDigit(raw[i+9]) {
			return i
		}
	}
	return -1
}

// tryGoRuby parses Go/Ruby style datestamps.
// Format: 2024/01/15 10:30:45
func tryGoRuby(raw []byte, pos int) (time.Time, bool) {
	r := raw[pos:]
	// Minimum: "YYYY/MM/DD HH:MM:SS" = 19 bytes.
	if len(r) < 19 {
		return time.Time{}, false
	}

	// Validate structure.
	if r[10] != ' ' || r[13] != ':' || r[16] != ':' {
		return time.Time{}, false
	}

	ts, err := time.Parse("2006/01/02 15:04:05", string(r[:19]))
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// weekdayPrefixes maps 3-letter weekday abbreviations for validation.
var weekdayPrefixes = map[string]bool{
	"Mon": true, "Tue": true, "Wed": true, "Thu": true,
	"Fri": true, "Sat": true, "Sun": true,
}

// findWeekdayMonthPrefix finds the first position matching "Dow Mon " (e.g. "Fri Feb ").
func findWeekdayMonthPrefix(raw []byte) int {
	// Need at least "Fri Feb 13 17:49:50" = 19+ bytes from the weekday start.
	for i := 0; i+19 <= len(raw); i++ {
		if raw[i+3] == ' ' && isUpperAlpha(raw[i]) && isLowerAlpha(raw[i+1]) && isLowerAlpha(raw[i+2]) {
			if !weekdayPrefixes[string(raw[i:i+3])] {
				continue
			}
			// Check for month at position i+4.
			if i+7 < len(raw) && isUpperAlpha(raw[i+4]) && isLowerAlpha(raw[i+5]) && isLowerAlpha(raw[i+6]) && raw[i+7] == ' ' {
				if _, ok := monthPrefixes[string(raw[i+4:i+7])]; ok {
					return i
				}
			}
		}
	}
	return -1
}

// tryCtime parses ctime / BSD-style timestamps.
// Formats:
//   - "Fri Feb 13 17:49:50 2026"       (with year)
//   - "Fri Feb 13 17:49:50.028 2026"   (with fractional seconds and year)
//   - "Fri Feb 13 17:49:50"            (no year — infer current)
//   - "Fri Feb 13 17:49:50.028"        (fractional seconds, no year)
//   - "Fri Feb  3 17:49:50"            (single-digit day with leading space)
func tryCtime(raw []byte, pos int) (time.Time, bool) {
	r := raw[pos:]
	// Skip weekday + space: "Fri " = 4 bytes.
	// Remaining is "Feb 13 17:49:50..." which is the syslog BSD format.
	if len(r) < 20 { // "Fri " + "Feb 13 17:49:50" = 4+15 = 19 min
		return time.Time{}, false
	}

	after := r[4:] // skip "Fri "
	if len(after) < 15 {
		return time.Time{}, false
	}

	// Validate syslog-style structure: Mon DD HH:MM:SS
	if after[3] != ' ' || after[6] != ' ' || after[9] != ':' || after[12] != ':' {
		return time.Time{}, false
	}

	// Find end of base timestamp (15 bytes) then check for fractional seconds and year.
	end := 15
	hasFrac := false
	if end < len(after) && after[end] == '.' {
		hasFrac = true
		end++
		for end < len(after) && isDigit(after[end]) {
			end++
		}
	}

	// Check for trailing year: " 2026"
	hasYear := false
	if end+5 <= len(after) && after[end] == ' ' &&
		isDigit(after[end+1]) && isDigit(after[end+2]) && isDigit(after[end+3]) && isDigit(after[end+4]) {
		hasYear = true
		end += 5
	}

	tsStr := string(after[:end])

	// Build format string.
	layouts := []string{}
	switch {
	case hasYear && hasFrac:
		layouts = append(layouts, "Jan  2 15:04:05.000000000 2006", "Jan 02 15:04:05.000000000 2006")
	case hasYear:
		layouts = append(layouts, "Jan  2 15:04:05 2006", "Jan 02 15:04:05 2006")
	case hasFrac:
		layouts = append(layouts, "Jan  2 15:04:05.000000000", "Jan 02 15:04:05.000000000")
	default:
		layouts = append(layouts, "Jan  2 15:04:05", "Jan 02 15:04:05")
	}

	now := time.Now()
	for _, layout := range layouts {
		ts, err := time.Parse(layout, tsStr)
		if err != nil {
			continue
		}
		if !hasYear {
			ts = ts.AddDate(now.Year(), 0, 0)
			if ts.After(now.Add(24 * time.Hour)) {
				ts = ts.AddDate(-1, 0, 0)
			}
		}
		return ts, true
	}

	return time.Time{}, false
}

func isDigit(b byte) bool      { return b >= '0' && b <= '9' }
func isUpperAlpha(b byte) bool { return b >= 'A' && b <= 'Z' }
func isLowerAlpha(b byte) bool { return b >= 'a' && b <= 'z' }
