package system

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

// ParseDuration parses a human-friendly duration string supporting units
// that time.ParseDuration does not: days (d) and weeks (w).
//
// Accepted units: s (seconds), m (minutes), h (hours), d (days), w (weeks).
// Compound expressions like "2w3d12h" are supported.
// Falls back to time.ParseDuration for sub-day units (e.g., "500ms").
//
// Examples:
//
//	"30s"      → 30 seconds
//	"5m"       → 5 minutes
//	"2h"       → 2 hours
//	"7d"       → 7 days (168 hours)
//	"2w"       → 14 days (336 hours)
//	"1w2d12h"  → 9.5 days (228 hours)
//	"0s"       → 0 (immediate)
//	"360d"     → 360 days
func ParseDuration(s string) (time.Duration, error) {
	// Collapse whitespace like ParseSize: "1h 30m" and "1 h" mean what they
	// look like. time.ParseDuration itself rejects any spaces.
	s = strings.Join(strings.Fields(s), "")
	if s == "" {
		return 0, errors.New("empty duration")
	}

	// If it contains 'd' or 'w', use our parser.
	hasExtended := false
	for _, c := range s {
		if c == 'd' || c == 'w' {
			hasExtended = true
			break
		}
	}
	if !hasExtended {
		return time.ParseDuration(s)
	}

	var total time.Duration
	remaining := s

	for remaining != "" {
		// Read number.
		i := 0
		for i < len(remaining) && (unicode.IsDigit(rune(remaining[i])) || remaining[i] == '.') {
			i++
		}
		if i == 0 {
			return 0, fmt.Errorf("invalid duration %q: expected number at %q", s, remaining)
		}

		numStr := remaining[:i]
		remaining = remaining[i:]

		if remaining == "" {
			return 0, fmt.Errorf("invalid duration %q: missing unit after %s", s, numStr)
		}

		// Read unit.
		unit := remaining[0]
		remaining = remaining[1:]

		var n float64
		if _, err := fmt.Sscanf(numStr, "%f", &n); err != nil {
			return 0, fmt.Errorf("invalid duration %q: bad number %q", s, numStr)
		}

		switch unit {
		case 's':
			total += time.Duration(n * float64(time.Second))
		case 'm':
			total += time.Duration(n * float64(time.Minute))
		case 'h':
			total += time.Duration(n * float64(time.Hour))
		case 'd':
			total += time.Duration(n * 24 * float64(time.Hour))
		case 'w':
			total += time.Duration(n * 7 * 24 * float64(time.Hour))
		default:
			return 0, fmt.Errorf("invalid duration %q: unknown unit %q", s, string(unit))
		}
	}

	return total, nil
}

// formatUnits are the units FormatDuration emits, largest first.
var formatUnits = []struct {
	size   time.Duration
	suffix string
}{
	{7 * 24 * time.Hour, "w"},
	{24 * time.Hour, "d"},
	{time.Hour, "h"},
	{time.Minute, "m"},
	{time.Second, "s"},
}

// FormatDuration formats a duration using the largest applicable units,
// omitting every component that is zero. Produces human-readable output like
// "7d", "2w3d", "12h30m", "3m". This is the operator-facing spelling of a
// duration — time.Duration's own String pads with zero components ("3m0s",
// "12h0m0s"), which no human writes.
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	var b strings.Builder
	remaining := d
	for _, unit := range formatUnits {
		n := remaining / unit.size
		if n == 0 {
			continue
		}
		fmt.Fprintf(&b, "%d%s", n, unit.suffix)
		remaining -= n * unit.size
	}
	if remaining > 0 {
		// Sub-second remainder: time.Duration picks the right small unit.
		b.WriteString(remaining.String())
	}

	return b.String()
}
