package config

import (
	"errors"
	"fmt"
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

// FormatDuration formats a duration using the largest applicable unit.
// Produces human-readable output like "7d", "2w3d", "12h30m".
func FormatDuration(d time.Duration) string {
	if d == 0 {
		return "0s"
	}

	var result string
	remaining := d

	if weeks := remaining / (7 * 24 * time.Hour); weeks > 0 {
		result += fmt.Sprintf("%dw", weeks)
		remaining -= weeks * 7 * 24 * time.Hour
	}
	if days := remaining / (24 * time.Hour); days > 0 {
		result += fmt.Sprintf("%dd", days)
		remaining -= days * 24 * time.Hour
	}
	if remaining > 0 {
		// Let time.Duration handle sub-day formatting.
		result += remaining.String()
	}

	return result
}
