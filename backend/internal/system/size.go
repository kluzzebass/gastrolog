package system

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

// ParseSize parses a human-friendly byte size string. Supports:
//   - Plain numbers: "1073741824" (bytes)
//   - KB/MB/GB/TB: "1GB", "500MB", "1.5TB"
//   - KiB/MiB/GiB/TiB: "1GiB", "512MiB"
//
// Case insensitive. Returns bytes as uint64.
func ParseSize(s string) (uint64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, errors.New("empty size string")
	}

	// Find where the number ends and the unit begins.
	i := 0
	for i < len(s) && (unicode.IsDigit(rune(s[i])) || s[i] == '.') {
		i++
	}
	if i == 0 {
		return 0, fmt.Errorf("invalid size: %q", s)
	}

	numStr := s[:i]
	unit := strings.TrimSpace(s[i:])

	num, err := strconv.ParseFloat(numStr, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size number: %w", err)
	}
	if num < 0 {
		return 0, fmt.Errorf("negative size: %q", s)
	}

	var multiplier float64
	switch strings.ToLower(unit) {
	case "", "b":
		multiplier = 1
	case "kb":
		multiplier = 1000
	case "mb":
		multiplier = 1000 * 1000
	case "gb":
		multiplier = 1000 * 1000 * 1000
	case "tb":
		multiplier = 1000 * 1000 * 1000 * 1000
	case "kib":
		multiplier = 1024
	case "mib":
		multiplier = 1024 * 1024
	case "gib":
		multiplier = 1024 * 1024 * 1024
	case "tib":
		multiplier = 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size unit: %q", unit)
	}

	return uint64(num * multiplier), nil
}

// FormatSize formats bytes as a human-readable string using binary units.
func FormatSize(bytes uint64) string {
	switch {
	case bytes >= 1024*1024*1024*1024:
		return fmt.Sprintf("%.1f TiB", float64(bytes)/(1024*1024*1024*1024))
	case bytes >= 1024*1024*1024:
		return fmt.Sprintf("%.1f GiB", float64(bytes)/(1024*1024*1024))
	case bytes >= 1024*1024:
		return fmt.Sprintf("%.1f MiB", float64(bytes)/(1024*1024))
	case bytes >= 1024:
		return fmt.Sprintf("%.1f KiB", float64(bytes)/1024)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}
