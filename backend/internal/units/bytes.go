// Package units provides human-readable formatting for byte counts and other units.
package units

import "fmt"

// Byte units are strict SI/IEC everywhere: KB/MB/GB/TB are decimal (×1000),
// KiB/MiB/GiB/TiB are binary (×1024) — the same table system.ParseSize and the
// frontend parseBytes accept. Displays compute binary and therefore carry IEC
// labels; printing "GB" for a ÷1024³ quantity would mislabel it.

// FormatBytesCompact formats a byte count as a compact string (e.g. "64MB",
// "2GiB") without spaces or decimals, using the largest unit that divides
// evenly — decimal preferred at each scale so "2GB" round-trips verbatim,
// binary as the exact fallback.
func FormatBytesCompact(b uint64) string {
	units := []struct {
		mult  uint64
		label string
	}{
		{1_000_000_000_000_000_000, "EB"},
		{1 << 60, "EiB"},
		{1_000_000_000_000_000, "PB"},
		{1 << 50, "PiB"},
		{1_000_000_000_000, "TB"},
		{1 << 40, "TiB"},
		{1_000_000_000, "GB"},
		{1 << 30, "GiB"},
		{1_000_000, "MB"},
		{1 << 20, "MiB"},
		{1_000, "KB"},
		{1 << 10, "KiB"},
	}
	for _, u := range units {
		if b >= u.mult && b%u.mult == 0 {
			return fmt.Sprintf("%d%s", b/u.mult, u.label)
		}
	}
	return fmt.Sprintf("%dB", b)
}

// FormatBytesDisplay formats a byte count as a human-readable string with
// spaces and decimal precision (e.g. "1.5 MiB").
func FormatBytesDisplay(b int64) string {
	switch {
	case b >= 1<<60:
		return fmt.Sprintf("%.1f EiB", float64(b)/(1<<60))
	case b >= 1<<50:
		return fmt.Sprintf("%.1f PiB", float64(b)/(1<<50))
	case b >= 1<<40:
		return fmt.Sprintf("%.1f TiB", float64(b)/(1<<40))
	case b >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
