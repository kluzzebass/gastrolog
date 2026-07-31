package units_test

import (
	"testing"

	"gastrolog/internal/system"
	"gastrolog/internal/units"
)

// The display formatter for operator-settable size values must round-trip
// through system.ParseSize: a value shown in `inspect` and copied back into a
// create command must re-parse to the same bytes. FormatBytesDisplay
// (.toFixed) drifts (1500 → "1.5 KiB" → 1536); FormatBytesCompact does not.
// 1001 and 1500 are the probe values that expose the drift.
func TestFormatBytesCompactRoundTripsThroughParseSize(t *testing.T) {
	for _, n := range []uint64{
		0, 1, 999, 1000, 1001, 1024, 1500,
		1<<20 + 7, 50 * 1000 * 1000 * 1000,
		1 << 30, 1<<30 + 1, 1 << 40,
	} {
		got, err := system.ParseSize(units.FormatBytesCompact(n))
		if err != nil {
			t.Errorf("ParseSize(FormatBytesCompact(%d)) errored: %v", n, err)
			continue
		}
		if got != n {
			t.Errorf("round-trip drift: %d → %q → %d", n, units.FormatBytesCompact(n), got)
		}
	}
}
