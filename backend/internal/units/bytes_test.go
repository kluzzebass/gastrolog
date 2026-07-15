package units

import "testing"

// The byte-unit table is strict SI/IEC and shared (in meaning) with
// system.ParseSize and the frontend's parseBytes/formatBytesBigint:
// KB/MB/GB/TB decimal, KiB/MiB/GiB/TiB binary. These tests pin the
// display side so a relabel or base change cannot drift silently.

func TestFormatBytesDisplayIECLabels(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1 << 10, "1.0 KiB"},
		{1 << 20, "1.0 MiB"},
		{1 << 30, "1.0 GiB"},
		{1 << 40, "1.0 TiB"},
		{2_000_000_000, "1.9 GiB"}, // "2GB" (decimal) displays as its exact binary size
	}
	for _, c := range cases {
		if got := FormatBytesDisplay(c.in); got != c.want {
			t.Errorf("FormatBytesDisplay(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFormatBytesCompactExactRoundTrip(t *testing.T) {
	cases := []struct {
		in   uint64
		want string
	}{
		{0, "0B"},
		{2_000_000_000, "2GB"}, // decimal preferred: entered "2GB" echoes back verbatim
		{2 << 30, "2GiB"},      // exact binary falls back to IEC
		{64_000_000, "64MB"},
		{64 << 20, "64MiB"},
		{1_000, "1KB"},
		{1024, "1KiB"},
		{999, "999B"}, // divisible by nothing: raw bytes, exactness over brevity
	}
	for _, c := range cases {
		if got := FormatBytesCompact(c.in); got != c.want {
			t.Errorf("FormatBytesCompact(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}
