package config

import (
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input string
		want  time.Duration
	}{
		{"0s", 0},
		{"30s", 30 * time.Second},
		{"5m", 5 * time.Minute},
		{"2h", 2 * time.Hour},
		{"1d", 24 * time.Hour},
		{"7d", 7 * 24 * time.Hour},
		{"1w", 7 * 24 * time.Hour},
		{"2w", 14 * 24 * time.Hour},
		{"360d", 360 * 24 * time.Hour},
		{"1w2d12h", 7*24*time.Hour + 2*24*time.Hour + 12*time.Hour},
		{"1d12h30m", 24*time.Hour + 12*time.Hour + 30*time.Minute},
		// Falls back to time.ParseDuration for sub-second.
		{"500ms", 500 * time.Millisecond},
		{"1h30m", 90 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseDuration(tt.input)
			if err != nil {
				t.Fatalf("ParseDuration(%q): %v", tt.input, err)
			}
			if got != tt.want {
				t.Errorf("ParseDuration(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseDurationErrors(t *testing.T) {
	t.Parallel()
	bad := []string{"", "abc", "5", "5x", "d", "w"}
	for _, s := range bad {
		t.Run(s, func(t *testing.T) {
			_, err := ParseDuration(s)
			if err == nil {
				t.Errorf("ParseDuration(%q) should fail", s)
			}
		})
	}
}

func TestFormatDuration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input time.Duration
		want  string
	}{
		{0, "0s"},
		{30 * time.Second, "30s"},
		{24 * time.Hour, "1d"},
		{7 * 24 * time.Hour, "1w"},
		{9 * 24 * time.Hour, "1w2d"},
		{9*24*time.Hour + 12*time.Hour, "1w2d12h0m0s"},
		{360 * 24 * time.Hour, "51w3d"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			got := FormatDuration(tt.input)
			if got != tt.want {
				t.Errorf("FormatDuration(%v) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
